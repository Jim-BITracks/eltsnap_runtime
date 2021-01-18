[CmdletBinding()]

Param(
[string]$src_conn,
[string]$dest_conn,
[string]$elt_conn,
[string]$guid,
[string]$dst_schema,
[string]$dst_table,
[string]$batch_size,
[string]$dest_truncate,
[string]$identity
)


$elt_connection = New-Object System.Data.Odbc.OdbcConnection
$elt_connection.ConnectionString = $elt_conn
$elt_cmd = New-Object System.Data.Odbc.OdbcCommand
$elt_cmd.Connection = $elt_connection
$elt_cmd.CommandText = "SELECT [runtime_key], [runtime_value] FROM [dbo].[ELT_runtime_values] where [server_execution_id]='$guid'"

try
{
    $elt_connection.Open()
    $SqlAdapter = New-Object System.Data.Odbc.OdbcDataAdapter
    $SqlAdapter.SelectCommand = $elt_cmd
    $DataSet = New-Object System.Data.DataSet

    $SqlAdapter.Fill($DataSet)
    $elt_connection.Close()
}
catch {
    $elt_connection.Close()
    Write-Output 'Error: ELT Framework connecting failed!' $_.Exception.Message
    }

try
    {
        $elt_cmd.CommandText = "UPDATE [dbo].[ELT_runtime_values] SET return_value='1' where [server_execution_id]='$guid'"
        $elt_connection.Open()
        $elt_cmd.ExecuteReader()
        $elt_connection.Close()
    }
catch
    {
        $elt_connection.Close()
        Write-Output "Error: Didin't insert value !"  $_.Exception.Message
    }
    
    

$table = @()
foreach ($row in $DataSet.Tables[0]) {
    $runtime_key = $row["runtime_key"].ToString();
    $runtime_value = $row["runtime_value"].ToString();
    $table += New-Object psObject -Property @{'key'= $runtime_key;'value'= $runtime_value}
}


foreach ($obj in $table) {

    if ($obj.key -eq 'source_query') {
        $src_sql_command = $obj.value

        }
        elseif ($obj.key -eq 'source_columns') {
            $col_name = $obj.value
        }

    }



#new method for BulkCopy
$source = 'namespace System.Data.SqlClient
{
	using Reflection;

	public static class SqlBulkCopyExtension
	{
		const String _rowsCopiedFieldName = "_rowsCopied";
		static FieldInfo _rowsCopiedField = null;

		public static int RowsCopiedCount(this SqlBulkCopy bulkCopy)
		{
			if (_rowsCopiedField == null) _rowsCopiedField = typeof(SqlBulkCopy).GetField(_rowsCopiedFieldName, BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Instance);
			return (int)_rowsCopiedField.GetValue(bulkCopy);
		}
	}
}
'
Add-Type -WarningAction Ignore -IgnoreWarnings -ReferencedAssemblies System.Runtime, System.Data, System.Data.SqlClient -TypeDefinition $source
$null= [Reflection.Assembly]::LoadWithPartialName("System.Data")


# truncate destination table
if ($dest_truncate -eq "Y")
{
    $dest_conn_ = New-Object System.Data.SqlClient.SqlConnection
    $dest_conn_.ConnectionString = $dest_conn
    $dest_cmd_ = New-Object System.Data.SqlClient.SqlCommand
    $dest_cmd_.Connection = $dest_conn_
    $dest_cmd_.CommandText = "TRUNCATE TABLE [$dst_schema].[$dst_table]"
    echo "destination truncated"
    try
    {
        $dest_conn_.Open()
        $dest_cmd_.ExecuteNonQuery()
        $dest_conn_.Close()

    }
    catch
    {
        $dest_conn_.Close()
        Write-Output 'Error: Truncate destination table failed!'
		return
    }
}

# set-up source connection
$src_conn_ = New-Object System.Data.SqlClient.SqlConnection
$src_conn_.ConnectionString = $src_conn
$src_cmd = New-Object System.Data.SqlClient.SqlCommand
$src_cmd.Connection = $src_conn_
$src_cmd.CommandText = $src_sql_command




# bulk copy
if($identity -eq "Y") {
    try
    {
        $src_conn_.Open()
        [System.Data.SqlClient.SqlDataReader] $SqlTable = $src_cmd.ExecuteReader()

        $dst_bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($dest_conn,[System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
        echo "identity insert"

        $dst_bulkCopy.DestinationTableName = "[$dst_schema].[$dst_table]"
        $dst_bulkCopy.BatchSize = $batch_size
        $dst_bulkCopy.BulkCopyTimeout = 3600 #in seconds = 1 hour
        $dst_bulkCopy.Add_SqlRowscopied({Write-Host "$($args[1].RowsCopied) rows copied" })
        $dst_bulkCopy.WriteToServer($SqlTable)

        $src_conn_.Close()

        $rowcount_end = [System.Data.SqlClient.SqlBulkCopyExtension]::RowsCopiedCount($dst_bulkCopy)

        Write-Output "Row Count $rowcount_end"

    }
    catch
    {
        $src_conn_.Close()
        Write-Output 'Error: Bulk Copy failed!'  $_.Exception.Message    }


    }
elseif($identity -eq "N")
{
    try
    {
        $src_conn_.Open()


        [System.Data.SqlClient.SqlDataReader] $SqlTable = $src_cmd.ExecuteReader()
        $columns = $SqlTable.GetColumnSchema().ForEach({$_.ColumnName}).Split(",").Trim()

        $dst_bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($dest_conn,[System.Data.SqlClient.SqlBulkCopyOptions]::Default)
        $dst_bulkCopy.DestinationTableName = "[$dst_schema].[$dst_table]"

        foreach ($column in $columns) { $dst_bulkCopy.ColumnMappings.Add($column, $column)}

        $dst_bulkCopy.BatchSize = $batch_size
        $dst_bulkCopy.BulkCopyTimeout = 3600 #in seconds = 1 hour
        $dst_bulkCopy.Add_SqlRowscopied({Write-Host "$($args[1].RowsCopied) rows copied" })
        $dst_bulkCopy.WriteToServer($SqlTable)
        $src_conn_.Close()
        echo "no identity insert bulk"
        $rowcount_end = [System.Data.SqlClient.SqlBulkCopyExtension]::RowsCopiedCount($dst_bulkCopy)

        Write-Output "Row Count $rowcount_end"

    }
    catch
    {
        $src_conn_.Close()
        Write-Output 'Error: Bulk Copy failed!'  $_.Exception.Message
    }
}