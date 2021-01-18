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
 } catch
    {
    $elt_connection.Close()
    Write-Output 'Error: ELT connection query failed!'  $_.Exception.Message
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

#new method for BulkCopy rowcount
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



$dest_conn_ = New-Object System.Data.SqlClient.SqlConnection
$dest_conn_.ConnectionString = $dest_conn
$dest_cmd_ = New-Object System.Data.SqlClient.SqlCommand
$dest_cmd_.Connection = $dest_conn_

    try {
        $dest_cmd_.CommandText = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='$dst_schema' and TABLE_NAME='$dst_table'"
        $dest_conn_.Open()
        $rdr = $dest_cmd_.ExecuteReader()
        $columns_destination = @()

        while ($rdr.Read()) {
            $columns_destination += $rdr["COLUMN_NAME"]
        }

        $dest_conn_.Close()

    }
    catch {
        $dest_conn_.Close()
        Write-Output 'Error: Column get operation failed!' $_.Exception.Message
		return
    }



# truncate destination table

    if ($dest_truncate -eq "Y") {

        $dest_cmd_.CommandText = "TRUNCATE TABLE [$dst_schema].[$dst_table]"

    try
    {
        $dest_conn_.Open()
        $dest_cmd_.ExecuteNonQuery()
        $dest_conn_.Close()
        echo "destination truncated"


    }
    catch
    {
        $dest_conn_.Close()
        Write-Output 'Error: Truncate destination table failed!' $_.Exception.Message

		return
    }

    }


# set-up source connection
$src_conn_ = New-Object system.data.odbc.odbcconnection
$src_conn_.ConnectionString = $src_conn
$src_cmd = New-Object System.Data.Odbc.OdbcCommand
$src_cmd.Connection = $src_conn_
$src_cmd.CommandText = $src_sql_command



# bulk copy

if($identity -eq "Y") {
    try
    {
        $src_conn_.Open()
        [System.Data.odbc.OdbcDataReader] $SqlTable = $src_cmd.ExecuteReader()

        $dst_bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($dest_conn,[System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
        echo "identity insert"

        $columns_source = $SqlTable.GetSchemaTable().Rows.ForEach({$_.ItemArray[0]}).Split(",").Trim()

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
        Write-Output 'Error: Bulk Copy failed!'  $_.Exception.Message 
    }


    }
elseif($identity -eq "N")
{
    try
    {
        $src_conn_.Open()


        [System.Data.odbc.OdbcDataReader] $SqlTable = $src_cmd.ExecuteReader([System.Data.CommandBehavior]:: KeyInfo)
        $columns_source = $SqlTable.GetSchemaTable().Rows.ForEach({$_.ItemArray[0]}).Split(",").Trim()
        
        $columns_final = @()
        for ($c = 0; $c -lt $columns_destination.Count; $c++) {
            $col_dest = $columns_destination[$c]
            foreach ($source_col in $columns_source) {
                if($col_dest -Match $source_col) {
                    $columns_final += $col_dest
                }
                
            }

        }


        $dst_bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($dest_conn,[System.Data.SqlClient.SqlBulkCopyOptions]::Default)
        $dst_bulkCopy.DestinationTableName = "[$dst_schema].[$dst_table]" 

        foreach ($column in $columns_final) { $dst_bulkCopy.ColumnMappings.Add($column, $column)}

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

