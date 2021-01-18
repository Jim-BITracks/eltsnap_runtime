[CmdletBinding()]

Param(
    [string]$col_list,
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
    }


# truncate destination table
if ($dest_truncate -eq "Y")
{
    $dest_conn_ = New-Object System.Data.Odbc.OdbcConnection
    $dest_conn_.ConnectionString = $dest_conn
    $dest_cmd_ = New-Object System.Data.Odbc.OdbcCommand
    $dest_cmd_.Connection = $dest_conn_
    $dest_cmd_.CommandText = "TRUNCATE TABLE $dst_schema.$dst_table"
    try
    {
        $dest_conn_.Open()
        $t = $dest_cmd_.ExecuteNonQuery()
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

try{
# set-up source connection
$src_conn_ = New-Object System.Data.SqlClient.SqlConnection
$src_conn_.ConnectionString = $src_conn
$src_cmd = New-Object System.Data.SqlClient.SqlCommand
$src_cmd.Connection = $src_conn_
$src_cmd.CommandText = $src_sql_command
}

catch {
    Write-Output 'Error: source connection execution failed !' $_.Exception.Message
}



try {
    $src_conn_.Open()
    $Datatable = New-Object System.Data.DataTable
    $tabledump= $src_cmd.ExecuteReader()
    $columns = $tabledump.GetColumnSchema().ForEach({$_.ColumnName}).Split(",").Trim()

    $k = $Datatable.Load($tabledump)
    $src_conn_.Close()
    $row_cnt = $Datatable.Rows.Count

    #$rowCount = $tabledump | Measure-Object | select -ExpandProperty Count

    $f=0


    $val= ('')


    $vals = New-Object System.Collections.ArrayList

    $dest_conn_ = New-Object System.Data.Odbc.OdbcConnection
    $dest_conn_.ConnectionString = $dest_conn
    $dest_cmd_ = New-Object System.Data.Odbc.OdbcCommand
    $dest_cmd_.Connection = $dest_conn_
    $dest_conn_.Open()
    $row_count = 0

    foreach ($item in $Datatable.Rows) {


        $f +=1


        for ($i = 0; $i -lt $item.ItemArray.Length; $i++) {
            $ite = $item[$i] -replace "'" , "''"
            $val +="'"+ $ite + "',"
        }
        $val = $val.TrimEnd(",")
        $val = "(" +$val+ "),"

        $vals +=  $val
        $val= ('')




        if ($f % 1000 -eq 0 -or $f -eq $row_cnt) {

            $values = [system.String]::Join(" ", $vals)
            $values = $values.TrimEnd(",")


            $cols =  [system.String]::Join(",", $columns)

            if ($identity -eq 'Y')
            {
                $postgresCommand = "SET IDENTITY_INSERT $dst_schema.$dst_table ON;Insert Into $dst_schema.$dst_table ($cols) values $values; SET IDENTITY_INSERT $dst_schema.$dst_table OFF;"


            }
            else {
                $postgresCommand = "Insert Into $dst_schema.$dst_table ($cols) values $values"

            }

            $dest_cmd_.CommandText = $postgresCommand



            $row_count += $dest_cmd_.ExecuteNonQuery()
            Write-Output 'Data written!'
            $vals = New-Object System.Collections.ArrayList

            }
    }    
    Write-Output "Row Count: $row_count"
    $dest_conn_.Close()


}

catch {
    Write-Output 'Error: source connection execution failed !' $_.Exception.Message
    $dest_conn_.Close()
    $src_conn_.Close()



}
