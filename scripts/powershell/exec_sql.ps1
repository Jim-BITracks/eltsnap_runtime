[CmdletBinding()]

Param(
[string]$src_conn,
[string]$elt_conn,
[string]$guid
)


$elt_connection = New-Object System.Data.Odbc.OdbcConnection
$elt_connection.ConnectionString = $elt_conn
$elt_cmd = New-Object System.Data.Odbc.OdbcCommand
$elt_cmd.Connection = $elt_connection
$elt_cmd.CommandText = "SELECT [runtime_value] FROM [dbo].[ELT_runtime_values] where [server_execution_id]='$guid'"

try
{
    $elt_connection.Open()
    $SqlAdapter = New-Object System.Data.Odbc.OdbcDataAdapter
    $SqlAdapter.SelectCommand = $elt_cmd
    $DataSet = New-Object System.Data.DataSet

    $SqlAdapter.Fill($DataSet)
    $elt_connection.Close()
}
catch
{
    $elt_connection.Close()
    Write-Output 'Error: ELT connection query failed!'  $_.Exception.Message
}

$query = $DataSet.Tables[0].Rows.Item(0)[0];

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



# set-up source connection

if ($src_conn.Split()[0].ToString().ToLower() -Match "driver" -or $src_conn.Split('=')[0].ToString().ToLower() -Match "dsn") {
    $src_conn_ = New-Object system.data.odbc.odbcconnection
    $src_conn_.ConnectionString = $src_conn
    $src_cmd = New-Object System.Data.Odbc.OdbcCommand
    $src_cmd.Connection = $src_conn_
    $src_cmd.CommandTimeout = 0
    $src_cmd.CommandText = $query


}
else {
    $src_conn_ = New-Object System.Data.SqlClient.SqlConnection
    $src_conn_.ConnectionString = $src_conn
    $src_cmd = New-Object System.Data.SqlClient.SqlCommand
    $src_cmd.Connection = $src_conn_
    $src_cmd.CommandTimeout = 0
    $src_cmd.CommandText = $query

}



# exe SQL
try
{
    $src_conn_.Open()
    $rowcount_end = $src_cmd.ExecuteScalar();
    Write-Output "Row Count $rowcount_end"

}
catch
{
    $src_conn_.Close()
    Write-Output 'Error: Execute SQL failed!'  $_.Exception.Message   
}