import pyodbc
# pip install pyodbc
import sys
for p in sys.path:
    print(p)

import snowflake.connector
# pip install --upgrade snowflake-connector-python

import sys
import argparse

# get command line arguments


try:
    parser = argparse.ArgumentParser()

    parser.add_argument("-eltsnap_server", type=str, default="")
    parser.add_argument("-eltsnap_database", type=str, default="")
    parser.add_argument("-eltsnap_environment", type=str, default="")
    parser.add_argument("-eltsnap_project", type=str, default="")
    parser.add_argument("-eltsnap_package", type=str, default="")
    parser.add_argument("-prg", "--ProjectRunGUID", type=str, default="")
    parser.add_argument("-pkg", "--PackageRunGIUD", type=str, default="")
    parser.add_argument("-fsrv", "--framework_server", type=str, default="")
    parser.add_argument("-fdb", "--framework_database", type=str, default="")
    parser.add_argument("-qkey", "--query_runtime_key", type=str, default="")
    parser.add_argument("-cstr", "--conn_string", type=str, default="")
    parser.add_argument("-fcstr", "--framework_conn_string", type=str, default="")

    parser.add_argument("-snowflake_Account")
    parser.add_argument("-snowflake_Region")
    parser.add_argument("-snowflake_User")
    parser.add_argument("-snowflake_Password")
    parser.add_argument("-use_snowflake_Warehouse")
    parser.add_argument("-use_snowflake_Database")
    parser.add_argument("-use_snowflake_Schema")

    args = parser.parse_args()

    framework_conn_string = args.framework_conn_string
    conn_string = args.conn_string
    ProjectRunGUID = args.ProjectRunGUID
    PackageRunGIUD = args.PackageRunGIUD

    print("Using eltsnap_server: " + args.eltsnap_server)
    print("Using eltsnap_database: " + args.eltsnap_database)
    print("Using eltsnap_environment: " + args.eltsnap_environment)
    print("Using eltsnap_project: " + args.eltsnap_project)
    print("Using eltsnap_package: " + args.eltsnap_package)

except argparse.ArgumentError as argument_error:
    print('Error: \n', str(argument_error))
    exit(1)

if not conn_string:
    conn_string = f"Driver={{SQL Server Native Client 11.0}};Server={args.eltsnap_server};Database={args.eltsnap_database};Trusted_Connection=yes;"

print(conn_string)

if ProjectRunGUID:
    ProjectRunGUID = ProjectRunGUID.replace("{", "").replace("}", "")

if PackageRunGIUD:
    PackageRunGIUD = PackageRunGIUD.replace("{", "").replace("}", "")

if not framework_conn_string:
    framework_conn_string = f"Driver={{SQL Server Native Client 11.0}};Server={args.framework_server};Database={args.framework_database};Trusted_Connection=yes;"

print("Using ProjectRunGUID: " + args.ProjectRunGUID)
print("Using PackageRunGIUD: " + args.PackageRunGIUD)
print("Using framework_server: " + args.framework_server)
print("Using framework_database: " + args.framework_database)
print("Using query_runtime_key: " + args.query_runtime_key)
print("")

if args.query_runtime_key != "":
    eltsnap_execute_sql_con = pyodbc.connect(framework_conn_string)
    eltsnap_execute_sql_cur = eltsnap_execute_sql_con.cursor()
    eltsnap_execute_sql_cur.execute(f"{{CALL [{args.framework_database}].[sync].[get runtime query] (?,?)}}", (ProjectRunGUID, args.query_runtime_key))
    try:
        execute_snowsql = eltsnap_execute_sql_cur.fetchone()[0]
    except:
        print("No runtime query found for runtime_key: " + args.query_runtime_key + " with project giud: " + ProjectRunGUID + "!")
        sys.exit(3)

print(execute_snowsql)

if not execute_snowsql.strip():
    print("No query to run!")
    sys.exit(3)

try:
    snowflake_Account = args.snowflake_Account
    snowflake_Region = args.snowflake_Region
    snowflake_User = args.snowflake_User
    snowflake_Password = args.snowflake_Password
    use_snowflake_Warehouse = args.use_snowflake_Warehouse
    use_snowflake_Database = args.use_snowflake_Database
    use_snowflake_Schema = args.use_snowflake_Schema
except Exception as exc:
    print("Error: There is no one of snowflakes parameters")
    print(exc)
    sys.exit(3)

# print select variables from run-time
print("Using snowflake Warehouse: " + use_snowflake_Warehouse)
print("Using snowflake Database: " + use_snowflake_Database)
print("Using snowflake Schema: " + use_snowflake_Schema)
print("")


# Connecting to the Snowflake DB
try:
    if snowflake_Region == 'n/a':
        cnx = snowflake.connector.connect(user=snowflake_User, password=snowflake_Password, account=snowflake_Account)
    else:
        cnx = snowflake.connector.connect(user=snowflake_User, password=snowflake_Password, account=snowflake_Account, region=snowflake_Region)

    cur = cnx.cursor()
    cur.execute("USE warehouse " + use_snowflake_Warehouse)
    cur.execute("USE database " + use_snowflake_Database)
    cur.execute("USE schema " + use_snowflake_Schema)
except snowflake.connector.errors.ProgrammingError as e:
    print(e)
    print('Error: {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
    cur.close()
    exit(1)
except snowflake.connector.errors.DatabaseError as dbexception:
    print('Error: \n', str(dbexception))
    exit(1)

print("connected to snowflake")
print("")

# Run Command(s)
try:
    sql_commands = execute_snowsql.split(";")
    row_count = 0
    for sql in sql_commands:
        print(sql)
        run_sql = sql.strip()
        if run_sql != '':
            cur = cnx.cursor()
            cur.execute(run_sql + ';')
            stmt_row_count = cur.rowcount
            if stmt_row_count is not None and not run_sql.endswith('--norowcount'):
                row_count = row_count + stmt_row_count
except snowflake.connector.errors.ProgrammingError as e:
    print(e)
    print('Error: {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
    cur.close()
    exit(1)
except snowflake.connector.errors.DatabaseError as dbexception:
    print('Error: \n', str(dbexception))
    exit(1)

# Clean-up
cur.close()

print('')
print('Row Count: ', row_count)
print('')
print("complete!")
