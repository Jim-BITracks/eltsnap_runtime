import pyodbc
# pip install pyodbc

import snowflake.connector
# pip install --upgrade snowflake-connector-python

import sys
import os
import argparse
import time

# get command line arguments
try:
    parser = argparse.ArgumentParser()
    parser.add_argument("eltsnap_server")
    parser.add_argument("eltsnap_database")
    parser.add_argument("eltsnap_environment")
    parser.add_argument("eltsnap_project")
    parser.add_argument("eltsnap_package")
    parser.add_argument("-prg","--ProjectRunGUID")
    parser.add_argument("-pkg","--PackageRunGIUD")
    parser.add_argument("-fsrv","--framework_server")
    parser.add_argument("-fdb","--framework_database")
    parser.add_argument("-qkey","--query_runtime_key")
    parser.add_argument("-cstr","--conn_string")
    parser.add_argument("-fcstr","--framework_conn_string")
    args = parser.parse_args()

    print("Using eltsnap_server: " + args.eltsnap_server)
    print("Using eltsnap_database: " + args.eltsnap_database)
    print("Using eltsnap_environment: " + args.eltsnap_environment)
    print("Using eltsnap_project: " + args.eltsnap_project)
    print("Using eltsnap_package: " + args.eltsnap_package)

except argparse.ArgumentError as argument_error:
    print('Error: \n', str(argument_error))
    exit(1)

if not args.conn_string:
    args.conn_string = "Driver={SQL Server Native Client 11.0};Server=" + args.eltsnap_server + ";Database=" + args.eltsnap_database + ";Trusted_Connection=yes;"

print(args.conn_string)

if not args.ProjectRunGUID:
    args.ProjectRunGUID = ""
else:
    args.ProjectRunGUID = args.ProjectRunGUID.replace("{", "")
    args.ProjectRunGUID = args.ProjectRunGUID.replace("}", "")    
    
if not args.PackageRunGIUD:
    args.PackageRunGIUD = ""
else:
    args.PackageRunGIUD = args.PackageRunGIUD.replace("{", "")
    args.PackageRunGIUD = args.PackageRunGIUD.replace("}", "")    

if not args.framework_server:
    args.framework_server = ""

if not args.framework_database:
    args.framework_database = ""

if not args.query_runtime_key:
    args.query_runtime_key = ""

if not args.framework_conn_string:
    args.conn_string = "Driver={SQL Server Native Client 11.0};Server=" + args.framework_server + ";Database=" + args.framework_database + ";Trusted_Connection=yes;"

print("Using ProjectRunGUID: " + args.ProjectRunGUID)
print("Using PackageRunGIUD: " + args.PackageRunGIUD)
print("Using framework_server: " + args.framework_server)
print("Using framework_database: " + args.framework_database)
print("Using query_runtime_key: " + args.query_runtime_key)
print("")

# get snowflake execute_sql variable
if args.query_runtime_key == "":
    eltsnap_execute_sql_con = pyodbc.connect(args.conn_string)
    eltsnap_execute_sql_cur = eltsnap_execute_sql_con.cursor()
    eltsnap_execute_sql_cur.execute("{CALL [" + args.eltsnap_database + "].[py].[get execute process package variable] (?,?,?)}", (args.eltsnap_project, args.eltsnap_package, 'execute_sql'))
    try:
        execute_snowsql = eltsnap_execute_sql_cur.fetchone()[0] 
    except:
        print("No execute_sql variable found for package: " + args.eltsnap_package + "!")
        sys.exit(0)

if args.query_runtime_key != "":
    eltsnap_execute_sql_con = pyodbc.connect(args.framework_conn_string)
    eltsnap_execute_sql_cur = eltsnap_execute_sql_con.cursor()
    eltsnap_execute_sql_cur.execute("{CALL [" + args.framework_database + "].[sync].[get runtime query] (?,?)}", (args.ProjectRunGUID, args.query_runtime_key))
    try:
        execute_snowsql = eltsnap_execute_sql_cur.fetchone()[0] 
    except:
        print("No runtime query found for runtime_key: " + args.query_runtime_key + " with project giud: " + args.ProjectRunGUID + "!")
        sys.exit(0)

print(execute_snowsql)

if not execute_snowsql.strip():  
    print("No query to run!")
    sys.exit(0)

# get all project parameter values
try:
    eltsnap_param_con = pyodbc.connect(args.conn_string)
    eltsnap_param_cur = eltsnap_param_con.cursor()
    eltsnap_param_cur.execute("{CALL [" + args.eltsnap_database + "].[py].[get project parameters] (?,?)}", (args.eltsnap_environment, args.eltsnap_project))
except pyodbc.Error as pyodbc_ex:
    print('Error: \n', str(pyodbc_ex))
    exit(1)


# replace parameter values for execute_snowsql
for row in eltsnap_param_cur:
    execute_snowsql = execute_snowsql.replace(row.parameter_reference, row.parameter_value)

# reset eltsnap_param_cur cursor
try:
    eltsnap_param_cur.close()
    eltsnap_param_cur = eltsnap_param_con.cursor()
    eltsnap_param_cur.execute("{CALL [" + args.eltsnap_database + "].[py].[get project parameters] (?,?)}", (args.eltsnap_environment, args.eltsnap_project))
except pyodbc.Error as pyodbc_ex:
    print('Error: \n', str(pyodbc_ex))
    exit(1)


# set python variables from run-time
for row in eltsnap_param_cur:
    #print(row.parameter_reference)
    if row.parameter_reference == '@[$Project::snowflake_Account]':
        snowflake_Account = row.parameter_value
    elif row.parameter_reference == '@[$Project::snowflake_Region]':
        snowflake_Region = row.parameter_value    
    elif row.parameter_reference == '@[$Project::snowflake_User]':
        snowflake_User = row.parameter_value 
    elif row.parameter_reference == '@[$Project::snowflake_Password]':
        snowflake_Password = row.parameter_value
    elif row.parameter_reference == '@[$Project::snowflake_Warehouse]':
        use_snowflake_Warehouse = row.parameter_value
    elif row.parameter_reference == '@[$Project::snowflake_Database]':
        use_snowflake_Database = row.parameter_value    
    elif row.parameter_reference == '@[$Project::snowflake_Schema]':
        use_snowflake_Schema = row.parameter_value     


# close eltsnap_param_cur cursor
try:
    eltsnap_param_cur.close()
except pyodbc.Error as pyodbc_ex:
    print('Error: \n', str(pyodbc_ex))
    exit(1)    
   
   
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
        if ( run_sql != '' ):
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
