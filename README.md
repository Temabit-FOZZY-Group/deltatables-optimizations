# deltatables-vacuum
A library that runs vacuum on all delta tables in all databases or on a subset. 

Build a jar and pass it to Spark.

```
Usage: deltatables-optimizations [vacuum|optimize] [options]

  --help                   use this jar via spark to perform vacuum on a desired table or database
  -i, --include [optional] <db1>,<db2>,<db3>.<table1>...
                           A list of objects to run VACUUM on.
                           By default, will run on all databases and tables in metastore
                           An example of accepted values:
                           user.* - all objects in databases that starts with "user"
                           user - all objects in user database
                           user_x.table - specific table
  -e, --exclude [optional] <db1>,<db2>,<db3>.<table1>...
                           A list of databases to exclude from VACUUM.
                           An example of accepted values:
                           user.* - all objects in databases that starts with "user"
                           user - all objects in user database
                           user_x.table - specific table
Command: vacuum
run vacuum on specific databases or tables
Command: optimize [options]
run optimize on specific databases or tables
  -c, --condition [optional] (e.g. date >= '2017-01-01')
                           A condition to run optimize with.
                           E.g. OPTIMIZE delta_table_name WHERE date >= '2017-01-01'

```