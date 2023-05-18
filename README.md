# deltatables-vacuum
A library that runs vacuum on all delta tables in all databases or on a subset. 

Build a jar and pass it to Spark.

```
Usage: deltatables-optimizations [vacuum] [options]

  --help                   use this jar via spark to perform vacuum or optimize on a desired table or database
Command: vacuum [options]
run vacuum on specific databases or tables
  -i, --include <db1>,<db2>,<db3>.<table1>...
                           A list of objects to run VACUUM on.
                           An example of accepted values:
                           user.* - all objects in databases that starts with "user"
                           user - all objects in user database
                           user_x.table - specific table
  -e, --exclude <db1>,<db2>,<db3>.<table1>...
                           A list of databases to exclude from VACUUM.
                           An example of accepted values:
                           user.* - all objects in databases that starts with "user"
                           user - all objects in user database
                           user_x.table - specific table 

```