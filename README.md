# deltatables-vacuum
A library that runs vacuum on all delta tables in all databases or on a subset. 

Build a jar and pass it to Spark. 

Accepted arguments:

```
-d, --databases <db1>,<db2>...        database name or list of databases to run vacuum on (accepts regexp)
-e, --excludeDb <db1>,<db2>...        database name or list of databases to exclude from vacuum (accepts regexp)
```