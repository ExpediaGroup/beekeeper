## Beekeeper-API

The `Beekeeper-api` module is an API which provides access to the Beekeeper database and allows to see what metadata and paths are currently being managed in the database.

This allows the user to manually enter a database and a table name and check whether this table has been successfully registered in Beekeeper along with the TTL configuration: the current status of the table, the date and time it will be deleted, the current cleanup delay... etc.

It currently supports two endpoints; one for the expired metadata (TTL) and another one for the unreferenced paths.

It also supports different types of filtering (see [filtering section](https://github.com/ExpediaGroup/beekeeper#filtering)).

### Expired metadata endpoint (`GET /metadata`)

This endpoint will return the TTL configuration of all expired partitions that are going to be deleted (or have been deleted) in a specific table. If it is unpartitioned it will just show one object; the table.

It is available in this url; 

    http://beekeeper-api.<address>/api/v1/database/{databaseName}/table/{tableName}/metadata

 where `{databaseName}` and `{tableName}` must be replaced by the database and table name the user wants to search for. So for example, if they wanted to check a table called `my_cool_table` in the database `my_cool_database`, they would go to 
 
    http://beekeeper-api.<address>/api/v1/database/my_cool_database/table/my_cool_table/metadata

### Unreferenced paths endpoint (`GET /unreferenced-paths`)

This endpoint will return the configuration of all unreferenced paths that are going to be deleted (or have been deleted) in a specific table. If it is unpartitioned it will just show one object; the table.

It is available in this url; 

    http://beekeeper-api.<address>/api/v1/database/{databaseName}/table/{tableName}/unreferenced-paths

 where `{databaseName}` and `{tableName}` must be replaced by the database and table name the user wants to search for. So for example, if they wanted to check a table called `my_cool_table` in the database `my_cool_database`, they would go to 
 
    http://beekeeper-api.<address>/api/v1/database/my_cool_database/table/my_cool_table/unreferenced-paths


### Filtering

The filtering available is the same in both endpoints, just keep in mind that in the first one it is referring to expired metadata and in the second one it is referring to unreferenced paths. 

The following table gives an overview of the filters available. It uses the metadata endpoint for the examples, but if the user wants to refer to paths they just have to replace `/database/{databaseName}/table/{tableName}/metadata` with `/database/{databaseName}/table/{tableName}/unreferenced-paths`.

![Filtering table](.README_images/filtering_table.png)

Note: the `partition_name` filter is only available for the expired metadata endpoint, as this variable is not available in the paths.
