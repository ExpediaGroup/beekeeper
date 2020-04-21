# Beekeeper Vacuum Tool

# Overview
Beekeeper Vacuum Tool looks for any files and folders in the data locations of a Hive table that are not referenced by either the Hive metastore or Beekeeper's database. Any unreferenced paths discovered are scheduled for removal by Beekeeper Cleanup.

# Prerequisites

* Java 11 on the machine executing the jar;
* the machine can connect to the Hive metastore service for the table;
* the machine has access to read the files where the table's data is located;
* the machine can connect to the Beekeeper database.

# Usage

**Note:** _All updates to the table being vacuumed must be paused for the duration of the vacuum process otherwise there is a risk that folders that have been newly created but not yet added to the metastore will be considered candidates for cleanup._
 
If there is a file called `application.properties` in the same folder as the jar, then simply run:
```bash
java -jar beekeeper-vacuum-tool-<version>.jar
```

Otherwise, properties can be provided inline:
```bash
java -Ddatabase=db_name -Dmetastore-uri=thrift://localhost:9083 [...] -jar beekeeper-vacuum-tool-<version>.jar
```

## Configuration

|Property|Required|Description|
|:----|:----:|:----|
| database | Yes | The Hive database name for the table to vacuum. |
| table | Yes | The Hive table name for the table to vacuum. |
| metastore-uri | Yes | Fully qualified URI of the source cluster's Hive metastore Thrift service. Example: `thrift://localhost:9083`. |
| dry-run | No | This property allows you to observe the status of paths on the file system, the metastore, and Beekeeper's database without actually scheduling anything for deletion. Default: `false`.|
| default-cleanup-delay | No | Time To Live (TTL) for unreferenced paths in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format: only days, hours, minutes and seconds can be specified in the expression. Default: `P3D` (three days). |
| partition-batch-size | No | Number of partitions to retrieve in each batch from a table. This property can be changed to a lower number if an out of memory exception occurs. Default: `1000`. |
| spring.datasource.url | Yes | Beekeeper's database JDBC URI. Example: `jdbc:mysql://beekeeper-db-host:3306/beekeeper?useSSL=false`. |
| spring.datasource.username | Yes | Username to connect to Beekeeper's database. |
| spring.datasource.password | Yes | Password to connect to Beekeeper's database. |

### Example Configuration

In a file named `application.properties`:

```properties
database=database_name
table=table_name
metastore-uri=thrift://localhost:9083
dry-run=true
default-cleanup-delay=P3D

spring.datasource.url=jdbc:mysql://beekeeper-db-host:3306/beekeeper?useSSL=false
spring.datasource.username=username
spring.datasource.password=password
```
