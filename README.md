![Logo](.README_images/full_logo.png)

![Java CI](https://github.com/ExpediaGroup/beekeeper/workflows/Java%20CI/badge.svg?event=push) [![Coverage Status](https://coveralls.io/repos/github/ExpediaGroup/beekeeper/badge.svg?branch=main)](https://coveralls.io/github/ExpediaGroup/beekeeper?branch=main) ![GitHub license](https://img.shields.io/github/license/ExpediaGroup/beekeeper.svg)
# Table of contents 

- [Overview](#overview)
  - [Start using](#start-using)
- [How does it work?](#how-does-it-work)
  - [Beekeeper Architecture](#beekeeper-architecture)
  - [Unreferenced paths](#unreferenced-paths)
  - [Time To Live, TTL](#time-to-live-ttl)
  - [Hive table configuration](#hive-table-configuration)
- [Running Beekeeper](#running-beekeeper)
  - [Using Docker](#using-docker)
  - [Endpoints](#endpoints)
  - [Application configuration](#application-configuration)
  - [Beekeeper-API](#beekeeper-api)
- [External links](#external-links)
- [Legal](#legal)

# Overview

Beekeeper is a service that schedules orphaned paths and expired metadata for deletion.

The original inspiration for a data deletion tool came from another of our open source projects called [Circus Train](https://github.com/HotelsDotCom/circus-train). At a high level, Circus Train replicates Hive datasets. The datasets are copied as immutable snapshots to ensure strong consistency and snapshot isolation, only pointing the replicated Hive Metastore to the new snapshot on successful completion. This process leaves behind snapshots of data which are now unreferenced by the Hive Metastore, so Circus Train includes a Housekeeping module to delete these files later.

Beekeeper is based on Circus Train's Housekeeping module, however it is decoupled from Circus Train so it can be used by other applications as well.

## Start using

To deploy Beekeeper in AWS, see the [terraform repo](https://github.com/ExpediaGroup/apiary-lifecycle). 

Docker images can be found in Expedia Group's [dockerhub](https://hub.docker.com/search/?q=expediagroup%2Fbeekeeper&type=image). 

# How does it work?

Beekeeper makes use of [Apiary](https://github.com/ExpediaGroup/apiary) - an open source federated cloud data lake - to detect changes in the Hive Metastore. One of Apiary’s components, the [Apiary Metastore Listener](https://github.com/ExpediaGroup/apiary-extensions/tree/main/apiary-metastore-events/sns-metastore-events/apiary-metastore-listener), captures Hive events and publishes these as messages to an SNS topic. Beekeeper uses these messages to detect changes to the Hive Metastore, and perform appropriate deletions.

Beekeeper is comprised of four separate Spring-based Java applications:
1. Scheduler Apiary - An application that schedules paths and metadata for deletion in a shared database, with one table for unreferenced paths and another for expired metadata. 
2. Path Cleanup - An application that perform deletions of unreferenced paths.
3. Metadata Cleanup - An application that perform deletions of expired metadata.
4. Beekeeper API - A REST API that allows users to see what metadata and paths are in the database.

## Beekeeper Architecture

![Beekeeper architecture](.README_images/architecture_diagram.png)

## Unreferenced paths
The "unreferenced" property can be added to tables to detect when paths become unreferenced. It will currently only be triggered by these events:
- `alter_partition`
- `alter_table`
- `drop_partition`
- `drop_table`

By default, `alter_partition` and `alter_table` events require no further configuration. However, in order to avoid unexpected data loss, other event types require whitelisting on a per table basis. See [Hive table configuration](#hive-table-configuration) for more details.

To check whether a table has been configured with the "unreferenced" property, the `beekeeper-api` can be used to look for the table and its current unreferenced paths (see [Unreferenced paths](#unreferenced-paths-endpoint-get-unreferenced-paths)).

### End-to-end lifecycle example
1. A Hive table is configured with the parameter `beekeeper.remove.unreferenced.data=true` (see [Hive table configuration](#hive-table-configuration) for more details.)
2. An operation is executed on the table that orphans some data (alter partition, drop partition, etc.)
3. Hive Metastore events are emitted by the [Hive Metastore Listener](https://github.com/ExpediaGroup/apiary-extensions/tree/main/apiary-metastore-events/sns-metastore-events/apiary-metastore-listener) as a result of the operation.
4. Hive events are picked up from the queue by Beekeeper using the [Apiary Receiver](https://github.com/ExpediaGroup/apiary-extensions/tree/main/apiary-metastore-events/sns-metastore-events/apiary-receivers).
5. Beekeeper processes these messages and schedules orphaned paths for deletion by adding them to a database.
6. The scheduled paths are deleted by Beekeeper after a configurable delay, the default is 3 days (see [Hive table configuration](#hive-table-configuration) for more details.)

## Time To Live, TTL 
The "expired" TTL property will delete tables, partitions, and their locations after a configurable delay. If no delay is specified the default is 30 days. 

If the table is partitioned the cleanup delay will also apply to each partition that is added to the table. The table will only be dropped when there are no remaining partitions. 

Once users have configured a table to use the TTL feature, they can use the `beekeeper-api` metadata endpoint to check if their table has been successfully registered in the Beekeeper database, and also be able to see when it is going to be deleted. More information in the [Beekeeper API](#Beekeeper-API) section.

### End-to-end lifecycle example
1. A Hive table is configured with the TTL parameter `beekeeper.remove.expired.data=true` (see [Hive table configuration](#hive-table-configuration) for more details).
2. This Hive event is picked up from the queue by Beekeeper using the [Apiary Receiver](https://github.com/ExpediaGroup/apiary-extensions/tree/main/apiary-metastore-events/sns-metastore-events/apiary-receivers), and the table is scheduled for cleanup with a configurable delay. 
3. An operation is executed on the table which alters it in some way, (alter table, add partition, alter partition)
4. These Hive events are once again picked up from the queue by Beekeeper using the Apiary receiver. Depending on the event, Beekeeper will do the following:
    - `Alter table` - Creates a new entry in the database with the updated table info
    - `Add partition` - The partition is scheduled to be deleted using the cleanup delay of the table 
    - `Alter partition` - Creates a new entry in the database with the updated partition info
5. The scheduled partitions, tables, and associated paths will be deleted by Beekeeper after the delay has passed.

**TTL Caveats**

Currently with the first release of Beekeeper TTL there are the following issues:
- If a table or partition is dropped by a user before the expiration time the related paths will become unreferenced and won’t be cleaned up. 
    - This can be avoided by also adding the "unreferenced" property to the table, see the [unreferenced paths](#unreferenced-paths) section. However, this property listens to any drop event on that table and we haven’t yet configured Beekeeper to ignore drop events made by itself. So this will mean that any path for a table/partition dropped by Beekeeper during the TTL cleanup will be scheduled for deletion again in the unreferenced cleanup table.
- If a partitioned table with existing partitions is renamed, these partitions will not be dropped until the table has expired. 
    - For example: A table is created with a cleanup delay of 2 days and a partition is added. The delay is changed to 10 days and the table is then renamed. With the current release the existing partition won’t be rescheduled to be deleted under the new table. So it will be deleted along with the table in 10 days instead of 2.  

## Hive table configuration

Beekeeper only actions on events which are marked with specific parameters. These parameters need to be added to the Hive table that you wish to be monitored by Beekeeper. The configuration parameters for Hive tables are as follows:

| Parameter             | Required | Possible values | Description |
|:----|:----:|:----:|:----|
| `beekeeper.remove.unreferenced.data=true`   | Yes |  `true` or `false`       | Set this parameter to ensure Beekeeper monitors your table for orphaned data. |
| `beekeeper.unreferenced.data.retention.period=X` | No | e.g. `P7D` or `PT3H` (based on [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)) | Set this parameter to control the delay between schedule and deletion by Beekeeper. If this is either not set, or configured incorrectly, the default will be used. Default is 3 days. |
| `beekeeper.hive.event.whitelist=X` | No | Comma separated list of event types to whitelist for orphaned data. Valid event values are: `alter_partition`, `alter_table`, `drop_table`, `drop_partition`. | Beekeeper will only process whitelisted events. Default value: `alter_partition`, `alter_table`. |
| `beekeeper.remove.expired.data=true`   | Yes |  `true` or `false`       | Set this parameter to enable TTL on your table. |
| `beekeeper.expired.data.retention.period=X` | No | e.g. `P7D` or `PT3H` (based on [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)) | Set this parameter to control the TTL duration for your table. If this is either not set, or configured incorrectly, the default value of `P30D` (30 days) will be used. |

**Usage**

*Unreferenced Paths*

This command can be used to add the parameter to a Hive Table:

```SQL
ALTER TABLE <table-name> SET TBLPROPERTIES("beekeeper.remove.unreferenced.data"="true");
```

*TTL*

You can either add the property when the table is created:
```SQL
CREATE TABLE <table> (<col_name> <type>, ... ) TBLPROPERTIES("beekeeper.remove.expired.data"="true", "beekeeper.expired.data.retention.period"="PT2M");
```

Or alter an existing table: 
```SQL
ALTER TABLE <table> SET TBLPROPERTIES("beekeeper.remove.expired.data"="true", "beekeeper.expired.data.retention.period"="PT1H");
```

*NOTE - if you add this property to a partitioned table any existing partitions will not be scheduled for deletion. They will be deleted along with the table when the TTL delay is met.*

# Running Beekeeper

Beekeeper consists of four Spring Boot applications which run independently of each other:

- `beekeeper-path-cleanup` periodically queries a database for paths to delete and performs deletions. 
- `beekeeper-metadata-cleanup` periodically queries a database for metadata to delete and performs deletions on hive tables, partitions, and s3 paths. 
- `beekeeper-scheduler-apiary` periodically polls an Apiary SQS queue for Hive Metastore events and inserts S3 paths and Hive tables to be deleted into a database, scheduling them for deletion.
- `beekeeper-api` allows users to retrieve information from the database and see what has been scheduled for deletion.

All applications (except the `beekeeper-api`) require configuration to be provided, see [Application configuration](#application-configuration) for details.

    java -jar <spring-boot-application>.jar --config=<config>.yml
    
`<config>.yml` takes this format:

```yaml
spring.datasource:
  url: jdbc:mysql://<database-url>:3306/beekeeper?useSSL=false
  username: <username>
  password: <password>   
  
# other config
```

This can be provided via a file or Spring can load properties from the environment (see below). 

## Using Docker

Three Docker images are created during `mvn install` - two for cleanup of paths and metadata, and one for scheduling. 

Configuration can be provided in one of two ways:

1. Using environment variables.

```
docker run --env-file <config-env>.env <image-id>
```

`<config-env>.env` takes this format:

```properties
spring_datasource_url=jdbc:mysql://<database-url>:3306/beekeeper?useSSL=false
spring_datasource_username=<user>
spring_datasource_password=<password>

# other config
```

Any additional configuration can be added in a similar way as the app will load properties from the docker environment.

2. Using a base64 encoded properties file as an environment variable:

```
export BEEKEEPER_CONFIG=$(base64 -w 0 -i <config>.yml)
docker run -e BEEKEEPER_CONFIG=$BEEKEEPER_CONFIG <image-id>
```

`<config>.yml` takes this format:

```yaml
spring.datasource:
  url: jdbc:mysql://<database-url>:3306/beekeeper?useSSL=false
  username: <username>
  password: <password>   
  
# other config
```

#### Database password

To avoid the problem of a plaintext password, AWS Secrets Manager is supported.

To use Secrets Manager, remove the password from the `<config>.yml`: 

```yaml
spring.datasource:
  url: jdbc:mysql://<database-url>:3306/beekeeper?useSSL=false
  username: <username>
  
# other config
```

and provide the password strategy and password key when running the container:

```
docker run -e BEEKEEPER_CONFIG=$BEEKEEPER_CONFIG -e DB_PASSWORD_STRATEGY=aws-secrets-manager -e DB_PASSWORD_KEY <password-key> <image-id>
```

#### Local dockerised database

If you would like to connect a dockerised application to a local MySQL database (e.g. initialised from `docker-compose up`), the two containers need to be on the same network:

    docker run --network beekeeper_default <image-id>

where `<database-url>` is the name of the running MySQL container.

## Endpoints

Being a Spring Boot Application, all [standard actuator endpoints](https://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-endpoints.html) are supported.

For example, the healthcheck endpoint at: `http://<address>:<port>/actuator/health`. 

Each application listens on a different port:

| Application                 | Port |
|:----|:----|
| `beekeeper-path-cleanup`    | 8008    |
| `beekeeper-metadata-cleanup`| 9008    |
| `beekeeper-scheduler-apiary`| 8080    |
| `beekeeper-api`             | 7008    |

To access an endpoint when running in a Docker container, the port must be published:

    docker run -p <port>:<port> <image-id>

## Application configuration
### Beekeeper Scheduler Apiary
| Property                            | Required | Description |
|:----|:----|:----|
| `apiary.queue-url`                  | Yes      | URL for SQS queue. |
| `beekeeper.default-cleanup-delay`   | No       | Default Time To Live (TTL) for orphaned paths in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format: only days, hours, minutes and seconds can be specified in the expression. Default value is `P3D` (3 days). |
| `beekeeper.default-expiration-delay`| No       | Default Time To Live (TTL) for tables in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format: only days, hours, minutes and seconds can be specified in the expression. Default value is `P30D` (30 days). |

### Beekeeper Path Cleanup
| Property                            | Required | Description |
|:----|:----:|:----|
| `cleanup-page-size`                 | No       | Number of rows that should be processed in one page. Default value is `500`. |
| `dry-run-enabled`                   | No       | Enable to simply display the deletions that would be performed, without actually doing so. Default value is `false`. |
| `scheduler-delay-ms`                | No       | Amount of time (in milliseconds) between consecutive cleanups. Default value is `300000` (5 minutes after the previous cleanup completes). |

### Beekeeper Metadata Cleanup
| Property                            | Required | Description |
|:----|:----:|:----|
| `cleanup-page-size`                 | No       | Number of rows that should be processed in one page. Default value is `500`. |
| `dry-run-enabled`                   | No       | Enable to simply display the deletions that would be performed, without actually doing so. Default value is `false`. |
| `scheduler-delay-ms`                | No       | Amount of time (in milliseconds) between consecutive cleanups. Default value is `300000` (5 minutes after the previous cleanup completes). |
| `Metastore-uri`                     | Yes      | URI of the Hive Metastore where tables to be cleaned-up are located. |

## Beekeeper-API

Beekeeper also has an API which provides read access to the Beekeeper database and allows you to see what metadata and paths are currently being managed.

The user can manually enter a database and a table name and check whether this table has been successfully registered in Beekeeper along with things like the current status of the table, the date and time it will be deleted, the current cleanup delay... etc.

It currently supports two endpoints; one for the expired metadata and another one for the unreferenced paths.

As well as supporting all [standard actuator endpoints](https://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-endpoints.html), the Beekeeper-API also supports the `swagger` endpoint (see the [Swagger documentation](https://swagger.io/docs/specification/about/)), which provides a visual documentation of the structure of the API, making it easy for the user to explore its capabilities. This is a good start if it's the first time you use the API. You can access it at this url:

    http://<host>/swagger-ui.html

For the two main endpoints, the base url (will be referred to as `<base-url>` in the following sections) is

    http://<host>/api/v1

and the rest of the url will depend on which endpoint you want to access (see [Expired metadata endpoint](#expired-metadata-endpoint) and [Unreferenced paths endpoint](#unreferenced-paths-endpoint)).

It also supports different filters (see [filtering section](#filtering)).

### Expired metadata endpoint (`GET /metadata`)

This endpoint will return the TTL configuration of all expired partitions that are going to be deleted (or have been deleted) in a specific table. If it is unpartitioned it will just show one object; the table.

    <base-url>/database/{databaseName}/table/{tableName}/metadata

 where `{databaseName}` and `{tableName}` must be replaced by the database and table name the user wants to search for. So for example, if they wanted to check a table called `my_cool_table` in the database `my_cool_database`, they would go to 
 
    <base-url>/database/my_cool_database/table/my_cool_table/metadata
    
The API will display all the partitions in that table unless it is unpartitioned, in that case it will show only one object; the table. If you want to check only the table object without all of its individual partitions, you'll have to search for the one with the variable `"partitionName"=null` as such:
    
    "path": "s3://mybucket/mydatabase/mytable",
    "databaseName": "mydatabase",
    "tableName": "mytable",
    "partitionName": null,
    "housekeepingStatus": "DELETED",
    "creationTimestamp": "2020-09-14T17:22:55",
    "modifiedTimestamp": "2020-09-14T18:36:52",
    "cleanupTimestamp": "2020-09-14T17:36:32",
    "cleanupDelay": "PT10M",
    "cleanupAttempts": 1,
    "lifecycleType": "EXPIRED"

You can do this by using [filters](#filtering). If you want to search for the table object, you'll have to add a filter with the path to the table, for example 

    /database/{databaseName}/table/{tableName}/metadata?path=s3://mybucket/mydatabase/mytable

### Unreferenced paths endpoint (`GET /unreferenced-paths`)

This endpoint will return the configuration of all unreferenced paths that are going to be deleted (or have been deleted) in a specific table. If it is unpartitioned it will just show one object; the table.

The url is the same one as in the [metadata endpoint](#expired-metadata-endpoint), except instead of ending in `metadata` it ends in `unreferenced-paths`.

It is available in this url; 

    <base-url>/database/{databaseName}/table/{tableName}/unreferenced-paths

 where `{databaseName}` and `{tableName}` must be replaced by the database and table name the user wants to search for. So for example, if they wanted to check a table called `my_cool_table` in the database `my_cool_database`, they would go to 
 
    <base-url>/database/my_cool_database/table/my_cool_table/unreferenced-paths


### Filtering

The filtering available is the same in both endpoints, just keep in mind that in the first one it is referring to expired metadata and in the second one it is referring to unreferenced paths. 

The following table gives an overview of the filters available. It uses the metadata endpoint for the examples, but if the user wants to refer to paths they just have to replace `/database/{databaseName}/table/{tableName}/metadata` with `/database/{databaseName}/table/{tableName}/unreferenced-paths`.

| Filter name              | Description | Example |
|:----|:----:|:----|
| `path`                   | Given a path name, return if there is a path with that name | `/database/my_database_name`<br>`/table/my_table_name/metadata?path=`<br>`s3://mybucket/mydb/mytable/myfile.part0001` |
| `partition_name`         | Given a partition name, return the partitions with that name | `/database/my_database_name`<br>`/table/my_table_name/metadata`<br>`?partition_name=event_date=2020-01-01/event_hour=0/event_type=B` |
| `housekeeping_status`    | Given  a housekeeping status, return all partitions with that status (SCHEDULED, FAILED or DELETED) | `/database/my_database_name`<br>`/table/my_table_name/metadata`<br>`?housekeeping_status=SCHEDULED` |
| `lifecycle_type`         | Given a lifecycle type, return all partitions with that lifecycle (EXPIRED or UNREFERENCED). Note: currently this filter is useless as the first endpoint only returns EXPIRED metadata and the second only returns UNREFERENCED paths | `/database/my_database_name`<br>`/table/my_table_name/metadata`<br>`?lifecycle_type=EXPIRED` |
| `registered_before`      | Given a timestamp, it will return all partitions that were registered in Beekeeper before that time | `/database/my_database_name`<br>`/table/my_table_name/metadata`<br>`?registered_before=2021-02-25T15:33:05` |
| `registered_after`       | Given a timestamp, it will return all partitions that were registered in Beekeeper after that time | `/database/my_database_name`<br>`/table/my_table_name/metadata`<br>`?registered_after=2021-02-25T15:33:05` |
| `deleted_before`         | Given a timestamp, it will return all partitions that were deleted before that time | `/database/my_database_name`<br>`/table/my_table_name/metadata`<br>`?deleted_before=2021-02-25T15:33:05` |
| `deleted_after`          | Given a timestamp, it will return all partitions that were deleted after that time | `	/database/my_database_name`<br>`/table/my_table_name/metadata`<br>`?deleted_after=2021-02-25T15:33:05` |

Note: the `partition_name` filter is only available for the expired metadata endpoint, as this variable is not available in the paths.

### Metrics

Beekeeper currently supports Graphite and Prometheus metrics.

Prometheus metrics are exposed at `/actuator/prometheus`.

Graphite metrics require configuration to enable. If Graphite is enabled, both host and prefix are required. If they are not provided, the application will throw an exception and not start.

The following table shows the configuration that can be provided:

| Property             | Required | Description |
|:----|:----:|:----|
| `graphite.enabled`   | No       | Enable to produce Graphite metrics. Default value is `false`. |
| `graphite.host`   | If enabled     | Graphite host. |
| `graphite.prefix` | If enabled     | Prefix for Graphite metrics. |
| `graphite.port`   | No       | Graphite port. Default is `2003`. |
| `prometheus.prefix` | No       | Prefix for Prometheus metrics. Default value is `beekeeper`. |

## External links

Please see the [Housekeeping](https://github.com/HotelsDotCom/housekeeping) library for more information.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019-2020 Expedia, Inc.
