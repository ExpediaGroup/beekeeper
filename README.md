# Beekeeper

# Overview
Beekeeper is a service that schedules orphaned paths for cleanup.

![Beekeeper architecture](.README_images/architecture_diagram.png)

# End-to-end lifecycle example

- A Hive table is configured with the parameter `beekeeper.remove.unreferenced.data=true` to have orphaned data managed by Beekeeper.
- An operation is executed on the table that orphans some data at particular paths (alter partition, drop partition, etc.).
- Hive metastore events are emitted by the [Hive metastore listener](https://github.com/ExpediaGroup/apiary-extensions/tree/master/apiary-metastore-listener) as a result of the operation.
- Hive events are picked up from the queue by Beekeeper using [Apiary Receiver](https://github.com/ExpediaGroup/apiary-extensions/tree/master/apiary-receivers).
- Beekeeper processes these messages and schedules orphaned paths for deletion by adding them to a database.
- The scheduled paths are deleted by default in 3 days (but can be configured with a different value with table parameter `beekeeper.unreferenced.data.retention.period`).

# Running the applications

Beekeeper comprises two Spring Boot applications, `beekeeper-cleanup` and `beekeeper-path-scheduler-apiary`, which run independently of each other:

- `beekeeper-cleanup` periodically queries a database for paths to delete and performs deletions. 
- `beekeeper-path-scheduler-apiary` periodically polls an Apiary SQS queue for Hive metastore events and inserts S3 paths to be deleted into a database, scheduling them for deletion.

Both applications require configuration to be provided, see [Application configuration](#application-configuration) for details.

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

# Using Docker

Two Docker images are created during `mvn install` one for cleanup and one for path scheduling. 

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

# Application configuration
## Beekeeper Path Scheduler Apiary
| Property                            | Required | Description |
|:----|:----|:----|
| `apiary.queue-url`                  | Yes      | URL for SQS queue. |
| `apiary.cleanup-delay-property-key` | No       | Table parameter to use for Apiary listener. Default value is `beekeeper.unreferenced.data.retention.period`. |
| `beekeeper.default-cleanup-delay`   | No       | Default Time To Live (TTL) for orphaned paths in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format: only days, hours, minutes and seconds can be specified in the expression. Default value is `P3D` (3 days). |

## Beekeeper Cleanup
| Property             | Required | Description |
|:----|:----:|:----|
| `cleanup-page-size`  | No       | Number of rows that should be processed in one page. Default value is `500`. |
| `dry-run-enabled`            | No       | Enable to simply display the deletions that would be performed, without actually doing so. Default value is `false`. |
| `scheduler-delay-ms` | No       | Amount of time (in milliseconds) between consecutive cleanups. Default value is `300000` (5 minutes after the previous cleanup completes). |

## Metrics

Beekeeper currently supports Graphite metrics. If enabled, both host and prefix are required. If they are not provided, the application will throw an exception and not start.

| Property             | Required | Description |
|:----|:----:|:----|
| `graphite.enabled`   | No       | Enable to produce Graphite metrics. Default value is `false`. |
| `graphite.host`   | If enabled     | Graphite host. |
| `graphite.prefix` | If enabled     | Prefix for reported metrics. |
| `graphite.port`   | No       | Graphite port. Default is `2003`. |

# External links

Please see the [Housekeeping](https://github.com/HotelsDotCom/housekeeping) library for more information.
