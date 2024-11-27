# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.6.0] - 2024-10-27
### Added
- Added filter for Iceberg tables in `beekeeper-scheduler-apiary` to prevent scheduling paths and metadata for deletion. 
- Added `IcebergValidator` to ensure Iceberg tables are identified and excluded from cleanup operations.

## [3.5.7] - 2024-10-25
### Changed
- Added error handling for bad requests with incorrect sort parameters.
- Added automatic pagination handling in the `/unreferenced-paths` endpoint for improved Swagger documentation. 
- Updated the Maven Central release workflow to run exclusively from the main branch.

## [3.5.6] - 2024-06-14
### Fixed
- Added aws-sts-jdk dependency in `beekeeper-metadata-cleanup`, `beekeeper-path-cleanup`, `beekeeper-scheduler-apiary` to solve IRSA unable to assume role issue.

## [3.5.5] - 2023-11-10
### Fixed
- Fixed paged API response by updating 'MetadataResponseConverter' and 'PathResponseConverter' to pass complete information about the number of pages and elements to the response Page.

## [3.5.4] - 2023-09-14
### Fixed
- Added localisation normalization so locations like `s3:/a/b` and `s3:/a/b/` will be considered the same and path won't be scheduled for deletion.

## [3.5.3] - 2023-03-08
### Changed
- Upgrade `Springboot` from `2.4.4` to `2.7.9`.
- Upgrade `Spring framework` from `5.3.5` to `5.3.25`.
- Migrate using `springfox` to `springdoc` due to incompatibilities with `spring boot 2.6+` actuators.
- Removed `micrometer` version from `beekeeper-vacuum-tool` to be the same as managed by `beekeeper/pom.xml`'s dependencies.
- Upgrade `specification-arg-resolver` version from `2.6.1` to `2.18.1` to be compatible with `springdoc`.

## [3.5.2] - 2023-01-06
### Fixed
- `cleanUpOldDeletedRecords` status check to not delete `DISABLED` entries immediately. 

## [3.5.1] - 2023-01-06
### Fixed
- Check for `expired` property before disabling tables from TTL feature rather than the `unreferenced` property.

## [3.5.0] - 2023-01-04
### Fixed
- Allow cleanup delays to be specified in months or years as well as smaller units by combining the Period and Duration specifications.
### Changed
- Version of MySQL container from `8.0.15` to `8.0.26` in the integration tests.

## [3.4.15] - 2023-01-04
### Fixed
- Throw exception if `cleanupDelay` can't be parsed instead of returning the default value.
### Changed
- Don't return records for cleanup after 10 attempts have been reached. 
- Added additional checks for the number of levels in table and partition paths so that invalid paths are not scheduled for deletion.

## [3.4.14] - 2022-11-25
### Fixed
- Return Slice instead of Page in queries to avoid scanning the whole table for the total number of pages. Details [here](https://www.baeldung.com/spring-data-jpa-pagination-sorting#paginate).

## [3.4.13] - 2022-11-23
### Fixed
- Removed `order by` from the query for getting records to clean up in order to speed up processing.

## [3.4.12] - 2022-10-19
### Fixed
- Error when deleting over 1000 files in a single request, added logic to break down the request in smaller parts
### Changed
- Upgraded `com.amazonaws` dependency version to `1.12.311` (was `1.11.960`).

## [3.4.11] - 2022-05-09
### Fixed
- S3 tests by using `test-containers` instead of `localstack-utils`.
### Changed
- Upgraded `test-containers` to `1.17.1` (was `1.15.2`).

## [3.4.10] - 2022-01-12
### Fixed
- If a table gets deleted before Beekeeper is scheduled to do so, its entries in the `housekeeping_metadata` table will be disabled. 

## [3.4.9] - 2022-01-12 (Released without changes)

## [3.4.8] - 2021-12-15
### Changed
- Upgraded `log4j2` to `2.16.0` (was `2.15.0`).

## [3.4.7] - 2021-12-15
### Fixed
- Added missing @Transactional annotation for MetadataDisableTablesService.

## [3.4.6] - 2021-12-14
### Fixed
- Added check for beekeeper property before performing the cleanup in the metadata service.
### Changed
- Upgraded `log4j2` to `2.15.0` because of log4j security issue.

## [3.4.5] - 2021-11-24
### Added
- Cleanup job for old rows in the `housekeeping_path` and `housekeeping_metadata` tables.

## [3.4.4] - 2021-11-11
### Fixed
- Fixed DB migration script version.

## [3.4.3] - 2021-11-11
### Changed
- DB migration to change indexes for the `housekeeping_path` and `housekeeping_metadata` tables.

## [3.4.2] - 2021-11-10
### Changed
- DB migration to add indexes for the `housekeeping_path` and `housekeeping_metadata` tables.

## [3.4.1] - 2021-08-27
### Added
- Added the `swagger` endpoint to the `beekeeper-api` module.

## [3.4.0] - 2021-08-18
### Added
- Added a `GET /unreferenced-paths` endpoint to the `beekeeper-api`.

## [3.3.0] - 2021-08-12
### Added
- Added a `GET /metadata` endpoint to the `beekeeper-api`.
### Changed
- Updated `eg-oss-parent` to version `2.4.0` (was `2.3.2`).
- Updated `snakeyaml` to version `1.27` (was `1.24`).
- Updated `mockito.version` to version `3.11.2` (was `3.9.0`).

## [3.2.0] - 2021-07-21
### Added
- Added `beekeeper-api` module.
### Changed
- Updated `aws.version` to version `1.11.960` (was `1.11.532`) in `beekeeper-cleanup`.
- Updated `eg-oss-parent` to version `2.3.2` (was `2.3.1`).
- Updated `localstack-utils` to version `0.2.12` (was `0.2.7`).

## [3.1.0] - 2021-02-22
### Changed
- Updated `eg-oss-parent` to version `2.3.0` (was `1.3.1`).
- Docker images are now built using Jib plugin.
- Excluded `javax.servlet` dependency from `hadoop-mapreduce-client` to avoid version conflict.

## [3.0.2] - 2021-02-09 [YANKED]
### Fixed
- Set lifecycle type to `UNREFERENCED` for paths picked up by Vacuum tool.

## [3.0.1] - 2020-09-10
### Fixed
- Integration tests for asserting metrics in metadata cleanup.

## [3.0.0] - 2020-09-08
### Added
- Add Time To Live (TTL) for all tables.
- DB migration for creating new `housekeeping_metadata` table.
### Changed
- Renamed `beekeeper-path-scheduler-apiary` module to `beekeeper-scheduler-apiary`.
- Renamed `beekeeper-path-scheduler` module to `beekeeper-scheduler`.
- Renamed `beekeeper-assembly-path-scheduler-apiary` module to `beekeeper-assembly-scheduler-apiary`.
- Docker image name changed from `beekeeper-path-scheduler-apiary` to `beekeeper-scheduler-apiary`
- DB migration to rename `path_status` column to `housekeeping_status` in the `path` table.
- DB migration to rename `path` table to `housekeeping_path` table.

## [2.1.0] - 2020-04-29
### Added
- `beekeeper-vacuum-tool` module.

## [2.0.0] - 2020-02-20
### Added
- Add `LifecycleEventType` enum in `beekeeper-core` to describe supported data Lifecycles.
- Prometheus support.

### Changed
- Refactored internals of `beekeeper-path-scheduler-apiary` to support generic Lifecycle scheduling.
    - Inserted additional workflow (handlers) between read & filter actions to support filters per Lifecycle type.
    - MessageReaderAdapter now has additional logic to orchestrate the updated workflow.
    - Renamed `PathEvent` to `BeekeeperEvent` to better reflect event types.
- Refactored internals of `beekeeper-path-scheduler` to support generic data Lifecycle scheduling.
    - Renamed `PathSchedulerService` to `UnreferencedPathSchedulerService` to differentiate types.
- Refactored internals of `beekeeper-cleanup` to support generic data Lifecycle deletions.
    - Refactored `PagingCleanupService` to be a generic orchestrator of Lifecycle handlers.
- `S3Client.listObjects()` to list all objects at a key with batching.  
- `eg-oss-parent` version updated to 1.3.1 (was 1.1.0).

## [1.2.0] - 2020-01-06
### Adding
- `TimedTaggable` annotation to time and report table level metrics.
- `BytesDeletedReporter` to report bytes deleted at a table level.
- `WhitelistedListenerEventFilter` to filter events unless listed in `beekeeper.hive.events.whitelist`.
- Fix for S3 paths which require encoding.

## [1.1.6] - 2019-11-27
### Changed
- Handling cleanup for `s3a` and `s3n` paths.

## [1.1.5] - 2019-11-18
### Changed
- Using default deletion delay if table parameter is configured incorrectly.

## [1.1.4] - 2019-11-07
### Changed
- Parent pom version to 1.1.0 (was 1.0.0).
- `MeterRegistry` to `GraphiteMeterRegistry` so that Spring metrics use Beekeeper's `GraphiteConfig` and not the default config.

### Added
- Health check endpoints.

## [1.1.3] - 2019-09-05
### Updated
- First release: producing two docker images with single version tag.
