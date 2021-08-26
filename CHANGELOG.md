# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.4.1] - UNRELEASED
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
