# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - TBD
### Added
- Add `LifecycleEventType` enum in `beekeeper-core` to describe supported data lifecycles.
- Increase unit test coverage for `beekeeper-path-scheduler-apiary` package.
- Increase coverage for integration tests.

### Changed
- Refactored internals of `beekeeper-path-scheduler-apiary` to support generic data lifecycle scheduling.
    - Inserted additional workflow (handlers) between read & filter actions to support filters per lifecycle type.
    - MessageReaderAdapter now has additional logic to orchestrate the updated workflow.
    - Rename `PathEvent` to `BeekeeperEvent` to better reflect event types.
- Refactored internals of `beekeeper-path-scheduler` to support generic data lifecycle scheduling.
    - Rename `PathSchedulerService` to `UnreferencedPathSchedulerService` to differentiate types
- Refactored internals of `beekeeper-cleanup` to support generic data lifecycle data lifecycle deletions.
    - Refactor `PagingCleanupService` to be a generic orchestrator of Lifecycle handlers.

## [1.2.1] - TBD
### Changed
- `S3Client.listObjects()` to list all objects at a key with batching.  

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
