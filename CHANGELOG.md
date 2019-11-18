# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
