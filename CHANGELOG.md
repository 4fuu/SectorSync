# Changelog

All notable changes to SectorSync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
SectorSync uses `YYYY.MMDD.REVISION` calendar versions with an unpadded numeric
`MMDD` field.

## [Unreleased]

### Changed

- Added an explicit revision field so multiple releases can be published on
  the same Asia/Hong_Kong calendar day.

## [2026.7.10] - 2026-07-10

### Changed

- Adopted calendar versions for daily releases and exact matching versions
  across the four published workspace crates.
- Kept `0.1.0` as the immutable historical crates.io and GitHub release.

## [0.1.0] - 2026-07-10

### Added

- Initial embedded SDK crates for core replication, wire frames, bounded
  transports, and runtime integration.
- Guarded benchmark runner, executable integration examples, and performance
  acceptance documentation.
- MIT license and release-oriented project metadata.

### Changed

- Spatial AOI queries adapt between direct grid probing and deterministic
  occupied-cell scans while retaining caller-owned scratch capacity.
- Workspace quality gate now passes strict Clippy across all targets.
