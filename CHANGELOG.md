# Changelog

All notable changes to SectorSync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
SectorSync uses `YYYY.MMDD.REVISION` calendar versions with an unpadded numeric
`MMDD` field.

## [Unreleased]

### Changed

- Added an explicit revision field so multiple releases can be published on
  the same Asia/Hong_Kong calendar day.
- Replication transport bridges now retain planning scratch and encode dirty
  components directly into binary packets without materializing intermediate
  entity/component delta trees.
- The guarded many-room benchmark now separates planning and encoding latency
  and can vary per-room entities, dirty ratio, and component payload size
  independently from player count.
- Parallel replication scratch is bounded by configured worker count instead of
  Station batch count, and reusable batch output APIs retain viewer plan/entity
  capacity across planning calls.
- Station, spatial index, and sparse component columns support explicit capacity
  reservation, with retained-capacity signals and A/B coverage in the many-room
  benchmark.
- Cell-index updates now skip allocation and index mutation when point entities
  remain in the same cell, with an observable update result and guarded
  many-room movement A/B coverage.
- Point entities crossing cells now retain their entity-cell membership storage
  and update it in place instead of removing and rebuilding the entity index
  mapping.
- Component storage now supports validated in-place slice updates and
  caller-owned typed encoding scratch, avoiding repeated blob-buffer allocation
  on high-frequency component writes.

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
