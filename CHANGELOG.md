# Changelog

All notable changes to SectorSync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
SectorSync uses `YYYY.MMDD.REVISION` calendar versions with an unpadded numeric
`MMDD` field.

## [Unreleased]

### Changed

- Split scheduling now supports fully reusable nested output slots for hotspot
  decisions and actions, plus borrowed execution/cooldown APIs, avoiding fresh
  reason and proposal buffers in steady-state passes.
- Hotspot cell split planning now offers reusable candidate/proposal storage and
  deterministic budgeted top-k selection; SplitScheduler exposes explicit
  scratch entry points without weakening conservative migration guards.
- Load-aware Station scheduling now offers caller-owned score/candidate scratch
  and deterministic budgeted top-k selection, while high budgets retain the
  existing full-sort path and owned APIs remain compatible.
- Event draining now rotates delayed events in bounded priority queues and
  offers caller-owned ready-output APIs, eliminating delayed and per-Station
  temporary vectors while preserving priority/FIFO order.
- Parallel multi-Station replication planning now offers borrowed `*_into`
  results backed by caller-owned output slots, avoiding repeated per-room and
  per-viewer result allocation in steady-state loops while preserving ordered
  owned-result APIs.
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
- Multi-cell sphere/AABB index updates now compare retained membership directly
  in deterministic grid order and allocate a new cell list only when membership
  actually changes.
- Replication packet encoding now uses a builder-limited, four-entity dirty-data
  sample to pre-size dense frames while conservatively falling back to normal
  buffer growth for sparse or empty frames.
- Single-viewer replication planning now offers reusable output variants for
  normal, cadence, priority, and priority/cadence paths; the transport bridge
  retains one plan slot and bounds reservation by actual candidate count.
- Budgeted priority planning now partitions and sorts only the deterministic
  top-k prefix when k is below half of eligible candidates, retaining full sort
  for large budgets.

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
