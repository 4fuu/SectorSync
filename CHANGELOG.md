# Changelog

All notable changes to SectorSync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
