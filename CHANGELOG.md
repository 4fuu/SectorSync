# Changelog

All notable changes to SectorSync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
SectorSync uses `YYYY.MMDD.REVISION` calendar versions with an unpadded numeric
`MMDD` field.

## [Unreleased]

### Changed

- Gateway session storage now retains ordered lookup for small tables and
  promotes once to hash lookup at 1,024 sessions, preserving route, admission,
  reconnect, expiry, capacity, and statistics semantics across migration.
- Core Command and Station Event priority queues now allocate slots on demand
  instead of reserving every configured maximum at construction; per-priority
  and aggregate retained capacities are observable for multi-room host metrics.
- `BudgetedTransport` batch validation now computes aggregate bytes and records
  the first oversized packet in one metadata scan while preserving aggregate
  budget error priority and all-or-nothing forwarding to the inner transport.
- In-memory Client batch sends now reuse one shared-state lock for bounded
  64-packet segments instead of locking once per packet, preserving ordered
  partial commits while preventing unbounded lock hold times.
- In-memory Client sends now validate target capacity and enqueue through one
  mutable endpoint lookup instead of probing the same target map twice per
  packet; error ordering, backpressure, and statistics remain unchanged.
- In-memory Client and Station endpoint maps now retain ordered lookup for
  small registries and promote once to hash lookup at 2,048 entries, improving
  large single-process registries without penalizing normal small rooms.
- In-memory Client and Station transport queues now allocate packet slots on
  demand instead of reserving each configured queue maximum at registration;
  retained per-queue and aggregate capacities are observable for host metrics.
- Security replay and reliable Client/Station duplicate-suppression windows now
  choose their bounded lookup index from configured capacity: compact ordered
  sets for small histories and faster hash lookup for histories of 256 or more.
- Client transport bridges now offer one fallible mixed-frame visitor pump for
  ACK, borrowed replication, and barrier frames without nested replication
  materialization; owned mixed pumping remains compatible.
- Reliable Client and Station receive endpoints now borrow-decode frames and
  reuse each inbound wire Vec as the delivered payload instead of allocating a
  second payload buffer; compatible owned decoders remain available.
- Station restore now preallocates record, generation, and entity-id index
  storage from snapshot size and offers tracked capacity observations.
- Station and frozen barrier snapshot export now support caller-owned reusable
  entity buffers and Station slots while retaining compatible owned snapshots.
- Split schedule execution now offers reusable ownership-update, nested
  migration-report, and shared migration working storage across multi-room
  passes while retaining compatible owned execution reports.
- Cell migration now scans borrowed cell membership instead of copying one
  handle Vec per cell, and offers caller-owned reusable deduplication,
  candidate, and report storage for repeated migration passes.
- Component storage now supports caller-owned output reuse when removing every
  component for an entity, plus a discard-only cleanup path when removed values
  are not needed; the compatible owned removal API remains available.
- Gateway client transport now offers a compact ingress pump that moves encoded
  ACK buffers directly into transport and returns fixed-size counts instead of
  cloning ACKs into retained per-command reports.
- Replication receive bridges now offer fallible borrowed-frame visitors that
  preserve source/target validation and cumulative statistics while avoiding
  nested owned frame materialization for immediate client-world application.
- Replication wire decoding now offers fully validated borrowed frame, entity,
  component, and payload views for immediate allocation-free consumption while
  retaining a compatible single-pass owned `RuntimeFrame` decoder.
- UDP client and Station adapters now expose borrowed non-blocking receive
  views backed by their existing reusable datagram buffers; owned receiver
  traits remain compatible for queued or transferred packets.
- Packet security opening now supports borrowed envelope decoding and
  caller-owned plaintext scratch, removing per-packet payload/tag parsing
  allocations and allowing high-rate receive paths to retain plaintext output.
- Packet security sealing now supports caller-owned encrypted-payload and tag
  scratch plus borrowed envelope encoding, avoiding per-packet internal buffer
  allocation while keeping authenticator/cipher providers external.
- `StationSet` and `StationIndexSet` now preserve ordered iteration while using
  an adaptive Station-id index for larger registries; explicit capacity APIs
  reserve ordered slots and lookup storage for multi-room runtimes.
- Reliable Client and Station senders now maintain per-peer in-flight counts,
  replacing full packet-map scans during window admission and count queries;
  ACK, timeout, and saturated-sequence replacement keep counts synchronized.
- Reliable Client and Station data frames can now encode borrowed payloads
  directly, and retry scans support caller-owned key scratch; sender retries no
  longer clone in-flight packets or payloads while compatibility APIs remain.
- Periodic load sampling now offers caller-owned subscriber aggregation,
  occupancy, Station sample, and per-cell output storage; the spatial index also
  supports reusable deterministic occupancy output while owned APIs remain
  compatible.
- Deployment stale-node marking now mutates node state and route epochs in one
  allocation-free ordered-map scan; the ordered stale-id query remains available
  for external control planes that need it.
- Gateway disconnected-session expiry now removes stale records in one
  allocation-free ordered-map retain scan instead of collecting client ids and
  performing a second removal pass.
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
