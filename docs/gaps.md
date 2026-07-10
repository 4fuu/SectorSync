# SectorSync Delivery Gap Register

This document tracks the remaining gaps between the current implementation and
the intended deliverable: a high-performance embedded Rust middleware for
large-map, multi-station, real-time entity synchronization.

It is not a product wish list. Items here should either protect SectorSync's
middleware boundary, prove performance/latency/resource claims, or make the SDK
usable from external game/business systems without turning SectorSync into a
game engine.

## Current Shape

Already implemented and covered by examples or tests:

- Core station/entity ownership with exactly one authoritative owner per
  entity and read-only ghost copies.
- 3D spatial primitives, station-local cell indexing, AOI candidate queries,
  range/frustum/tag visibility filters, and reusable scratch buffers.
- Compiled sync policies, adaptive replication cadence, budget-aware priority
  selection, replication planning, replication frame building, and bounded
  replication send/ACK tracking.
- Bounded command/event queues, cross-station event frames, station event
  routing, station-to-station packet transport, and reliable station packet
  helpers.
- Gateway/session primitives, low-level client command ingress, command ACKs,
  gateway command pipeline, gateway-to-deployment dispatch metadata, and
  bounded client/gateway transport bridges.
- A cohesive SDK integration guide and executable command-to-replication flow
  covering external validation, bounded failure handling, barrier/migration
  sequencing, and observability handoff.
- Production adapter guidance maps authentication/cipher, transport, key
  lifecycle, route discovery, persistence, and GPU hooks to external owners,
  backed by bounded local security and deployment examples.
- Runtime barriers for tick-boundary freeze/snapshot/resume and frozen
  snapshot upgrade hooks.
- Conservative split scheduling, cell migration execution, deployment route
  metadata, runtime load sampling from station/index/router state plus explicit
  subscriber input, and bounded load-aware station scheduling.
- Deterministic hotspot calibration covers Normal/Warm/Hot classification,
  conservative scheduler guards, before/after pressure, and proposed/actual
  moved cell/entity counts while keeping heavier calibration explicitly gated.
- A committed performance acceptance matrix backed by machine-readable p50/p95/
  p99, command/replication/router/split/scheduler fields, smoke-safe baseline
  comparisons, and explicit heavy-profile opt-in.

## Delivery Status

No SDK-blocking delivery gaps remain for the basic embedded deliverable.

Completion evidence:

- README provides a use-case map to the integration, replication, barrier,
  load/migration, production-adapter, and performance workflows.
- `docs/sdk-integration.md`, `docs/performance-acceptance.md`, and
  `docs/production-adapters.md` define integration order, error handling,
  verification gates, and external ownership boundaries.
- AGENTS names lightweight verification commands for each important SDK
  boundary and keeps heavy calibration behind explicit `--allow-heavy`.
- `cargo doc --workspace --no-deps` completes without rustdoc warnings.
- Executable examples and integration tests cover the recommended external
  usage flows while core modules retain low-level middleware ownership.

## Explicit Non-Gaps

These are intentionally outside SectorSync unless implemented as optional
external adapters:

- Game business logic, combat, inventory, quest, economy, or gameplay ECS.
- Built-in account authentication, anti-cheat, matchmaking, or reconnect loops.
- Durable persistence, crash recovery, backups, or failover orchestration.
- Process manager, service discovery, cloud API integration, or cluster
  scheduler.
- Built-in GPU kernels, GPU memory/resource scheduling, or mandatory
  accelerator runtime.
- Dynamic script/WASM/plugin hot loading inside the core runtime.

## Future Work

Future changes should be driven by integration feedback, production-specific
adapters, or explicitly guarded calibration. They must not move the explicit
non-gaps above into SectorSync core by default.
