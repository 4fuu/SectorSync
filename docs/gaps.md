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
- Runtime barriers for tick-boundary freeze/snapshot/resume and frozen
  snapshot upgrade hooks.
- Conservative split scheduling, cell migration execution, deployment route
  metadata, runtime load sampling from station/index/router state plus explicit
  subscriber input, and bounded load-aware station scheduling.
- A committed performance acceptance matrix backed by machine-readable p50/p95/
  p99, command/replication/router/split/scheduler fields, smoke-safe baseline
  comparisons, and explicit heavy-profile opt-in.

## Delivery Gaps

### 1. Hotspot Calibration

Gap:

- Split scheduling and load-aware scheduling are conservative and bounded, but
  thresholds and scoring still need calibration against larger synthetic
  workloads and, eventually, production telemetry.

Why it matters:

- Incorrect thresholds can cause avoidable migration churn, under-splitting, or
  unfair station scheduling even if individual primitives are correct.

Completion evidence:

- Split tuning examples cover normal, warm, hot, cooldown, target capacity, and
  insufficient-improvement cases.
- Benchmarks report before/after pressure and migrated-cell/entity counts.
- AGENTS keeps heavyweight calibration explicitly gated.

### 2. Production Boundary Adapters

Gap:

- The core intentionally does not implement production auth, secret storage,
  NAT traversal, service discovery, durable cluster state, failover, or process
  supervision. The remaining work is to make sure adapter hooks are sufficient
  without importing those responsibilities.

Why it matters:

- SectorSync should be embeddable into many server stacks. Over-owning
  production infrastructure would make it less reusable and harder to keep fast.

Completion evidence:

- Transport/security traits remain bounded and dependency-light.
- Examples show how external adapters provide authentication/encryption and
  route discovery metadata without SectorSync owning those systems.
- Documentation names unsupported production responsibilities directly.

### 3. Documentation Finish

Gap:

- README and AGENTS are current enough for development, but not yet a polished
  SDK manual.

Why it matters:

- A usable middleware SDK needs stable mental models, executable examples, and
  explicit boundary rules, especially because low-level interfaces are exposed
  intentionally.

Completion evidence:

- README links to examples by use case rather than only listing commands.
- AGENTS names verification commands for every important SDK boundary.
- Public APIs have enough rustdoc for `cargo doc` to be useful.
- Gaps in this file are updated or removed as commits close them.

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

## Suggested Next Commit Order

1. Calibrate hotspot/scheduler examples against larger guarded workloads.
2. Polish production-boundary adapter examples without moving production
   ownership into SectorSync.

Each step should keep default verification lightweight and should leave heavier
work behind explicit command flags.
