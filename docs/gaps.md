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
- Runtime barriers for tick-boundary freeze/snapshot/resume and frozen
  snapshot upgrade hooks.
- Conservative split scheduling, cell migration execution, deployment route
  metadata, and bounded load-aware station scheduling.
- Resource-guarded benchmarks with smoke defaults and explicit heavy-profile
  opt-in.

## Delivery Gaps

### 1. Runtime Load Sampling

Gap:

- Hotspot planning and load-aware station scheduling can consume
  `StationLoadSample`, but the committed SDK still needs a documented,
  verified path that derives those samples from runtime structures such as
  station storage, spatial indexes, event queues, and caller-provided subscriber
  counts.

Why it matters:

- Integrators should not have to hand-author load samples just to use hotspot
  splitting or scheduler prioritization.
- Sampling must stay middleware-level: no game semantics, hidden threads,
  process placement, OS metrics, GPU execution, or cluster scheduler ownership.

Completion evidence:

- Public runtime sampler API.
- Unit test covering owned/ghost counts, per-cell classification, queued event
  pressure, and subscriber count aggregation.
- Executable example showing sampling feeding load-aware scheduling.
- README and AGENTS updates naming the example and boundary rules.

### 2. SDK Flow Hardening

Gap:

- Many low-level pieces exist, but the recommended external integration flow is
  still spread across many examples rather than one cohesive SDK guide.

Why it matters:

- External business systems need a clear path for: command validation before
  SectorSync, data-driven station-local updates, replication planning,
  transport bridge use, barrier use, migration, and observability handoff.

Completion evidence:

- A concise SDK integration guide or expanded `sdk_flow` example that shows the
  intended order of operations.
- Error-handling expectations for bounded queues, transport rejections, barrier
  states, and missing routes.
- Clear examples of what external systems own: auth, anti-cheat, persistence,
  matchmaking, process orchestration, GPU batches, and business ECS.

### 3. Performance Acceptance Matrix

Gap:

- The benchmark runner has guarded profiles and baselines, but the project does
  not yet have a committed acceptance matrix that maps performance targets to
  concrete commands, expected fields, and pass/fail thresholds.

Why it matters:

- "High performance" needs evidence across entity scale, client scale, interest
  set bounds, tick latency, command latency, bandwidth, event throughput,
  replication throughput, split behavior, and resource guard behavior.

Completion evidence:

- Documented benchmark matrix with smoke-safe defaults and optional heavy runs.
- Recorded fields for p50/p95/p99 tick time, command apply latency, selected
  replication candidates, encoded entities/components, bytes, queue drops,
  router pressure, and split/scheduler decisions.
- Baseline comparison commands for full broadcast, room broadcast, and naive
  grid AOI.
- No heavy benchmark path runs implicitly without `--allow-heavy`.

### 4. Hotspot Calibration

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

### 5. Production Boundary Adapters

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

### 6. Documentation Finish

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

1. Finish runtime load sampling and document the example.
2. Add or expand a cohesive SDK integration flow.
3. Add a benchmark acceptance matrix document and make smoke output map to it.
4. Calibrate hotspot/scheduler examples against larger guarded workloads.
5. Polish production-boundary adapter examples without moving production
   ownership into SectorSync.

Each step should keep default verification lightweight and should leave heavier
work behind explicit command flags.
