# SectorSync

SectorSync is an embedded Rust library for high-performance, spatially aware
real-time entity replication.

It targets very large maps, many entities, many world instances, station-based
spatial ownership, low-latency command application, range/frustum culling,
interest management, adaptive update rates, hotspot splitting, and cross-station
event routing.

SectorSync is not a full game server framework. It does not own combat,
inventory, quests, economy, persistence, deployment, service discovery, or
crash recovery. Game-specific systems are expected to integrate through
station-local APIs, command/event hooks, custom components, and external
transport/routing adapters.

## Core Direction

- Embedded Rust library first, not a daemon.
- CPU-first high-performance core; GPU acceleration is external or future
  optional adapter work.
- In-memory runtime state, with snapshot/restore/migration APIs but no built-in
  durable storage.
- Fixed 3D cell topology with dynamic station ownership.
- Exactly one authoritative owner station per entity at any point in time.
- Read-only ghost entities may exist in neighboring stations for AOI, visibility,
  prewarming, and query acceleration.
- Station-local execution is single-owner and mostly lock-free; multiple
  stations can run in parallel.
- Cross-station events are ordered at tick boundaries and do not use distributed
  transactions.
- Client connectivity, gateway processes, cluster orchestration, and production
  transport are integration concerns outside the core library.

## Phase 1 Scope

Phase 1 should produce a usable core library and a benchmark simulator:

- `sectorsync-core`: entity IDs, station IDs, cell IDs, 3D spatial grid,
  ownership model, station runtime primitives, command/event envelopes,
  sync policies, snapshot/restore hooks, migration primitives, and runtime
  barrier support.
- `sectorsync-bench`: deterministic workloads, simulated clients, simulated
  stations, baseline modes, and performance reports.
- `sectorsync-wire`: wire/frame traits and default frame types.
- `sectorsync-transport`: transport traits and fake transport support.
- `sectorsync-runtime`: orchestration helpers for multi-station simulation.

The first implementation should stay resource-aware. The development machine is
not assumed to be a production benchmark host, so expensive tests must be
explicitly gated and default checks must stay lightweight.

## Workspace Layout

Current crates:

- `crates/sectorsync-core`: IDs, command envelopes, 3D spatial primitives,
  station-local entity storage, ghost/owner roles, dirty masks, compiled sync
  policies, custom component registry/storage, typed component codecs, schema
  helpers, cell indexing, interest queries, replication planning, bounded
  command/event queues, handoff transfer types, hotspot planning, barrier
  metadata, and snapshot metadata.
- `crates/sectorsync-wire`: frame shapes plus default binary encode/decode for
  replication frames with entity/component delta payloads, command
  acknowledgements, and barrier notifications.
- `crates/sectorsync-transport`: transport sink trait, batch packet API,
  byte-budget transport wrapper, and fake transport for tests/benchmarks.
- `crates/sectorsync-runtime`: in-process station collection helpers, a full
  runtime barrier controller for tick-boundary freeze/snapshot/resume flows, and
  an in-process entity migration executor built on two-phase handoff. It also
  includes a station event router and simple station scheduler.
- `crates/sectorsync-bench`: deterministic lightweight benchmark executable.

Useful commands:

```bash
cargo test --workspace
cargo run -p sectorsync-bench -- --profile=smoke
cargo run -p sectorsync-bench -- --profile=smoke --baseline=full
cargo run -p sectorsync-bench -- --profile=large --allow-heavy
```

The default smoke profile is intentionally small. Larger benchmark profiles must
be requested explicitly with `--allow-heavy`, for example `--profile=medium
--allow-heavy` or `--profile=large --allow-heavy`. Without `--allow-heavy`, a
heavy profile request stays on smoke-sized data and reports
`heavy_profile_denied=true`.

## Performance Targets

The project is judged by multiple hard metrics together, not by a single number:

- Large entity scale.
- Large simulated client scale.
- Bounded per-client interest sets.
- Stable station tick latency.
- Low command-to-apply latency.
- Efficient downlink bandwidth estimates.
- High command/event/frame throughput.
- Hotspot detection, splitting, downgrade, or aggregation behavior.

The benchmark suite should include simple baselines such as full broadcast,
room broadcast, and naive grid AOI so SectorSync's policy-driven approach can be
measured against simpler strategies.

## Non-Goals

- Full ECS game framework.
- Built-in business persistence.
- Built-in crash recovery or failover.
- Built-in process manager or cluster scheduler.
- Mandatory GPU dependency.
- Mandatory client SDK in Phase 1.
- Dynamic script/WASM/plugin hot loading in Phase 1.

## Development Status

This repository is being built iteratively. The README and `AGENTS.md` are
living documents and should be updated whenever the architecture, rules, or
implementation scope changes materially.

Initial status:

- Git repository initialized on branch `main`.
- Rust workspace scaffolded.
- Core low-level SDK types exist for station ownership, 3D spatial indexing,
  interest queries, policy tables, replication planning, event queues, barriers,
  snapshots, commands, and fake transport integration.
- Runtime barrier controller can request scoped barriers, wait for station tick
  alignment, freeze, export snapshots, and resume.
- Two-phase owner handoff primitives support target ghost prewarming, incoming
  owner commit, and source downgrade to short-lived ghost.
- Runtime migration executor can move an authoritative entity between in-process
  stations while leaving the old station with a short-lived ghost.
- Bounded command queues support priority ordering and barrier-aware
  buffer/reject/drain behavior.
- Custom component registry and sparse blob storage allow external systems to
  register game-owned data without forcing a full ECS framework.
- Typed component codecs and schema helpers support compact user-defined
  component encoding without forcing a serialization framework.
- Wire codec supports binary encode/decode for replication, command ACK, and
  barrier frames. Replication frames can carry concrete entity/component deltas.
- Transport SDK supports packet batches and byte-budget enforcement wrappers.
- Runtime event router queues cross-station events by target station and drains
  events once their target tick is ready.
- Hotspot planner evaluates station/cell load samples and proposes high-pressure
  cells for external schedulers to move.
- Smoke benchmark runs through planning, frame encoding, fake transport, and
  hotspot report fields. It also reports command enqueue/apply counts,
  command latency in ticks, max queue depth, payload entity/component delta
  counts, tick timing estimates, threshold checks, and an aggregate
  `benchmark_ok` verdict.

Not complete yet:

- Automatic station ownership split execution and migration scheduling.
- Multi-station scheduler and bounded cross-station transport integration beyond
  core queue primitives.
- Generated schema helpers.
- Real transport adapters.
- Large-scale benchmark validation against the stated hard metrics.
