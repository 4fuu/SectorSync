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
  policies, cell indexing, interest queries, replication planning, event queues,
  barrier metadata, and snapshot metadata.
- `crates/sectorsync-wire`: frame shapes and a default binary frame encoder.
- `crates/sectorsync-transport`: transport sink trait and fake transport for
  tests/benchmarks.
- `crates/sectorsync-runtime`: in-process station collection helpers.
- `crates/sectorsync-bench`: deterministic lightweight benchmark executable.

Useful commands:

```bash
cargo test --workspace
cargo run -p sectorsync-bench -- --profile=smoke
cargo run -p sectorsync-bench -- --profile=smoke --baseline=full
```

The default smoke profile is intentionally small. Larger benchmark profiles must
be requested explicitly, for example `--profile=medium` or `--profile=large`.

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
- Smoke benchmark runs through planning, frame encoding, and fake transport.

Not complete yet:

- Full runtime barrier execution semantics.
- Two-phase owner handoff implementation.
- Hotspot detection and station ownership split execution.
- Multi-station scheduler and bounded cross-station queues beyond core queue
  primitives.
- Component registry and custom component codecs.
- Real transport adapters.
- Large-scale benchmark validation against the stated hard metrics.
