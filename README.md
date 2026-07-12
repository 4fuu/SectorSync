# SectorSync

[![CI](https://github.com/4fuu/SectorSync/actions/workflows/ci.yml/badge.svg)](https://github.com/4fuu/SectorSync/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

SectorSync is dependency-light Rust middleware for spatial, real-time entity
replication across large maps and multiple simulation stations. It provides
bounded, embeddable primitives without imposing a game engine, ECS, gateway
process, async runtime, or cluster platform.

## Capabilities

- Deterministic 3D cell indexing, AOI queries, visibility hooks, and
  allocation-light movement updates.
- Single-owner authority, read-only ghosts, handoff, snapshots, barriers, and
  cell migration primitives.
- Compiled cadence, priority, visibility, and byte-budget replication planning
  with caller-owned reusable scratch.
- Bounded command, event, replication, gateway, deployment-route, and tracking
  state with explicit backpressure.
- Binary wire frames plus in-memory, reliable-packet, security-hook, and
  non-blocking UDP adapters.
- Runtime load sampling, conservative hotspot splitting, and deterministic
  station scheduling.
- Adaptive small/large collection paths and reusable buffers for multi-room,
  single-process deployments.
- Guarded, machine-readable benchmarks for latency, bytes, queue pressure,
  scheduler decisions, and retained capacity.

SectorSync owns synchronization mechanics, not game semantics. Authentication,
anti-cheat, matchmaking, gameplay state, persistence, process orchestration,
service discovery, production cryptography, and GPU execution remain with the
embedding application. See [Production adapter boundaries](docs/production-adapters.md).

## Requirements

- Rust 1.88 or newer with Edition 2024 support.
- No operating-system service, database, or mandatory runtime dependency.

## Installation

Use only the layers needed by the application:

```toml
[dependencies]
sectorsync-core = "=2026.712.0"
sectorsync-wire = "=2026.712.0"
sectorsync-transport = "=2026.712.0"
sectorsync-runtime = "=2026.712.0"
```

`sectorsync-core` works independently. Higher layers add wire, transport, and
runtime integration without introducing a mandatory ECS, async runtime, or
network service.

Optional performance features are explicit:

```toml
sectorsync-core = { version = "=2026.712.0", features = ["simd"] }
sectorsync-runtime = { version = "=2026.712.0", features = ["parallel"] }
```

`simd` enables the safe range-only SIMD candidate path. `parallel` exposes a
bounded replication pool and deterministic parallel station planning. Neither
feature creates hidden threads in the default build.

## Quick Start

```rust
use sectorsync_core::prelude::{
    Bounds, CellIndex, EntityId, GridSpec, InstanceId, NodeId, PolicyId,
    Position3, Station, StationConfig, StationId,
};

let mut station = Station::new(StationConfig {
    station_id: StationId::new(1),
    node_id: NodeId::new(1),
    instance_id: InstanceId::new(1),
    tick_rate_hz: 20,
});
let mut index = CellIndex::new(GridSpec::new(32.0).expect("valid grid"));
let position = Position3::new(64.0, 0.0, 64.0);

let handle = station
    .spawn_owned(
        EntityId::new(42),
        position,
        Bounds::Point,
        PolicyId::new(1),
    )
    .expect("entity should spawn");
index.upsert(handle, position, Bounds::Point);

assert_eq!(index.query_sphere(position, 128.0), vec![handle]);
```

Run the complete validated command-to-replication flow:

```bash
cargo run -p sectorsync-bench --example sdk_flow
```

The [SDK integration guide](docs/sdk-integration.md) covers bootstrap, per-tick
ordering, ownership, bounded failures, barriers, migration, and observability.

## Workspace

| Crate | Purpose |
| --- | --- |
| `sectorsync-core` | Spatial index, authority, components, policy, replication, snapshots |
| `sectorsync-wire` | Bounded binary frame encoding and decoding |
| `sectorsync-transport` | In-memory, reliable-packet, security-hook, and UDP adapters |
| `sectorsync-runtime` | Bridges, gateway routing, barriers, load sampling, scheduling, migration |
| `sectorsync-bench` | Executable examples and guarded performance acceptance runner |

The four library crates share one exact workspace version and publish in
dependency order: core, wire, transport, then runtime.

## Performance

Run the bounded smoke acceptance profile:

```bash
cargo run -q -p sectorsync-bench -- --profile=smoke
```

The benchmark emits machine-readable latency, selection, byte, queue,
replication, scheduler, threshold, and `benchmark_ok` fields. Medium, large,
and oversized manual workloads require explicit `--allow-heavy` opt-in.

The guarded many-room example models independent room instances and sequential
single-process work:

```bash
cargo run --release -q -p sectorsync-bench --example many_rooms
```

The deterministic gameplay-shaped scenario adds active/idle/hot room mixes,
commands, movement, component changes, projectiles, events, frame transport,
client decode, ACK tracking, and bounded room recreation:

```bash
cargo run --release -q -p sectorsync-bench --example dynamic_gameplay
```

On the current development host, its default 500-room workload has demonstrated
30 Hz headroom for spatial planning and direct delta encoding. This excludes
gameplay, room lifecycle, persistence, matchmaking, and kernel networking, so
it is regression evidence rather than a production capacity guarantee.

Benchmark profiles, baselines, A/B commands, measurements, and interpretation
rules live in the [performance acceptance matrix](docs/performance-acceptance.md).

## Documentation

Start with the [documentation index](docs/README.md):

- [SDK integration guide](docs/sdk-integration.md): application ownership,
  bootstrap, tick flow, failures, and executable examples.
- [Performance acceptance matrix](docs/performance-acceptance.md): guarded
  profiles, thresholds, A/B evidence, and heavy calibration.
- [Production adapter boundaries](docs/production-adapters.md): security,
  transport, routing, persistence, and infrastructure ownership.
- [Delivery status](docs/gaps.md): completed SDK scope, explicit non-goals, and
  criteria for future work.

Generate API documentation with:

```bash
cargo doc --workspace --all-features --no-deps
```

## Development

The release-quality gate is:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
cargo run -q -p sectorsync-bench -- --profile=smoke
git diff --check
```

See [CONTRIBUTING.md](CONTRIBUTING.md) before submitting changes. Public SDK
changes require focused tests and a relevant executable example. Routine
verification stays smoke-safe; heavier performance runs must be deliberate.

## Versioning

SectorSync uses calendar versions in `YYYY.MMDD.REVISION` form with an unpadded
numeric `MMDD` field. The date identifies a release and does not claim semantic
API compatibility. Workspace dependencies remain exact and equal; review the
[release notes](CHANGELOG.md) before upgrading.

Authority, boundedness, and explicit-state invariants remain compatibility
commitments.

## Security and License

Report vulnerabilities through the private process in [SECURITY.md](SECURITY.md),
not a public issue. SectorSync is available under the [MIT License](LICENSE).
