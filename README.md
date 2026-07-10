# SectorSync

[![CI](https://github.com/4fuu/SectorSync/actions/workflows/ci.yml/badge.svg)](https://github.com/4fuu/SectorSync/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

SectorSync is a dependency-light Rust middleware workspace for spatial,
real-time entity replication across large maps and multiple simulation
stations. It provides low-level, bounded primitives that can be embedded in a
game or simulation backend without imposing an engine, ECS, gateway process,
or cluster platform.

The current calendar-versioned line is the basic embedded SDK. Its public
boundaries are covered by workspace tests, executable integration examples,
strict Clippy, rustdoc, and a guarded performance acceptance runner.

## Features

- Uniform 3D cell indexing with deterministic AOI candidate queries.
- Exactly one authoritative owner per entity and read-only ghost semantics.
- Range, frustum, tag, cadence, priority, and byte-budget replication filters.
- Reusable caller-owned query and replication scratch buffers.
- Bounded command, event, client packet, and station packet queues.
- Binary command, acknowledgement, replication, barrier, dispatch, and station
  event frames.
- Low-level in-memory, reliable packet, and non-blocking UDP adapters.
- Tick-boundary barriers for freeze, snapshot, upgrade, and resume workflows.
- Runtime load sampling, conservative hotspot splitting, migration, and
  deterministic station scheduling.
- Guarded smoke benchmarks with machine-readable latency, bandwidth, queue,
  scheduler, and scratch-capacity fields.

## What SectorSync Is Not

SectorSync is not a game engine, ECS framework, full game-server framework, or
production cluster platform. The embedding application remains responsible for
game rules, authentication, anti-cheat, durable persistence, crash recovery,
service discovery, process orchestration, production cryptography, and GPU
workloads.

See [Production adapter boundaries](docs/production-adapters.md) for the exact
integration ownership model.

## Requirements

- Rust `1.88` or newer.
- Edition 2024 support.
- No operating-system service or external database is required.

## Installation

After the first registry release, use only the layers needed by the embedding
application:

```toml
[dependencies]
sectorsync-core = "=2026.7.10"
sectorsync-wire = "=2026.7.10"
sectorsync-transport = "=2026.7.10"
sectorsync-runtime = "=2026.7.10"
```

`sectorsync-core` can be used by itself. The higher layers build on it without
adding mandatory async runtimes, serialization frameworks, ECS frameworks, or
network services.

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
let grid = GridSpec::new(32.0).expect("valid grid");
let mut index = CellIndex::new(grid);
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

let candidates = index.query_sphere(position, 128.0);
assert_eq!(candidates, vec![handle]);
```

Run the complete validated command-to-replication flow:

```bash
cargo run -p sectorsync-bench --example sdk_flow
```

The integration order, ownership checks, bounded failures, barriers, migration,
and observability handoff are described in the
[SDK integration guide](docs/sdk-integration.md).

## Workspace

| Crate | Purpose | Publish target |
| --- | --- | --- |
| `sectorsync-core` | Spatial index, authority, entities, policies, AOI, components, replication planning, snapshots | Yes |
| `sectorsync-wire` | Bounded binary frame definitions, encoders, decoders, and replication frame builder | Yes |
| `sectorsync-transport` | In-memory, reliable packet, security-hook, and UDP transport adapters | Yes |
| `sectorsync-runtime` | Transport bridges, gateway/deployment routing, barriers, load sampling, scheduling, and migration | Yes |
| `sectorsync-bench` | Executable examples and guarded performance acceptance runner | No |

Published crates use one workspace version. The release order is core, wire,
transport, then runtime.

## Examples

Representative integration flows:

```bash
cargo run -p sectorsync-bench --example sdk_flow
cargo run -p sectorsync-bench --example replication_bridge
cargo run -p sectorsync-bench --example client_bridge
cargo run -p sectorsync-bench --example load_sampling
cargo run -p sectorsync-bench --example split_migration
cargo run -p sectorsync-bench --example barrier_upgrade
cargo run -p sectorsync-bench --example secure_command_ingress
```

The focused example-to-feature map is maintained in
[AGENTS.md](AGENTS.md#verification). Security examples use explicit test hooks
and are not production cryptography.

## Performance

Routine checks use the bounded smoke profile:

```bash
cargo run -q -p sectorsync-bench -- --profile=smoke
```

On the current development host, the 2,000-entity, 100-client, four-station,
five-tick smoke workload reports approximately 2 ms p99 tick time, selects 125
logical entity updates, estimates 4,000 payload bytes, and retains four
candidate handles in replication scratch. These figures are regression evidence,
not production capacity guarantees.

Compare identical workloads with:

```bash
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=sectorsync
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=full
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=room
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=naive-grid
```

Medium, large, and oversized manual profiles require `--allow-heavy`. See the
[performance acceptance matrix](docs/performance-acceptance.md) before changing
thresholds or running larger workloads.

## Documentation

- [SDK integration guide](docs/sdk-integration.md)
- [Performance acceptance matrix](docs/performance-acceptance.md)
- [Production adapter boundaries](docs/production-adapters.md)
- [Delivery status and explicit non-goals](docs/gaps.md)

Generate local API documentation with:

```bash
cargo doc --workspace --no-deps
```

## Development

The default quality gate is intentionally lightweight:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
cargo run -q -p sectorsync-bench -- --profile=smoke
git diff --check
```

GitHub Actions runs the same quality gate on pushes and pull requests, plus a
separate Rust 1.88 compatibility check. At 08:00 Asia/Hong_Kong each day, the
Automatic release workflow checks `main` for commits after the current release.
When work is pending, it assigns the current Asia/Hong_Kong calendar version,
reruns the quality gate, publishes the four library crates to crates.io in
dependency order, and creates a GitHub Release with source archives and
checksums. At most one version is released per local calendar day; later commits
wait for the next day. The workflow can also be started manually and resumes
safely when a crate version was already published by an earlier partial run.
Registry authentication uses crates.io Trusted Publishing; the repository does
not store a long-lived registry token.

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting changes. The
project preserves a narrow middleware boundary and requires examples or tests
for public SDK changes.

## Stability

SectorSync uses calendar versions in unpadded `YYYY.M.D` form, such as
`2026.7.10`. The three numeric fields remain valid for Cargo's SemVer parser,
but the date identifies a release and does not claim API compatibility.
Workspace crates depend on the exact same calendar version; consumers should
review release notes before upgrading. Authority, boundedness, and
explicit-state invariants remain compatibility commitments.

## Security

Report suspected vulnerabilities using the private process described in
[SECURITY.md](SECURITY.md). Do not open a public issue for an unpatched security
problem.

## License

SectorSync is available under the [MIT License](LICENSE).
