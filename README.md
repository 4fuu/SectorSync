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
- Point updates skip unchanged work and relocate their entity-cell mapping in
  place when crossing cells.
- Multi-cell sphere/AABB updates compare retained membership without a temporary
  cell-list allocation.
- Exactly one authoritative owner per entity and read-only ghost semantics.
- Range, frustum, tag, cadence, priority, and byte-budget replication filters.
- Budgeted priority planning partitions deterministic top-k candidates instead
  of sorting the full eligible set when the send budget is small.
- Reusable caller-owned query and replication scratch buffers.
- Reusable single-viewer plan output across normal, cadence, and priority paths.
- Reusable parallel multi-Station output slots and selected-entity capacity.
- Explicit Station, spatial-index, and component-column capacity reservation.
- Reusable typed-component encoding scratch and in-place blob byte updates.
- Component entity cleanup supports reusable removed-value output and a
  discard-only path for high-frequency despawn loops.
- Dense replication frames use bounded dirty-data sampling to reduce output
  buffer growth, while sparse frames conservatively retain normal allocation.
- Replication receivers can iterate fully validated borrowed entity/component
  views without materializing nested owned frame or payload buffers.
- Replication receive bridges can visit borrowed frames immediately with
  separate propagation of transport/validation and caller application errors.
- Client bridges can visit mixed ACK, borrowed replication, and barrier frames
  in one validated allocation-light receive loop.
- Bounded command, event, client packet, and station packet queues.
- Gateway session expiry uses one allocation-free ordered-map retain scan.
- Gateway client ingress can move ACK buffers directly into transport and
  return compact counts when per-command reports are not required.
- Deployment stale-node marking updates state and route epochs in one
  allocation-free ordered-map scan.
- Event draining retains delayed priority queues in place and supports reusable
  caller-owned ready output across Stations and ticks.
- Binary command, acknowledgement, replication, barrier, dispatch, and station
  event frames.
- Low-level in-memory, reliable packet, and non-blocking UDP adapters.
- UDP client and Station adapters support borrowed receive views that reuse the
  configured datagram buffer for synchronous packet consumption.
- Reliable Client and Station senders encode borrowed payloads directly and
  support caller-owned retry scan scratch without cloning in-flight payloads.
- Reliable receivers borrow-decode frames and reuse inbound wire Vec storage
  for unique delivered payloads.
- Reliable sender window admission uses per-peer counters instead of scanning
  all in-flight packets as concurrent reliable windows grow.
- Packet security sealing supports caller-owned payload/tag scratch and borrowed
  envelope encoding while production algorithms and keys remain external.
- Packet security opening supports borrowed envelope decoding and caller-owned
  plaintext scratch for allocation-free steady-state receive output.
- Tick-boundary barriers for freeze, snapshot, upgrade, and resume workflows.
- Frozen multi-Station snapshots can retain Station slots and entity-record
  capacity across repeated in-memory exports.
- Snapshot restore preallocates Station entity/index storage and can report
  whether either capacity grew during insertion.
- Runtime load sampling, conservative hotspot splitting, migration, and
  deterministic station scheduling.
- Cell migration scans borrowed index membership and supports caller-owned
  deduplication, candidate, and report storage across repeated split passes.
- Split execution can retain ownership updates, nested migration reports, and
  shared migration scratch across actions and rooms.
- Periodic load sampling supports caller-owned subscriber, occupancy, Station,
  and per-cell scratch storage across scheduling windows.
- Ordered Station and spatial-index registries use allocation-free linear lookup
  for small sets and an adaptive ID index for larger multi-room collections.
- Load-aware Station scheduling supports caller-owned score/candidate scratch
  and deterministic top-k selection when the advancement budget is small.
- Hotspot split planning supports reusable cell/proposal storage and selects a
  deterministic top-k when only a bounded number of cells may move.
- Split scheduling can retain decision reasons, action proposals, candidate
  cells, and outer result slots, with borrowed execution and cooldown APIs.
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
sectorsync-core = "=2026.711.0"
sectorsync-wire = "=2026.711.0"
sectorsync-transport = "=2026.711.0"
sectorsync-runtime = "=2026.711.0"
```

`sectorsync-core` can be used by itself. The higher layers build on it without
adding mandatory async runtimes, serialization frameworks, ECS frameworks, or
network services.

Performance integrations are opt-in:

```toml
sectorsync-core = { version = "=2026.711.0", features = ["simd"] }
sectorsync-runtime = { version = "=2026.711.0", features = ["parallel"] }
```

`simd` enables the safe eight-lane range-only candidate path. `parallel` exposes
an explicitly constructed, bounded replication pool with deterministic station
batch planning and synchronous ordered batch mapping. Neither feature creates
threads or changes planner behavior in the default build.

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
cargo run -p sectorsync-bench --release --example replication_decode_borrowed
cargo run -p sectorsync-bench --release --example replication_decode_borrowed -- --owned
cargo run -p sectorsync-bench --release --example replication_receive_visit
cargo run -p sectorsync-bench --release --example replication_receive_visit -- --owned
cargo run -p sectorsync-bench --example client_bridge
cargo run -p sectorsync-bench --example load_sampling
cargo run -p sectorsync-bench --release --example load_sampling_reuse
cargo run -p sectorsync-bench --release --example load_sampling_reuse -- --fresh-output
cargo run -p sectorsync-bench --release --example station_registry_lookup
cargo run -p sectorsync-bench --release --example station_registry_lookup -- --full-scan
cargo run -p sectorsync-bench --example load_scheduler
cargo run -p sectorsync-bench --release --example station_schedule_reuse
cargo run -p sectorsync-bench --release --example station_schedule_reuse -- --fresh-output
cargo run -p sectorsync-bench --example split_migration
cargo run -p sectorsync-bench --release --example hotspot_split_reuse
cargo run -p sectorsync-bench --release --example hotspot_split_reuse -- --fresh-output
cargo run -p sectorsync-bench --release --example split_schedule_reuse
cargo run -p sectorsync-bench --release --example split_schedule_reuse -- --fresh-output
cargo run -p sectorsync-bench --example barrier_upgrade
cargo run -p sectorsync-bench --example secure_command_ingress
cargo run -p sectorsync-bench --release --example security_seal_reuse
cargo run -p sectorsync-bench --release --example security_seal_reuse -- --fresh-scratch
cargo run -p sectorsync-bench --release --example security_open_reuse
cargo run -p sectorsync-bench --release --example security_open_reuse -- --fresh-output
cargo run -p sectorsync-bench --example gateway_session
cargo run -p sectorsync-bench --release --example gateway_ack_ownership
cargo run -p sectorsync-bench --release --example gateway_ack_ownership -- --retain-reports
cargo run -p sectorsync-bench --release --example gateway_expiry_scan
cargo run -p sectorsync-bench --release --example gateway_expiry_scan -- --collect-remove
cargo run -p sectorsync-bench --example deployment_routing
cargo run -p sectorsync-bench --release --example deployment_stale_scan
cargo run -p sectorsync-bench --release --example deployment_stale_scan -- --collect-mark
cargo run -p sectorsync-bench --example station_event_transport
cargo run -p sectorsync-bench --release --example in_memory_queue_capacity
cargo run -p sectorsync-bench --release --example endpoint_map_lookup
cargo run -p sectorsync-bench --release --example endpoint_map_lookup -- --btree
cargo run -p sectorsync-bench --release --example udp_receive_borrowed
cargo run -p sectorsync-bench --release --example udp_receive_borrowed -- --owned
cargo run -p sectorsync-bench --release --example reliable_frame_encode
cargo run -p sectorsync-bench --release --example reliable_frame_encode -- --owned-frame
cargo run -p sectorsync-bench --release --example reliable_retry_reuse
cargo run -p sectorsync-bench --release --example reliable_retry_reuse -- --fresh-scan
cargo run -p sectorsync-bench --release --example reliable_window_lookup
cargo run -p sectorsync-bench --release --example reliable_window_lookup -- --full-scan
cargo run -p sectorsync-bench --release --example bounded_dedup_index
cargo run -p sectorsync-bench --release --example bounded_dedup_index -- --btree
cargo run -p sectorsync-bench --release --example event_drain_reuse
cargo run -p sectorsync-bench --release --example event_drain_reuse -- --fresh-output
cargo run -p sectorsync-bench --features parallel --example parallel_replication
cargo run -p sectorsync-bench --release --features parallel --example parallel_output_reuse
cargo run -p sectorsync-bench --release --features parallel --example parallel_output_reuse -- --fresh-output
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

For a deliberate measurement on the current development host, use the guarded
release-mode `local` profile. It scales from detected host parallelism, caps the
workload at 24,000 entities and 480 clients, fully encodes selected replication
deltas in a bounded dense-AOI scenario, and enforces a 10-second between-tick
time budget:

```powershell
$env:CARGO_BUILD_JOBS=4
cargo run --release -q -p sectorsync-bench -- --profile=local --allow-heavy
```

This is a repeatable local regression and capacity signal, not a production or
cross-machine network capacity guarantee.

The optimized 128 Hz simulation check keeps replication at an explicit 32 Hz,
spreads viewers across four deterministic phases, and enforces the 7.8125 ms
tick budget:

```powershell
$env:CARGO_BUILD_JOBS=4
cargo run --release -q -p sectorsync-bench --features optimized -- `
  --profile=local --allow-heavy --planner=parallel --threads=8 `
  --replication-hz=32 --tick-ms-p99-budget=7.8125
```

Use `--replication-hz=128` only as the harsher all-clients-every-tick comparison;
it is not stable at 128 Hz on the current development host.

For the guarded many-room shape, each room receives its own `InstanceId`, and
its Station count grows deterministically with player count. The runner advances
all rooms sequentially on one thread, plans with reusable scratch, and directly
encodes dirty component data without an intermediate frame object tree. Viewer
plan slots and their selected-entity buffers are retained across sweeps:

```powershell
$env:CARGO_BUILD_JOBS=4
cargo run --release -q -p sectorsync-bench --example many_rooms
```

The default run covers 500 rooms with 4-24 players each, one Station per 12
players, eight entities per player, and eight measured sweeps. Use
`--entities-per-room` to model entity counts independently from players, plus
`--dirty-percent` and `--component-bytes` to vary actual delta pressure. The
runner reports planning, movement, and encoding phase p99 values separately.
Use `--moving-percent` to exercise indexed entity movement;
`--cross-cell-movement` switches from sub-cell movement to adjacent-cell
movement, while `--force-index-reinsert` exists only for an old-path A/B
comparison. Use `--component-update-percent` to exercise repeated component
writes; `--force-component-replace` is its allocation/replacement comparison.
`--no-frame-capacity-hint` disables dense-frame output capacity hints for an
A/B comparison.
This measures spatial planning and wire encoding, not gameplay, matchmaking,
room lifecycle, persistence, or network capacity.

Repeated sphere/AABB updates that remain within the same covered cells have a
separate guarded release benchmark:

```powershell
cargo run --release -q -p sectorsync-bench --example multi_cell_bounds
cargo run --release -q -p sectorsync-bench --example multi_cell_bounds -- `
  --materialize-cell-list
```

The second command materializes the temporary cell list removed by the
optimized path for an explicit A/B comparison.

Single-viewer plan output reuse has a separate guarded comparison for bridge-
style repeated sends:

```powershell
cargo run --release -q -p sectorsync-bench --example single_viewer_planning -- `
  --entities=32 --calls-per-tick=500 --ticks=30
cargo run --release -q -p sectorsync-bench --example single_viewer_planning -- `
  --entities=32 --calls-per-tick=500 --ticks=30 --fresh-plan-output
```

Budgeted priority selection has an allocation-neutral algorithm comparison:

```powershell
cargo run --release -q -p sectorsync-bench --example priority_top_k
cargo run --release -q -p sectorsync-bench --example priority_top_k -- --full-sort
```

For explicit parallel planning, `ParallelReplicationScratch` retains at most one
planning scratch lane per configured worker, not per Station batch. The
`ReplicationBatchScratch` `*_into` APIs provide the same allocation reuse for
caller-managed serial or custom scheduling loops.

`plan_station_batches_into` and `plan_station_range_batches_into` also retain
one output slot per observed Station batch. Their borrowed
`ParallelReplicationView` exposes only the active rooms, so smaller later calls
neither expose stale results nor discard previously grown entity capacity. The
owned-result methods remain available when the integration needs ownership.

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
When work is pending, it assigns the current Asia/Hong_Kong calendar prefix and
next same-day revision, reruns the quality gate, publishes the four library
crates to crates.io in dependency order, and creates a GitHub Release with
source archives and checksums. The first release of a local day uses revision
zero; later manual or scheduled releases increment it. The workflow resumes
safely when a crate version was already published by an earlier partial run.
Registry authentication uses crates.io Trusted Publishing; the repository does
not store a long-lived registry token.

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting changes. The
project preserves a narrow middleware boundary and requires examples or tests
for public SDK changes.

## Stability

SectorSync uses calendar versions in `YYYY.MMDD.REVISION` form. The `MMDD`
field is encoded as an unpadded integer, so July 10 starts at `2026.710.0` and
January 5 starts at `2027.105.0`. Same-day releases increment the final field.
The three numeric fields remain valid for Cargo's SemVer parser, but the date
identifies a release and does not claim API compatibility. Workspace crates
depend on the exact same calendar version; consumers should review release
notes before upgrading. Authority, boundedness, and explicit-state invariants
remain compatibility commitments.

## Security

Report suspected vulnerabilities using the private process described in
[SECURITY.md](SECURITY.md). Do not open a public issue for an unpatched security
problem.

## License

SectorSync is available under the [MIT License](LICENSE).
