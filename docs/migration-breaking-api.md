# Breaking API Migration

This release removes high-level method-name matrices. Normal integrations use
the `sectorsync` facade; direct low-level crate users retain scratch, output,
and stage-order control through a small set of `*_into` kernels.

## Dependency And Product Path

Replace multiple normal-path dependencies with the facade:

```toml
[dependencies]
sectorsync = "=2026.711.0"
```

Use `StationRuntime` for coherent Station, spatial-index, component, and
optional command-queue state. `low_level_parts_mut` remains the explicit escape
hatch; the caller must restore all coherence invariants before returning to a
product operation.

| Previous workflow | Product replacement |
| --- | --- |
| Manual `Station` + `CellIndex` spawn/move/despawn | `StationRuntime::{spawn_owned,move_owned,despawn}` |
| `ReplicationTransportBridge::send_viewer*` | `ReplicationExecutor::{replicate,replicate_batch}` |
| Priority method suffixes | `ReplicationExecutorConfig::prioritized` |
| Cadence method suffixes | `ReplicationRequest::with_last_sent` |
| Dirty/custom eligibility suffixes | `ReplicationRequest::with_eligibility` |
| Implicit parallel helpers | Explicit `ParallelReplicationExecutor` with a supplied bounded pool |
| Owned receive default | `ReceiveExecutor::pump` visitor |
| Retained receive frames | `ReceiveExecutor::pump_owned` |
| Endpoint retry with external scratch | Endpoint `retry_due` (scratch retained internally) |
| Owned load sampling | Facade `LoadSampler::sample`, or low-level `sample_all_into` plus explicit copy |
| Split scheduler method matrix | Facade `SplitExecutor::{plan,execute_planned}` |
| Owned Station scheduling and event drain | Facade `StationExecutor`, or low-level `*_into` kernels |

## Low-Level Planner Mapping

The removed methods returned owned output or encoded behavior in their names.
Low-level callers now choose storage once and call one of these kernels:

| Removed family | Replacement |
| --- | --- |
| `plan_for_viewer` / `plan_for_viewer_with_scratch*` | `plan_for_viewer_into` |
| `plan_for_viewers_with_scratch` | `plan_for_viewers_into` |
| `plan_for_viewer_eligible*` | `plan_for_viewer_configured_into` with `eligible` |
| `plan_for_viewer_work_bounded*` | configured `Throughput` mode |
| `plan_for_viewers_work_bounded*` | configured batch `Throughput` mode |
| `plan_for_viewer_prioritized*` | configured `Prioritized` mode |
| `plan_for_viewer_with_cadence*` | configured `last_sent` callback |
| `plan_for_viewer_prioritized_with_cadence*` | configured `Prioritized` plus `last_sent` |
| `plan_for_viewer_range_with_scratch*` | `plan_for_viewer_range_into` |
| `plan_for_viewers_range_with_scratch` | `plan_for_viewers_range_into` |

Configured batch planning also takes an explicit maximum repeated AOI range
cache. Pass zero for no candidate reuse. The facade defaults to a bounded 64
entries and hides this scratch policy behind executor configuration.

```rust,ignore
let mut scratch = ReplicationScratch::default();
let mut plan = ReplicationPlan::default();
ReplicationPlanner::plan_for_viewer_configured_into(
    station,
    index,
    policies,
    viewer,
    visibility,
    budget,
    ReplicationSelectionMode::Prioritized,
    |_, handle, entity| dirty(handle, entity),
    |_, handle| tracker.last_sent(client_id, handle),
    &mut scratch,
    &mut plan,
);
```

Owned output is now an explicit boundary operation: clone `plan`, call
`view.plans.to_vec()`, or build an application-owned transfer object only when
the data must outlive reusable storage.

## Maintenance Mapping

Low-level `SplitScheduler` exposes only `plan_into` and `execute_into`; state,
tick, and scratch are explicit. The facade `SplitExecutor` retains those inputs
and provides the normal caller-driven maintenance flow. `StationLoadSampler`
similarly exposes only `sample_all_into`, while facade `LoadSampler::sample`
owns the reusable storage.

Facade `StationExecutor` owns schedule and event output storage. Low-level
`StationScheduler` retains only `plan_loaded_into`, `advance_loaded_into`, and
`drain_ready_events_into`; explicitly copy a returned view or event slice only
at a real lifetime boundary.

Reliable endpoint `retry_due_with_scratch` is replaced by endpoint `retry_due`.
Direct sender users rename the low-level call to `retry_due_into` and continue
supplying `ReliableClientRetryScratch` or `ReliableStationRetryScratch`.

## Parallel Mapping

`ReplicationThreadPool::plan_station_batches` and
`plan_station_range_batches` returned owned nested results and are removed.
Use `plan_station_configured_batches_into`, `plan_station_batches_into`, or
`plan_station_range_batches_into` with `ParallelReplicationScratch`. The
returned view is valid until that scratch is reused. The product facade exposes
the same configured behavior through `ParallelReplicationExecutor` and never
constructs a thread pool implicitly.

## Transport And Decode Mapping

`ReplicationTransportBridge` is now a low-level caller-plan sender. Its
`send_viewer*` methods are removed; call a planner kernel and then `send_plan`,
or use the facade executor for the normal combined operation.

`ReplicationReceiveBridge::pump` is the borrowed visitor path.
`ReplicationReceiveBridge::pump_owned` is the explicit retaining path. The old
`pump_visit` spelling is removed. `BinaryFrameDecoder::decode_replication`
returns a validated borrowed replication frame; use the generic
`FrameDecoder::decode` only when an owned `RuntimeFrame` is required.

## Examples

| Location | Classification |
| --- | --- |
| `crates/sectorsync/examples/quick_start.rs` | Product bootstrap |
| `crates/sectorsync/examples/replication_flow.rs` | Product replication and borrowed receive |
| `crates/sectorsync-bench/examples/sdk_flow.rs` | Product end-to-end integration |
| `crates/sectorsync-bench/examples/*` (all others) | Deliberate low-level, subsystem, or performance acceptance examples |

Benchmark-only owned materialization helpers live in the unpublished
`sectorsync-bench` crate. They are not SDK compatibility APIs.

## Facade Publication Bootstrap

The automatic release order is core, wire, transport, runtime, then facade.
Before the first public facade release, an owner must reserve `sectorsync` with
a manual bootstrap publish and configure the repository/environment as its
crates.io Trusted Publisher. Do not add a long-lived crates.io token. After
that one-time external step, the OIDC release workflow can publish all five
crates and safely resume partial releases.
