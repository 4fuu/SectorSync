# Breaking API Redesign Contract

This document is the implementation contract for the breaking API redesign.
It records the intended product surface, low-level escape hatches, removal
policy, and the smoke-safe evidence that must remain comparable while the work
is delivered in focused milestones.

The root planning document is intentionally local and is not part of the
repository. This contract is the reviewable source for implementation and
migration decisions.

## Product Entry Point

The normal dependency will be a thin `sectorsync` facade crate. It delegates to
the existing core, wire, transport, and runtime crates and does not duplicate
their algorithms.

The normal product types are:

- `StationRuntime`, which keeps SectorSync-owned Station, spatial-index, and
  component-store state coherent.
- `ReplicationExecutor`, which owns reusable planning/output storage and
  directly emits bounded packets.
- `ReceiveExecutor`, which validates and visits borrowed input by default.
- Explicit gateway, client, transport, and maintenance objects that are driven
  by the embedding application's event loop.

The facade creates no hidden threads, timers, retries, or maintenance loops.
Authentication, gameplay state, persistence, service discovery, production
cryptography, and process placement remain external.

## Intended Quick Start

The final API should support a flow of this shape without exposing scratch
types or requiring manual Station/index synchronization:

```rust,ignore
use sectorsync::prelude::*;

let mut station = StationRuntime::new(StationRuntimeConfig::new(
    StationConfig::new(station_id, node_id, instance_id, 20),
    GridSpec::new(32.0)?,
));

let entity = station.spawn_owned(SpawnEntity::new(
    entity_id,
    Position3::new(64.0, 0.0, 64.0),
    Bounds::Point,
    policy_id,
))?;

station.move_owned(entity, Position3::new(65.0, 0.0, 64.0))?;
```

Names in this document are provisional until their focused implementation
tests exist. The ownership and execution behavior is the stable contract.

## Intended Replication Flow

Normal callers select semantic behavior once at construction. They do not
choose among owned, scratch, and output-reuse variants on every call.

```rust,ignore
let mut replication = ReplicationExecutor::throughput(replication_config);

replication.replicate(
    ReplicationRequest::new(&station, &viewer, &component_selection),
    &mut transport,
)?;

replication.replicate_batch(
    ReplicationBatchRequest::new(&station, &viewers, &component_selection),
    &mut transport,
)?;
```

`Throughput` uses deterministic first-fit, work-bounded selection. A separate
`Prioritized` constructor provides deterministic global top-k semantics and may
inspect the full eligible set. Parallel execution requires an explicitly
supplied bounded pool and exposes the same request methods.

Visibility, eligibility, cadence/tracking, component selection, and budgets are
typed request/configuration inputs. The mode branch occurs outside per-entity
hot loops.

## Intended Receive Flow

Immediate borrowed consumption is the normal path:

```rust,ignore
receive.pump(&mut transport, |frame| {
    for entity in frame.entities() {
        apply_entity(entity)?;
    }
    Ok(())
})?;
```

Owned materialization remains explicit through names such as `pump_owned`,
`decode_owned`, or `to_owned` when data must be queued, replayed, retained, or
moved across the input-buffer lifetime.

## Low-Level Escape Hatch

Applications may continue to depend directly on the low-level crates and own
all scratch, storage, and stage ordering:

```rust,ignore
ReplicationPlanner::plan_for_viewers_into(
    station,
    index,
    policies,
    viewers,
    visibility,
    budget,
    scratch,
    output,
);

builder.encode_binary_bounded_into(
    client_id,
    tick,
    station,
    plan,
    components,
    selection,
    frame_budget,
    packet,
)?;
```

Low-level APIs remain explicit and allocation-light. They are not re-exported
through the product prelude.

## Public Surface Classification

The public workflow surface is classified by behavior rather than by keeping
every current spelling.

| Current family | Classification | Replacement |
| --- | --- | --- |
| `Station`, `CellIndex`, `ComponentStore` primitives | Keep low-level | `StationRuntime` is the coherent product path |
| Allocating `ReplicationPlanner::plan_for_*` methods | Remove | Low-level `*_into` kernels or explicit owned conversion |
| Scratch methods returning owned plans | Remove | Low-level `*_into` kernels |
| Cadence/priority/scratch method combinations | Replace | Executor mode plus typed tracking request |
| `ReplicationTransportBridge::send_viewer*` matrix | Replace | `ReplicationExecutor::replicate` |
| Parallel owned-result planning | Rename/move low-level | Reusable executor output; explicit owned collection |
| `pump` and `pump_visit` pairs | Replace | Visitor `pump`; retaining path `pump_owned` |
| `decode` and `decode_ref` pairs | Replace | Borrowed `decode`; explicit `decode_owned`/`to_owned` |
| Scheduler owned/scratch/state combinations | Replace at product layer | Stateful executor with one normal method |
| Retry methods with/without caller scratch | Replace at product layer | Endpoint-owned scratch; low-level caller-scratch kernel |
| Scratch and capacity diagnostics in core prelude | Move | Owning low-level modules and metrics surface |
| Authority, queue, error, and bounded report types | Keep | Re-export only common product types |

Before destructive removal, every example must be marked as a product example
or a deliberate low-level example. Removed methods require a mechanical entry
in the final migration guide.

## Baseline Evidence

The following results were collected on 2026-07-12 from commit `5e9aa03` with a
clean tracked worktree. Timings are same-host directional evidence only.

All four guarded smoke baselines reported `benchmark_ok=true`:

| Baseline | Updates | Estimated bytes | Tick p99 | Planning p99 | Encoding p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `sectorsync` | 125 | 4,000 | 2.533 ms | 1.996 ms | 0.121 ms |
| `full` | 1,000,000 | 32,000,000 | 1.699 ms | 0.013 ms | 1.264 ms |
| `room` | 250,000 | 8,000,000 | 1.873 ms | 0.013 ms | 1.292 ms |
| `naive-grid` | 400 | 12,800 | 2.629 ms | 1.984 ms | 0.243 ms |

The SectorSync smoke workload performed 500 viewer queries, scanned 252,375
occupied cells, and selected 125 updates.

Three smoke-safe release runs of `dynamic_gameplay` completed 30 ticks with
4,110 viewer plans and identical 41,409 selected/encoded entities:

| Run | Tick p99 | Replication p99 | Command p99 | Simulation p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 6.230 ms | 6.070 ms | 0.118 ms | 0.047 ms |
| 2 | 6.091 ms | 5.969 ms | 0.088 ms | 0.051 ms |
| 3 | 6.154 ms | 5.952 ms | 0.149 ms | 0.051 ms |

These results establish that replication is the first optimization target. They
do not establish that full/room broadcast is faster: the baseline workloads
select different work and smoke payload materialization is intentionally
bounded.

## Acceptance Contract

Every implementation milestone must preserve:

- Authority, ghost, handoff, migration, barrier, and event ordering invariants.
- Queue, payload, tracking, scratch, and retained-capacity bounds.
- Deterministic viewer/selection ordering and workload checksums.
- Explicit route, admission, encoding, transport, visitor, and partial-progress
  failures.
- Exact packet bytes where the wire contract is unchanged.
- No hidden threads or implicit cross-client state.

The normal facade path must match the best existing reusable/borrowed path after
warm-up. The final owned packet allocation remains permitted where the transport
sink takes ownership.

Benchmark field, threshold, or baseline changes require all four guarded smoke
baselines. Station, replication, receive, tracker, transport, or lifecycle
changes also require the smoke-safe `dynamic_gameplay` release example.

## Delivery Order

1. Add the facade and curated prelude without changing algorithms.
2. Add coherent `StationRuntime` operations and explicit low-level accessors.
3. Add replication and receive executors backed by current best kernels.
4. Add measured within-call batch AOI reuse without adding another product API.
5. Add measured incremental multi-cell membership updates.
6. Accept maintenance/transport optimizations only after focused A/B evidence.
7. Remove superseded workflow APIs, update all examples, and publish a
   mechanical migration guide.

Each item is a focused milestone and should be committed independently.
