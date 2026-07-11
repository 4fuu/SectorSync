# SectorSync SDK Integration Flow

This guide defines the recommended integration order for an embedding game or
simulation server. SectorSync remains a low-level in-memory middleware library;
the sequence below is an integration contract, not a required framework or
process topology.

Run the cohesive executable path with:

```bash
cargo run -p sectorsync-bench --example sdk_flow
```

The example performs external validation, bounded gateway admission,
barrier-aware station queueing, an external station-local component update,
replication planning/transport, client-side frame validation, and metrics
handoff in one process.

## Ownership Boundary

| Concern | Owner |
| --- | --- |
| Account auth, session credentials, anti-cheat, matchmaking | External gateway/business system |
| Game command schema, permissions, rules, and payload translation | External validator/business system |
| Business ECS, combat, inventory, quests, economy | External game system |
| Durable persistence, crash recovery, backups, failover | External infrastructure |
| Process supervision, service discovery, cloud placement, cluster scheduling | External infrastructure |
| GPU batch execution and accelerator resource management | External compute adapter |
| Bounded command/event queues, station ownership, ghosts, AOI, replication plans | SectorSync |
| In-memory barriers, snapshots, handoff, migration primitives | SectorSync |
| Low-level packet/frame bridges, route metadata, replay windows | SectorSync |

External systems may feed validated state, events, component bytes, route
metadata, subscriber counts, and GPU batch results into SectorSync. SectorSync
does not interpret their business meaning.

## Bootstrap

Create capacities and ownership explicitly before accepting traffic:

1. Create each `Station` with its station, node, instance, and tick metadata.
   Use `Station::with_capacity` when expected local entity count is known;
   reserve free handles separately only for expected despawn churn.
2. Create one station-local `CellIndex`, optionally with explicit entity and
   occupied-cell capacity, and keep it synchronized with entity
   spawn, movement, removal, handoff, and cell migration.
3. Compile `PolicyTable` entries, register custom component descriptors, and
   call `ComponentStore::reserve_component` for known sparse-column sizes.
4. Configure bounded command queues, event queues, transport packet limits,
   replication budgets, trackers, gateway sessions, and deployment routes.
5. Register station/client transport endpoints and expected packet sources.
6. Keep durable state and service discovery outside these runtime objects.

Do not rely on default limits without checking that they fit the embedding
application's resource budget.

## Per-Tick Order

### 1. Validate Before SectorSync

Authenticate the client and validate command schema, permissions, anti-cheat,
game rules, and target intent externally. Translate only accepted input into a
business-agnostic `CommandFrame`; its `kind` and `payload` remain opaque to
SectorSync.

The `sdk_flow` example's `validate_health_request` function represents this
boundary. Rejected input never enters gateway admission or station queues.

### 2. Admit And Route Commands

Use `GatewayClientTransportBridge` when pumping bounded client packets, or call
`GatewayCommandPipeline` after an external transport has delivered frame bytes.
The pipeline validates generic session generation, sequence/replay state,
per-tick admission limits, station route metadata, queue presence, and barrier
ingress policy. It stamps `received_at` and returns an ACK report.

Call `GatewaySessionTable::expire_disconnected` at an application-controlled
maintenance cadence. Expiry performs one allocation-free ordered-map scan,
retains connected sessions and sessions exactly at the grace boundary, and
increments expiration statistics only for removed records. Account/session
credential lifecycle remains external.

For multi-node delivery, resolve `DeploymentRouteTable` metadata and send the
stamped envelope through `CommandDispatchTransportBridge`. External service
discovery decides where the node endpoint is; SectorSync only validates route
and packet metadata.

Call `DeploymentRouteTable::mark_stale_offline` from an externally scheduled
heartbeat maintenance pass. It scans node records once, marks newly stale nodes
Offline, advances their route epochs, and updates detection/offline counters
without allocating an intermediate ID list. Use `stale_nodes` separately only
when the embedding control plane actually needs ordered stale IDs.

### 3. Apply Station-Local Business Work

At the station tick boundary, pop a bounded number of commands from
`CommandQueues`. The external business system decodes the opaque payload,
rechecks authoritative ownership, and applies its rule-specific work. Feed the
result back through controlled APIs such as:

- `Station::move_owned` and `Station::set_tags` for authoritative built-ins.
- `ComponentStore::set_blob` or `set_typed` for occasional external component
  data. For repeated writes, retain a caller-owned `ComponentEncodeScratch` and
  call `set_typed_with_scratch`; use `set_blob_from_slice` when compact bytes
  are already available. Existing blob capacity is reused when sufficient.
- `CellIndex::upsert` after transform/bounds changes. Point updates that remain
  in the same cell avoid index mutation and allocation automatically; point
  updates that cross cells retain their entity-cell list and update the mapping
  in place. Sphere/AABB updates compare existing multi-cell membership before
  allocating a replacement list. Use `upsert_tracked` only when the application
  needs the outcome.
- `EventRouter` or station event transport for ordered cross-station effects.

For steady-state event loops, retain one `Vec<StationEvent>` and pass it to
`EventRouter::drain_ready_into` for one Station or
`StationScheduler::drain_ready_events_into` for an ordered Station set. Output
is cleared without releasing capacity. Delayed Critical, Important, and
BestEffort events remain in their existing priority queues in FIFO order;
SectorSync does not infer retry policy or business timing state.

Ghost records are read-only and must never finalize business state.

### 4. Plan And Send Replication

Build viewer input from caller-owned client state, then use
`ReplicationTransportBridge` with explicit policy, component selection,
visibility, and budget input. The caller-owned bridge lazily retains its planning
scratch and directly encodes selected dirty components into the outbound packet,
avoiding intermediate entity/component delta allocations. Integrations that call
`ReplicationPlanner` directly should retain their own `ReplicationScratch` and
use `ReplicationBatchScratch` with the `*_into` batch APIs when viewer groups are
planned repeatedly. Explicit parallel integrations retain at most one planning
scratch lane per configured worker. Steady-state multi-room loops should use
`plan_station_batches_into` or `plan_station_range_batches_into`; their borrowed
`ParallelReplicationView` exposes active per-Station outputs backed by retained
plan/entity capacity. Consume or encode that view before the next planning call.
Cadence, priority, per-client tracking, and send state must remain explicit.

The bridge retains one `ReplicationPlan` output slot across its normal, cadence,
priority, and priority/cadence send paths. Transport or encoding failures return
the slot to the bridge before the error is surfaced. Direct single-viewer loops
can use the corresponding `*_into` planner methods. Output reservation is capped
by both the budget hard limit and actual candidate count.

Priority planning uses deterministic top-k partitioning when the budget selects
less than half of eligible candidates, then sorts only that prefix by score,
distance, and handle. At or above half, it conservatively uses the existing full
sort. Output ordering and skipped-by-budget statistics are unchanged.

`ReplicationTransportBridge` also requests a bounded initial packet capacity
when up to four uniformly sampled entities all contain encodable dirty data.
Sparse or empty samples return a zero hint and retain normal `Vec` growth. Direct
wire integrations can use `ReplicationFrameBuilder::sampled_binary_capacity_hint`
before `encode_binary_into`; the planner-only `binary_capacity_hint` is available
when component storage is not accessible.

On the receive side, `ReplicationReceiveBridge` validates expected packet
source and frame target before returning decoded frames. Applying those frames
to a client world remains an external client responsibility.

Only clear dirty state after the integration's chosen delivery/ACK contract
confirms it. `ReplicationTracker` provides bounded send/ACK bookkeeping but does
not invent that protocol.

### 5. Handle Barriers Explicitly

Preserve this control sequence:

1. `BarrierController::request` with an explicit command mode.
2. Advance/poll until all scoped stations align at the target tick.
3. Enter `Frozen` before exporting snapshots or invoking upgrade/migration hooks.
4. Broadcast barrier state through `BarrierTransportBridge` when clients need
   pause/resume notifications.
5. Resume only after external work succeeds and restored stations validate.

`CommandQueueMode::Buffer` uses a bounded barrier buffer whose capacity is the
saturating sum of the ready priority limits. If releasing buffered commands hits
a full ready queue, the blocked command remains buffered for retry.

SectorSync does not load scripts or decide whether the game should use seamless
or visible paused updates.

### 6. Sample, Split, And Migrate

Use `StationLoadSampler` to derive station/entity/cell/event pressure from
runtime structures plus caller subscriber counts. Feed samples into
`SplitScheduler` or `StationScheduler`. Keep target capacity, cooldown, action,
and moved-cell limits explicit.

For repeated load-aware scheduling, retain `StationScheduleScratch` and call
`plan_loaded_into` or `advance_loaded_into`. The scratch keeps only derived
Station scores and stateless candidates; the borrowed `StationScheduleView`
must be consumed before the next call. Duplicate samples preserve existing
last-value-wins behavior. When the advancement budget is less than half the
Station count, the scheduler partitions and sorts only the deterministic top-k;
larger budgets use full sorting.

For repeated hotspot and split passes, retain `HotspotSplitScratch` and use
`HotspotPlanner::propose_cell_split_into`, `SplitScheduler::plan_with_scratch`,
or `plan_with_state_and_scratch`. Candidate and proposal buffers grow on demand;
the planner partitions deterministic top-k cells only when the move budget is
less than half the sample. Scratch does not retain cooldown or ownership state,
and it does not bypass target-capacity, improvement, action, or migration guards.

When the complete split pass repeats, prefer `SplitSchedulerScratch` with
`plan_into` or `plan_with_state_into`. The returned `SplitScheduleView` exposes
only active decision/action slots while retaining decision reason vectors,
action proposal coordinates, and hotspot candidates across passes. Use
`execute_view` and `SplitSchedulerState::record_schedule_view` directly before
the next planning call; owned schedule APIs remain available when results must
outlive the scratch borrow.

Execute ownership changes through `CellMigrationExecutor` or
`EntityMigrationExecutor` so target ghosts are prewarmed, owner commit is
single-authority, source ghosts survive the handoff window, and both spatial
indexes are refreshed. External cluster placement and failover remain outside
this flow.

### 7. Export Observability

Read reports and `stats()` values at bounded intervals and hand them to the
external metrics stack. Useful fields include queue depth/rejections, command
latency, router pressure, selected replication candidates, encoded bytes,
transport rejection counts, tracker capacity, load samples, split decisions,
barrier duration, and migrated entity/cell counts.

SectorSync reports counters and decisions; it does not start telemetry threads,
collect OS metrics, or choose production alerting policy.

## Error Expectations

| Failure | Required integration response |
| --- | --- |
| External auth/schema/rule rejection | Reject before creating a `CommandFrame`; use the external reason model. |
| Gateway replay, stale generation, disconnect, or rate limit | Return the negative ACK, audit as needed, and do not enqueue. |
| `CommandQueueError::QueueFull` | Apply caller backpressure/drop policy; never add an unbounded side queue. |
| `CommandQueueError::RejectedByBarrier` | Honor the configured pause policy and return/retry externally as appropriate. |
| Missing station queue | Treat as an integration topology/configuration error; do not silently reroute. |
| Missing/stale deployment route | Refresh external routing/control-plane state; SectorSync does not discover nodes. |
| Transport packet/byte budget rejection | Reduce/batch within the explicit limit or apply external backpressure. |
| Packet source/target mismatch | Drop and count it; do not apply or forward the frame. |
| Replication budget exhaustion | Send the bounded selected set and defer remaining candidates by caller policy. |
| Barrier not frozen for snapshot/upgrade | Stop the operation and continue the request/align/freeze sequence. |
| Migration owner/index mismatch | Abort the migration action and repair authoritative metadata before retrying. |
| Tracker/capacity exhaustion | Prune or resize explicitly; never spill into hidden unbounded history. |

## Executable References

| Use case | Example |
| --- | --- |
| Cohesive validated command-to-replication path | `sdk_flow` |
| Client/gateway command, ACK, replication, barrier receive | `client_bridge` |
| Gateway admission and negative ACK behavior | `gateway_command_pipeline` |
| Gateway-to-node route resolution and dispatch | `gateway_deployment_dispatch` |
| Direct UDP command ingress and ACK | `command_ingress` |
| Internal station dispatch and target validation | `gateway_deployment_dispatch` |
| AOI-to-frame replication transport | `replication_bridge`, `replication_bridge_priority` |
| Pause/freeze/resume client notification | `barrier_transport` |
| Frozen snapshot upgrade hook and restore | `barrier_upgrade` |
| Load sampling into bounded station scheduling | `load_sampling` |
| Split planning and cell migration | `split_migration`, `split_tuning` |
| Cross-station event transport | `station_event_transport`, `reliable_station_event` |

All routine verification should stay on the smoke profile. Medium, large, and
manual heavy scale require explicit `--allow-heavy` opt-in.
