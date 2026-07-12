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
   Component IDs may be sparse; column storage scales with registered IDs, not
   the highest numeric ID.
4. Configure bounded command queues, event queues, transport packet limits,
   replication budgets, trackers, gateway sessions, and deployment routes.
5. Register station/client transport endpoints and expected packet sources.
6. Keep durable state and service discovery outside these runtime objects.

Do not rely on defaults without checking the application's resource budget.
The steady-state allocation and lookup rules are:

| Surface | Behavior | Integration rule |
| --- | --- | --- |
| Command/Event queues | Priority storage starts empty, grows on accepted traffic, and retains peak capacity | Treat configured limits as hard backpressure/drop bounds |
| In-memory packet queues | Endpoint registration reserves no packet slots; drained queues retain reached capacity | Size limits from the worst acceptable backlog and payload budget |
| Endpoint registries | Ordered below 2,048 entries, then promoted once to hash lookup | No tuning is required; promotion preserves queues and does not reverse |
| Client send | Validation and enqueue share one mutable target lookup | Error ordering and post-enqueue statistics remain stable |
| In-memory batch send | Locks bounded 64-packet segments and stops at the first failure | The successful prefix remains queued and ordered |
| Budgeted batch send | Checks aggregate and per-packet byte limits in one scan | Rejected batches are never forwarded |

Capacity inspection APIs remain available for host metrics: Command queue
`ready_retained_capacity`, `total_ready_retained_capacity`, and
`barrier_buffer_retained_capacity`; Event queue `retained_capacity` and
`total_retained_capacity`; transport `queued_capacity` and
`retained_queue_capacity`.

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

For the common local-queue path, use
`GatewayClientTransportBridge::pump_ingress_compact`. It returns fixed-size
accepted/rejected/ACK counts and moves each encoded ACK Vec directly into the
transport packet. Use the compatible `pump_ingress` when the caller must retain
per-command pipeline errors, routing metadata, reason codes, or encoded ACK
bytes after sending. Both variants share the same packet decode, source check,
gateway admission, queueing, and accumulated bridge statistics.

Call `GatewaySessionTable::expire_disconnected` at an application-controlled
maintenance cadence. Expiry performs one allocation-free ordered-map scan,
retains connected sessions and sessions exactly at the grace boundary, and
increments expiration statistics only for removed records. Account/session
credential lifecycle remains external.

Gateway session lookup adapts without configuration. Tables start with ordered
storage for small room/local Gateway deployments and promote once to hash
storage when adding the 1,024th distinct session. Routes, reconnect generations,
admission counters, disconnected state, expiry, and capacity accounting survive
the migration. Promoted tables stay hashed after expiry/removal to avoid churn
around the threshold; no iteration order is exposed as Gateway behavior.
Refreshing an existing session through `connect` performs one mutable session
lookup. `AlreadyConnected`, `Reconnected`, `ReplacedExpired`, route changes,
generation resets, and their cumulative counters retain the same behavior.

For multi-node delivery, resolve `DeploymentRouteTable` metadata and send the
stamped envelope through `CommandDispatchTransportBridge`. External service
discovery decides where the node endpoint is; SectorSync only validates route
and packet metadata.

Call `DeploymentRouteTable::mark_stale_offline` from an externally scheduled
heartbeat maintenance pass. It scans node records once, marks newly stale nodes
Offline, advances their route epochs, and updates detection/offline counters
without allocating an intermediate ID list. Use `stale_nodes` separately only
when the embedding control plane actually needs ordered stale IDs.

For reliable Client or Station links, call `retry_due` on the endpoint from the
application's maintenance loop. The endpoint owns and reuses its retry scratch.
Custom low-level integrations use the sender's `retry_due_into` kernel with
caller-owned scratch. The sender indexes retry deadlines; non-due polls inspect
only the earliest deadline. Reliable senders encode stored payload slices
directly into the owned transport packet and do not clone the in-flight payload.
Transport failure leaves attempts unchanged; timeout and retry order remain
bounded by the configured window, interval, and attempt limits.
`in_flight_for` and send-window admission use an active-peer count index instead
of scanning all packets. ACK and timeout removal update that index immediately;
the final packet for a peer also removes its count entry.

`CommandDispatchTransportBridge::send_envelope` encodes the borrowed stamped
envelope directly. Use an owned `CommandDispatchFrame` only when dispatch data
must be retained independently; immediate transmission does not clone its
opaque payload.

Reliable Client and Station endpoints borrow-decode inbound frames and reuse
the received wire Vec as the unique delivered payload after removing the fixed
reliable header in place. This is automatic in `handle_inbound`; use
`ReliableClientFrame::decode_ref` or `ReliableStationFrame::decode_ref` when
inspecting frame bytes directly, and compatible owned `decode` only when the
payload must be materialized independently. ACK ordering, duplicate suppression,
source metadata, and bounded delivery history are unchanged.
Duplicate histories and security replay windows select their lookup index from
the configured bound automatically: capacities below 256 retain the compact
ordered-set path, while capacities of 256 or more use hash lookup. No full-bound
allocation occurs at construction, so applications should configure history for
their actual retry/replay horizon rather than inflating it for performance.

`ReplicationTracker::record_plan_sent` skips the per-entity capacity pre-scan
when current entries plus the entire plan length already fit under
`max_entries`; this is conservative even if every entity is new. Near the
limit it retains the exact existing/new scan and fails before mutating any
record. Applications should keep the tracker bound sized for their real
client/entity horizon rather than relying on the fast path to weaken limits.
Tracker record lookup also adapts automatically. It starts ordered for small
client/entity sets and promotes once to hash storage when adding the 2,048th
distinct key. Last-sent/ACK state, explicit client cleanup, tick pruning,
capacity errors, and statistics survive migration. Promoted trackers remain
hashed after pruning to avoid allocation churn around the threshold; tracker
APIs expose keyed records rather than storage iteration order.

For repeated security sealing, retain `PacketSecurityScratch` and call
`seal_into`, `seal_with_nonce_into`, or `seal_with_key_ring_into`. The scratch
retains encrypted-payload and authentication-tag storage; the final wire Vec
remains caller-owned and can be moved into a transport packet. Failed payload,
cipher, authenticator, or tag validation does not append partial wire bytes.
Authenticators, ciphers, keys, and certificates remain external integration
responsibilities.

For repeated security opening, retain `PacketSecurityOpenScratch` and call
`open_with_scratch` or `open_with_key_ring_and_scratch`. The returned
`PacketSecurityOpenView` borrows plaintext from that scratch and must be
consumed before the next call that mutates it. Use the compatible owned `open`
methods when plaintext must outlive the scratch or move into caller-owned
storage. Authentication and replay checks still run before decryption, and a
failed authentication leaves the previous scratch plaintext unchanged.

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
- On entity despawn, call `ComponentStore::clear_entity` when removed component
  values are not needed. If the application must inspect or transfer those
  values, retain one `Vec<(ComponentId, ComponentBlob)>` and pass it to
  `remove_entity_into`; use the compatible `remove_entity` owned result for
  occasional cleanup. Entity lifetime and gameplay teardown ordering remain
  application-owned.
- `CellIndex::upsert` after transform/bounds changes. Point updates that remain
  in the same cell avoid index mutation and allocation automatically; point
  entities store their single cell inline without a per-entity heap allocation.
  Sphere/AABB updates compare existing multi-cell membership before
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
`ReplicationExecutor`. Select Throughput or Prioritized once in
`ReplicationExecutorConfig`; provide visibility, optional eligibility, and
optional last-sent state through `ReplicationRequest` or
`ReplicationBatchRequest`. The executor owns reusable scratch and output,
uses bounded direct encoding, and surfaces partial transport progress.

The sequential batch path reuses candidates only for repeated quantized AOI
cell ranges within the current call. The cache is bounded by
`max_cached_query_ranges` and stores no cross-client state. Explicit parallel
integrations construct `ParallelReplicationExecutor` with a supplied bounded
pool; no default constructor creates threads.

Applications that need custom stage ordering can call
`plan_for_viewer_into`, `plan_for_viewers_into`, or the configured variants
and retain their own `ReplicationScratch` and output. Range-only SIMD kernels
remain available as `plan_for_viewer_range_into` and
`plan_for_viewers_range_into`. Parallel low-level callers use the corresponding
`plan_station_*_into` methods and consume the borrowed
`ParallelReplicationView` before reusing its scratch.

Sparse-update integrations pass an eligibility callback to the configured
kernel or facade request. `ComponentStore::has_dirty_selected` is suitable for
global dirty state; per-client delivery state remains caller-owned. Throughput
uses deterministic work-bounded first-fit selection and reports
`unexamined_after_budget`. Prioritized mode inspects the eligible set to choose
a deterministic global top-k.

`ReplicationBudget::max_bytes` is the concrete frame limit used by the facade
executor. The first entity that would exceed the frame budget is rolled back and
reported through `skipped_by_frame_bytes`. Direct wire integrations can use
`ReplicationFrameBuilder::sampled_binary_capacity_hint` followed by
`encode_binary_bounded_into`, then submit the owned packet through their sink.
The low-level `ReplicationTransportBridge::send_plan` remains available for
this caller-planned boundary; it no longer plans viewers.

On the receive side, `ReplicationReceiveBridge` validates expected packet
source and frame target before returning decoded frames. Applying those frames
to a client world remains an external client responsibility.

High-throughput adapters that apply a frame immediately can instead call
`BinaryFrameDecoder::decode_replication`. The validated
`ReplicationFrameRef`, `EntityDeltaRef`, and `ComponentDeltaRef` iterators
borrow all nested storage and component bytes from the input packet, avoiding
decoder-owned collections and payload copies. The views cannot outlive or be
mutated independently of that packet. Use `FrameDecoder::decode` or
`ReplicationFrameRef::to_owned` when frames must be queued, retained, or moved
across that lifetime boundary. Source/target validation and application of
component semantics remain caller responsibilities on this low-level path.

`ReplicationReceiveBridge::pump` combines that borrowed decoding with the
bridge's expected-source, frame-target, and accumulated-statistics checks. The
fallible visitor can apply components directly to caller-owned client state;
`ReplicationReceiveVisitError` keeps visitor failures distinct from transport
and validation failures. Accepted-frame statistics are recorded before visitor
invocation and are not rolled back on an application error. Use `pump_owned`
when frames must be queued, replayed, or transferred.

When one client receive loop must also handle command ACKs and barrier state,
use `ClientTransportBridge::pump`. Its `ClientInboundFrameRef` carries ACK
and barrier values plus borrowed replication frames through one expected-source,
target-validation, statistics, and visitor-error path. Consume replication
payload slices before returning from the visitor. Use the mixed
`pump_owned` when any decoded frame must be retained or transferred.

For synchronous UDP receive loops, `UdpTransport::try_recv_ref` and
`UdpStationTransport::try_recv_station_ref` expose bytes from each adapter's
configured reusable datagram buffer. Consume and decode the view before the
next mutable adapter operation. Use the compatible `TransportReceiver` or
`StationTransportReceiver` methods when a packet must be queued, transferred,
or retained independently of the adapter.

Only clear dirty state after the integration's chosen delivery/ACK contract
confirms it. `ReplicationTracker` provides bounded send/ACK bookkeeping but does
not invent that protocol.

When a room or Station is destroyed, explicitly remove it from `StationSet` and
`StationIndexSet`, call `EventRouter::unregister_station`, and unregister its
in-memory transport endpoints. Ordered runtime registries preserve the relative
order of survivors. Router and transport teardown return discarded queue counts
so integrations can require zero backlog or account for intentional loss before
reusing an identifier.

Use a separate `InMemoryTransportHub` per independent room or test shard, as in
`dynamic_gameplay`. A hub intentionally serializes its endpoint registry,
queues, batch partial-commit rules, and statistics under one lock; it is a
bounded in-process adapter, not the production cross-room network transport.

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

For repeated frozen-state inspection or external checkpoint collection, retain
`BarrierSnapshotScratch`, reserve the expected Station/entity shape, and call
`export_snapshots_into`. Each `StationSnapshot` slot reuses its entity Vec;
consume the returned slice before the next export mutates the scratch. Use
`Station::snapshot_into` for an individual Station and the compatible owned
snapshot APIs when data must be moved into an upgrade hook, persistence adapter,
or longer-lived queue. SectorSync still owns only the frozen in-memory snapshot,
not durable storage or recovery policy.

`Station::restore` automatically sizes record, generation, and entity-id index
storage from the migrated snapshot before insertion. Use `restore_tracked` only
when the integration needs `StationRestoreStats` for capacity telemetry or
acceptance checks; both APIs preserve the same validation and restored state.
The upgrade executor uses the preallocated path automatically.

SectorSync does not load scripts or decide whether the game should use seamless
or visible paused updates.

### 6. Sample, Split, And Migrate

Use `StationLoadSampler` to derive station/entity/cell/event pressure from
runtime structures plus caller subscriber counts. Feed samples into
`SplitScheduler` or `StationScheduler`. Keep target capacity, cooldown, action,
and moved-cell limits explicit.

For periodic product-path sampling, use facade `LoadSampler::sample`; it owns
and reuses subscriber aggregation, deterministic occupancy, Station output
slots, and each Station's cell output. Low-level callers retain
`StationLoadSamplerScratch` and call `sample_all_into`. Consume the borrowed
sample slice before the next call. Explicitly copy it only when it must outlive
the sampler storage. Subscriber counts remain caller input and duplicate
Station ids are combined with saturating arithmetic.

When one process hosts many Stations, construct `StationSet` and
`StationIndexSet` with `with_capacity`, or call `reserve` before registration.
Both registries retain insertion-order iteration. Sets below 64 slots use the
small linear path without a lookup-table allocation; at 64 slots they build an
ID-to-slot index and use it for `get`, `get_mut`, and `get_pair_mut`. Capacity
and active-path accessors make the selected storage visible to integrations.

For repeated load-aware scheduling, retain `StationScheduleScratch` and call
`plan_loaded_into` or `advance_loaded_into`. The scratch keeps only derived
Station scores and stateless candidates; the borrowed `StationScheduleView`
must be consumed before the next call. Duplicate samples preserve existing
last-value-wins behavior. When the advancement budget is less than half the
Station count, the scheduler partitions and sorts only the deterministic top-k;
larger budgets use full sorting.

For normal split maintenance, facade `SplitExecutor` owns planning, cooldown,
and execution storage. Call `plan`, inspect the borrowed schedule, then call
`execute_planned`; the application still decides when either action runs.
Low-level callers use `SplitScheduler::plan_into` with explicit state, tick, and
`SplitSchedulerScratch`, then `execute_into` with
`SplitScheduleExecutionScratch`. Consume borrowed views before reusing their
storage and explicitly materialize a `SplitSchedule` only when its lifetime
requires ownership. All paths preserve target-capacity, improvement, cooldown,
action, and migration guards.

Execute ownership changes through `CellMigrationExecutor` or
`EntityMigrationExecutor` so target ghosts are prewarmed, owner commit is
single-authority, source ghosts survive the handoff window, and both spatial
indexes are refreshed. `CellMigrationExecutor::migrate_cells` automatically
scans borrowed index membership without copying a handle Vec per cell. For
repeated split execution, retain `CellMigrationScratch` and a
`CellMigrationReport`, reserve for the expected entity count, and call
`migrate_cells_into`; both working and result capacity survive subsequent
passes. As with the owned API, an error can follow already committed earlier
entity migrations; reusable reports retain successfully completed entries, but
the application must reconcile authoritative state instead of retrying blindly.
External cluster placement and failover remain outside this flow.

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
| Reliable Client/Station retry and duplicate suppression | `reliable_command_ingress`, `reliable_station_event` |
| Internal station dispatch and target validation | `gateway_deployment_dispatch` |
| AOI-to-frame replication transport | `replication_bridge`, `replication_bridge_priority` |
| Pause/freeze/resume client notification | `barrier_transport` |
| Frozen snapshot upgrade hook and restore | `barrier_upgrade` |
| Load sampling into bounded station scheduling | `load_sampling`, `load_sampling_reuse` |
| Split planning and cell migration | `split_migration`, `split_tuning` |
| Cross-station event transport | `station_event_transport`, `reliable_station_event` |

All routine verification should stay on the smoke profile. Medium, large, and
manual heavy scale require explicit `--allow-heavy` opt-in.
