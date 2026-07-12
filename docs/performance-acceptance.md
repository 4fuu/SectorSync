# SectorSync Performance Acceptance Matrix

This document maps SectorSync performance claims to reproducible commands,
machine-readable output fields, and default pass/fail gates. It is a development
acceptance matrix, not a production capacity promise for arbitrary hardware.

## How to Use This Document

| Need | Section |
| --- | --- |
| Routine regression gate | [Default acceptance run](#default-acceptance-run) |
| Verdict and output contracts | [Pass/fail gates](#passfail-gates) and [recorded evidence](#recorded-evidence) |
| Planner comparison | [Baseline comparison](#baseline-comparison) |
| Host-sized or many-room evidence | [Scenario measurements](#scenario-measurements) |
| Focused A/B evidence | [Hot-path measurements](#hot-path-measurements) |
| Deliberate large runs | [Heavy calibration](#heavy-calibration) |

All benchmark examples are deterministic and guarded. Unless a section says
otherwise, local timings compare alternating release-mode runs on the same
development host. Portable acceptance comes from identical result checksums,
bounded work/capacity counters, path verdicts, and `benchmark_ok=true`; timing
percentages are directional regression evidence, not cross-machine guarantees.

## Default Acceptance Run

Use the guarded smoke profile for routine development:

```bash
cargo run -q -p sectorsync-bench -- --profile=smoke
```

The run is accepted only when it exits successfully and prints
`benchmark_ok=true`. The smoke workload stays within these default guards:

| Input | Smoke value | Guard without `--allow-heavy` |
| --- | ---: | ---: |
| Entities | 2,000 | 4,000 |
| Clients | 100 | 150 |
| Stations | 4 | 8 |
| Ticks | 5 | 5 |

The runner prints the detected host parallelism for context, but it does not
collect OS metrics or change the middleware execution model.

## Pass/Fail Gates

| Claim | Measured field | Default gate | Verdict field |
| --- | --- | ---: | --- |
| Stable tick latency | `tick_ms_p99` | <= `threshold_tick_ms_p99` (35 ms) | `threshold_tick_ok` |
| Low command apply latency | `command_latency_ticks_max` | <= `threshold_command_latency_ticks_max` (2 ticks) | `threshold_command_latency_ok` |
| Bounded command pressure | `command_queue_max` | <= `threshold_command_queue_max` (1,024) | `threshold_command_queue_ok` |
| No command queue loss | `command_queue_drops` | <= `threshold_command_queue_drops_max` (0) | `threshold_command_queue_drops_ok` |
| No router loss | `router_event_drops` | <= `threshold_router_event_drops_max` (0) | `threshold_router_event_drops_ok` |
| Bounded estimated downlink | `estimated_payload_bytes` | <= `threshold_estimated_payload_bytes` (64 MiB) | `threshold_payload_ok` |
| Command delivery conservation | enqueue/dispatch/apply counters | all accepted workload commands reach apply | `threshold_command_delivery_ok` |
| Router delivery conservation | routed/drained/drop counters | routed equals drained plus dropped | `threshold_router_delivery_ok` |
| Client/gateway bridge roundtrip | command/ACK/replication bridge counters | all sampled commands ACK and all sampled frames arrive | `threshold_client_bridge_ok` |
| Complete payload materialization | selected/materialized delta counters | full profiles materialize every selected delta | `threshold_payload_materialization_ok` |
| Complete non-empty workload | completed ticks, packets, time budget | all requested ticks and encoded packets complete within budget | `threshold_workload_completed_ok` |
| Exact replication workload | selected update count, viewer count, deterministic work checksum | measured values equal the independently computed reference workload | `threshold_replication_workload_ok` |
| Requested planner available | requested/applied planner mode | requested mode is compiled and applied | `threshold_planner_mode_ok` |
| Requested profile admitted | requested/applied profile and heavy guard | guarded profiles require `--allow-heavy` | `threshold_profile_admitted_ok` |

`benchmark_ok` is the conjunction of every verdict field above. Override
thresholds only when a benchmark scenario has a documented reason:

```bash
cargo run -q -p sectorsync-bench -- --profile=smoke \
  --tick-ms-p99-budget=40 \
  --command-latency-ticks-budget=3 \
  --command-queue-budget=2048 \
  --command-queue-drops-budget=0 \
  --router-event-drops-budget=0 \
  --payload-bytes-budget=67108864
```

## Recorded Evidence

Every acceptance run records these field groups:

| Area | Fields |
| --- | --- |
| Workload/guard | profile/guard fields, planner request/applied mode, SIMD/parallel availability, thread count, tick/replication rates and phases, entity/client/station/tick counts, completed ticks, time budget |
| Tick latency | `tick_ms_p50`, `tick_ms_p95`, `tick_ms_p99`, `tick_ms_max`, planning/encoding phase percentiles, 128 Hz budget/headroom/verdict, `elapsed_ms` |
| Replication selection | expected/measured viewer and update counts, expected/measured deterministic work checksum, `replication_scratch_queries`, grid/occupied strategy query counts, probed/scanned/matched cell counts, `replication_scratch_candidates`, `replication_candidates_selected` |
| Replication scratch capacity | `replication_scratch_candidate_capacity_max`, `replication_scratch_dedup_capacity_max`, `replication_scratch_matching_cell_capacity_max`, `replication_scratch_priority_capacity_max` |
| Encoded payload | per-frame materialization limit and requirement, `encoded_packets`, `encoded_bytes`, `payload_entity_deltas`, `payload_component_deltas`, `estimated_payload_bytes`, materialization verdict |
| Direct commands | `commands_enqueued`, `commands_applied`, `command_latency_ticks_avg`, `command_latency_ticks_max`, `command_queue_max`, `command_queue_drops` |
| Gateway/deployment dispatch | `gateway_commands_dispatched`, dispatch packet/byte/enqueue/apply/latency fields |
| Client/gateway bridge | command bytes, gateway accepted/ACK/applied counts, received packet/byte/ACK/replication/entity/component counts |
| Event router pressure | `router_events_routed`, `router_events_drained`, `router_event_drops`, `router_queue_max` |
| Hotspot/split decisions | `max_cell_entities`, `warm_stations`, `hotspot_stations`, `split_candidate_cells` |
| Station scheduler decisions | `scheduler_candidates_considered`, `scheduler_stations_selected`, `scheduler_total_advances` |

Percentiles are computed from per-tick wall-clock samples. The five-tick smoke
profile is a regression signal, not a statistically strong production latency
study; p95 and p99 may resolve to the same sample. Use guarded larger profiles
only for deliberate calibration.

## Baseline Comparison

Run all comparisons with the same profile and host conditions:

```bash
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=sectorsync
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=full
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=room
cargo run -q -p sectorsync-bench -- --profile=smoke --baseline=naive-grid
```

Interpretation:

- `full` selects every entity for every viewer.
- `room` selects the station-sized entity share for every viewer.
- `naive-grid` uses a direct spatial sphere query without compiled policy,
  visibility, cadence, priority, or replication budget logic.
- `sectorsync` uses the policy-driven planner with reusable scratch storage.

All four modes now use the same phased viewer schedule and identical command,
gateway, client-bridge, and event workloads. Only replication selection changes.
Each mode computes its expected viewer count, selected update count, and
deterministic work checksum outside the timed loop; a faster run that silently
drops selection work fails `threshold_replication_workload_ok`.

The scratch-backed spatial query chooses deterministically between probing the
full query cell volume and scanning the index's non-empty cells. Large sparse
queries sort matched occupied cells before collecting handles, preserving grid
query order while avoiding empty-cell probes. Strategy work counters and
retained capacity fields make this choice visible without collecting OS metrics.

Compare at least selected candidates, estimated payload bytes, encoded bytes,
and tick percentiles. Smoke, medium, and large profiles intentionally
materialize at most 16 sample entity deltas per frame to keep routine runs
bounded, so `estimated_payload_bytes` is their comparable logical bandwidth
estimate while `encoded_bytes` measures the bounded wire-codec workload actually
executed. The guarded local profile below raises the limit to the planner's 300
entity viewer budget and fails unless every selected update is materialized.

## Scenario Measurements

These runs exercise multiple subsystems together. Use them for local capacity
signals after the routine smoke gate passes.

### Guarded Local Host Measurement

The `local` profile is a deliberate, release-mode measurement sized from the
host's available parallelism. It remains behind `--allow-heavy` and is never run
by CI. On the current 6-core/12-thread, 31.67 GiB development host it resolves
to 24,000 entities, 480 clients, eight stations, and 30 measured ticks:

```powershell
$env:CARGO_BUILD_JOBS=4
cargo run --release -q -p sectorsync-bench -- --profile=local --allow-heavy
```

The default profile uses one benchmark thread. The optional `parallel` planner
creates an explicit bounded pool, partitions viewers deterministically by
station, and fuses each station's planning, payload construction, and encoding
inside one worker task. Both paths materialize every selected entity delta up to
the planner's hard 300-entity viewer limit and stop before starting another tick
once the 10-second run budget is exhausted.
It prints `ticks_completed`, `time_budget_exhausted`,
`threshold_payload_materialization_ok`, and
`threshold_workload_completed_ok`; `benchmark_ok=true` requires all 30 ticks and
full selected-payload materialization.

The scale formula is conservative and capped: 2,000 entities and 40 clients per
logical processor, clamped to 8,000-24,000 entities and 160-480 clients, with
four to eight stations. Unlike the wide smoke workload, the local profile places
entities and viewers in a deterministic dense 2,000 x 200 x 2,000 world with a
256-unit AOI. This drives materially more selected updates without increasing
the entity/client cap. The cap is intentionally far below the host's physical
memory capacity, while the four-job Cargo setting bounds release-build CPU
pressure. The 10-second budget is a cooperative guard between ticks, not a hard
process watchdog.

This measures real SectorSync spatial planning, queues, bridges, binary encoding,
and in-process transports with a deterministic synthetic workload. It does not
measure WAN behavior, kernel UDP throughput, cross-machine deployment, durable
storage, or production gameplay code. Record the full output and run it at least
three times before treating changes as a local regression.

### Optional SIMD, Parallel, and 128 Hz Modes

Feature behavior is explicit:

- `sectorsync-core/simd` uses `wide` 0.7.33 for a safe eight-lane range-only
  distance filter and keeps the generic visibility path scalar.
- `sectorsync-runtime/parallel` exposes `ReplicationThreadPool`; constructing the
  pool is the only operation that creates threads. Its default uses half of host
  logical parallelism and caps at eight workers.
- `sectorsync-bench/optimized` enables both features for comparison. SIMD remains
  optional because gather overhead on the current AoS station layout can erase
  its arithmetic benefit.

The all-clients-every-tick stress comparison is:

```powershell
cargo run --release -q -p sectorsync-bench --features optimized -- `
  --profile=local --allow-heavy --planner=parallel --threads=8 `
  --replication-hz=128
```

On the current development host this mode can fall below 7.8125 ms, but repeated
runs have also exceeded it; it is not accepted as stable 128 Hz evidence.

For a realistic 128 Hz simulation with 32 Hz client replication, use four
deterministic viewer phases and make the 128 Hz budget an actual pass/fail gate:

```powershell
cargo run --release -q -p sectorsync-bench --features optimized -- `
  --profile=local --allow-heavy --planner=parallel --threads=8 `
  --replication-hz=32 --tick-ms-p99-budget=7.8125
```

Three consecutive measurements on the current host reported 2.00-2.54 ms tick
p99 and 5.28-5.81 ms of 128 Hz headroom while fully encoding 443,402 selected
deltas per 30-tick run. This remains local deterministic evidence, not a hard
real-time or production-network guarantee.

### Many-Room Single-Thread Measurement

The guarded room benchmark models one `InstanceId` per room and assigns
`ceil(players / players_per_station)` Stations, capped by
`max_stations_per_room`. All room/Station work runs sequentially on the calling
thread; every viewer performs a real Cell query and replication plan, and every
dirty component selected for a viewer is encoded directly into a concrete binary
delta. Viewer plan/entity output capacity is retained across sweeps. The runner
reports planning and encoding p99 plus `batch_plan_slots_max` and
`batch_entity_capacity_max` separately. Station, id-index, free-handle,
spatial-index, occupied-cell, and component-column retained capacities are also
reported, and `threshold_retained_capacity_ok` requires active storage coverage:

```powershell
$env:CARGO_BUILD_JOBS=4
cargo run --release -q -p sectorsync-bench --example many_rooms
```

The default cap is 500 rooms, 4-32 players per room, eight Stations per room,
16 entities per player, 256 explicit entities per room, 256 bytes per component,
and ten sweeps. The default workload uses the smaller
4-24 player, one Station per 12 players, eight entities per player, and eight
sweep shape. Oversized manual values are clamped unless `--allow-heavy` is
present, and the runner stops before another sweep after its 10-second budget.

On the current development host, the default 500-room workload resolves to 784
Stations, 6,966 players, and 55,728 entities. Repeated release runs reported
26.14-28.46 ms sweep p99 while directly encoding the same 2,002,536 selected
entities and 75,709,944 bytes per run. Planning accounted for 12.13-14.02 ms p99
and encoding for 13.83-15.07 ms p99. This is guarded regression evidence with
30 Hz headroom on this host, not a complete room-server capacity claim.

Entity pressure can be decoupled from player count, and dirty/component pressure
can be varied independently:

```powershell
cargo run --release -q -p sectorsync-bench --example many_rooms -- `
  --min-players=4 --max-players=10 --entities-per-room=128 `
  --dirty-percent=10 --component-bytes=32 --sweep-p99-budget-ms=33.333
```

On the current host that guarded shape resolved to 500 Stations, 3,494 players,
and 64,000 entities. It selected 1,695,816 entities but encoded only 128,368
dirty entity/component deltas and 9,154,528 bytes. Three consecutive runs
reported 23.58-31.25 ms sweep p99, 11.87-14.70 ms planning p99, and
11.51-17.11 ms encoding p99.

For the denser 4-10 player, 16-entities-per-player shape, reusable batch output
retained at most ten plan slots and 1,280 selected-entity slots per Station.
Three release runs reported 19.11-23.87 ms sweep p99.

The default path preallocates known per-Station entity capacity. Use
`--no-preallocate` only for an A/B comparison. In the 500-room, 4-10 player,
16-entities-per-player shape, preallocation retained exactly 55,904 Station
record slots versus 77,568 growth-based slots. Three alternating runs reported
23.12-25.13 ms setup with preallocation and 24.24-28.01 ms without it; median
setup fell from 25.85 ms to 24.15 ms.

Use `--moving-percent` to move a deterministic share of indexed entities by a
sub-cell offset on every sweep. The default SDK path detects unchanged cell
membership before allocating or mutating index storage. The benchmark reports
`index_updates_inserted`, `index_updates_unchanged`,
`index_updates_relocated`, `movement_ms_p99`, and
`threshold_movement_updates_ok`. The legacy
`threshold_same_cell_movement_ok` field is retained for output compatibility.
`point_relocation_in_place` identifies the optimized cross-cell path.
`--cross-cell-movement` moves entities between adjacent cells, while
`--force-index-reinsert` is a benchmark-only A/B mode that removes and
reinserts each moving handle.

For the 500-room, 4-10 player, 16-entities-per-player shape with 100% movement,
447,232 same-cell index updates completed without reinsertion. Three alternating
release A/B runs reported median movement p99 of 4.481 ms versus 17.534 ms with
forced reinsertion, median sweep p99 of 21.697 ms versus 40.503 ms, and median
total time of 160.631 ms versus 270.190 ms. These figures are local regression
evidence; the counters and verdict establish which path actually ran.

With `--cross-cell-movement`, point entities retain and rewrite their existing
one-cell membership list rather than removing and rebuilding the entity index
entry. The same 447,232-update workload reported median movement p99 of
11.313 ms versus 16.992 ms with forced reinsertion, median sweep p99 of
29.919 ms versus 36.810 ms, and median total time of 208.854 ms versus
261.278 ms across three alternating release A/B runs.

Use `--component-update-percent` to rewrite a deterministic share of component
blobs before replication planning on every sweep. The default path uses
`set_blob_from_slice` to retain existing blob byte capacity;
`--force-component-replace` allocates a new payload and replaces the stored blob
for comparison. The runner reports `component_updates`,
`component_updates_in_place`, `component_updates_replaced`,
`component_update_ms_p99`, `component_update_in_place`, and
`threshold_component_updates_ok`.

In the 500-room, 4-10 player, 16-entities-per-player shape with 32-byte
components and 100% component updates, three alternating release A/B runs each
performed 447,232 writes. Median component-update p99 was 5.653 ms in place
versus 9.855 ms with replacement, median sweep p99 was 29.158 ms versus
32.465 ms, and median total time was 193.121 ms versus 219.395 ms. The isolated
component phase improved in every run; whole-sweep figures also include normal
planning and encoding variance.

Frame output capacity can be compared with `--no-frame-capacity-hint`. The
default path samples at most four uniformly distributed planned entities and
pre-sizes only when every sample contains encodable dirty data. Otherwise the
hint is zero and the output uses normal `Vec` growth. Output includes
`frame_capacity_hint_enabled`, cumulative `frame_capacity_hint_bytes`,
`frame_capacity_bytes`, `frame_capacity_slack_bytes`, and
`threshold_frame_capacity_ok`.

In the 500-room, 4-10 player, 16-entities-per-player, 32-byte component shape at
100% dirty density, five alternating release A/B runs reported median encoding
p99 of 10.547 ms with sampling versus 12.442 ms without it, median sweep p99 of
20.815 ms versus 22.967 ms, and median total time of 142.309 ms versus
166.244 ms. Both paths emitted 105,630,128 bytes; cumulative output-buffer
capacity was 105,630,128 bytes with hints versus 153,370,624 bytes through
growth, eliminating 47,740,496 bytes of cumulative slack. At 10% and 0% dirty
density the conservative gate returned zero hints and retained the same buffer
capacities as the comparison path. These cumulative values describe allocation
pressure across the run, not simultaneous resident memory.

This workload does not include gameplay logic, room creation/destruction churn,
idle-room scheduling, command/event pumps, kernel networking, persistence, or
matchmaking. Treat it as evidence for active-room spatial planning and encoding,
not a complete room-server capacity promise.

### Dynamic Gameplay Measurement

`dynamic_gameplay` is the broader deterministic application-shaped workload.
It keeps game semantics inside the benchmark while exercising SectorSync's
public middleware path: 4-10 players per room, active/idle/hot room mixes,
gateway command admission, bounded command queues, player/NPC movement, sparse
component changes, projectile spawn/despawn, event routing, reusable batch
planning, direct binary frame encoding, in-memory packet delivery, borrowed
client decode, replication tracking, immediate benchmark ACKs, and bounded room
recreation.

Run the smoke-safe release profile with:

```powershell
cargo run --release -q -p sectorsync-bench --example dynamic_gameplay
```

The default workload uses 20 rooms, 128 initial entities per room, and 30
ticks. `standard` (100 rooms, 256 entities, 180 ticks) and `large` (500 rooms,
512 entities, 300 ticks) require `--allow-heavy`. Manual values remain clamped
to the active profile's limits, and every run has a cooperative ten-second
budget.

Two initial release runs completed all 30 ticks with 1,392 admitted and
applied commands, 60 routed and drained events, 4,110 viewer plans and packets,
3,035,294 encoded and decoded bytes, zero oversized packets, and 6.595-6.875 ms
tick p99 on the development host. They selected 537,076 entities but encoded
41,409, exposing about 13x redundant planner and tracker work.

After moving the benchmark's caller-owned dirty rule into planner eligibility,
the same deterministic workload selected and encoded 41,409 entities, reducing
the ratio to 1.000 and tracker ACK work from 527,157 to 40,234. The final path
compiles dirty handles into a dense index/generation marker before viewer
planning, avoiding repeated component-store probes. Encoded bytes remain
3,035,294. Five final release runs reported median tick p99 of 6.940 ms and
median total elapsed time of 34.531 ms; the initial recorded run took 62.737 ms.
Host timings are directional rather than portable acceptance thresholds.

The workload uses the explicit work-bounded planner path. Its
`unexamined_after_budget` counter is machine-readable; it remains zero in the
default sparse-dirty trace because no viewer fills the 256-entity budget. Core
tests separately verify that a saturated first-fit plan stops after selecting
its budget and reports the untouched candidate suffix.

Room recreation also exercises explicit teardown. The smoke trace unregisters
the old router queue and all client/server endpoints before registering the
replacement room. The measured run removed six endpoints, discarded zero
queued packets and events, and released 20 retained packet slots. Lifecycle
acceptance checks the expected endpoint count, zero backlog, and nonzero released
capacity whenever recreation occurs.

The trace assigns inventory to sparse `ComponentId` 65,535. Direct low-ID slots
plus the sparse high-ID table retain 100 slots for 60 registered room-local
columns across 20 rooms; capacity no longer scales to the maximum numeric ID.
All 2,620 indexed
entities at measurement time use inline single-cell membership, eliminating
their former one-element membership allocations. Both conditions have explicit
machine-readable threshold verdicts.

Machine-readable output retains phase percentiles, command/event/transport
conservation, spawn/despawn and lifecycle counts, selected/encoded/component
counts, actual bytes, packet-budget failures, tracker state/ACK counts, world
and client checksums, retained capacity classes, threshold verdicts, and
`benchmark_ok`. It does not implement combat, persistence, matchmaking, kernel
networking, or production client prediction.

The focused `replication_frame_budget` workload deliberately underestimates 32
64-byte component updates as 256 planner bytes. With a concrete 512-byte frame
budget, bounded encoding admitted four entities into 417 bytes and atomically
rolled back the fifth. Its verdict requires the final frame to remain within
budget despite the optimistic planner estimate.

## Hot-Path Measurements

These focused A/B runs isolate one algorithm, lookup, ownership, or allocation
choice. Their percentages must not be added together or treated as whole-SDK
speedups.

### Batch AOI Candidate Reuse Measurement

The configured batch planner quantizes each viewer sphere to the exact cell
range used by `CellIndex`. Within one call, it queries the first occurrence of
a repeated range and reuses that ordered candidate slice for later viewers.
Unique ranges are never copied, cache entries are caller-owned and retained by
`ReplicationBatchScratch`, and the product executor bounds active entries to 64
by default. Visibility, eligibility, cadence, priority, and byte/entity budgets
still run independently for every viewer.

Run guarded dense and sparse comparisons with reuse enabled and disabled:

```powershell
cargo run --release -q -p sectorsync-bench --example batch_aoi_reuse -- `
  --shape=identical --layout=dense
cargo run --release -q -p sectorsync-bench --example batch_aoi_reuse -- `
  --shape=identical --layout=dense --no-query-reuse
cargo run --release -q -p sectorsync-bench --example batch_aoi_reuse -- `
  --shape=partial --layout=sparse
cargo run --release -q -p sectorsync-bench --example batch_aoi_reuse -- `
  --shape=unique --layout=dense
```

Shapes cover identical ranges, paired partial overlap, mixed repeated/unique
ranges, and fully unique quantized ranges. Guards cap 4,000 entities, 500
viewers, and 30 calls without `--allow-heavy`; execution also has a 10-second
budget. Machine-readable output includes unique/reused range counts, actual
grid/occupied queries and scanned cells, retained cache capacity, deterministic
output checksums, latency percentiles, and `benchmark_ok`.

With 4,000 entities, 500 identical-range viewers, and 20 calls, five
alternating release A/B runs produced identical outputs. Reuse performed 20
occupied-cell queries versus 10,000 with reuse disabled. Median call p99 was
1.209 ms versus 3.838 ms, about a 68.5% reduction on this development host.
The fully unique shape reports zero reused ranges and equal query counts; a
sample was effectively neutral at 0.325 ms versus 0.318 ms, so no unique-range
latency improvement is claimed. Output equality, zero reuse for unique ranges,
bounded retained capacity, reduced query work for repeated ranges, and
`benchmark_ok=true` are the portable acceptance signals.

### Multi-Cell Bounds Update Measurement

The guarded `multi_cell_bounds` benchmark covers repeated sphere updates whose
27-cell membership remains unchanged. The default path compares retained cells
directly in deterministic grid order. `--materialize-cell-list` additionally
constructs and consumes the temporary list removed by the optimized path:

```powershell
cargo run --release -q -p sectorsync-bench --example multi_cell_bounds
cargo run --release -q -p sectorsync-bench --example multi_cell_bounds -- `
  --materialize-cell-list
```

The default guard allows at most 50,000 entities and 20 ticks without
`--allow-heavy`; execution also has a 10-second budget. Machine-readable output
includes update percentiles, update outcome counts, materialized cell count,
path/workload/membership verdicts, and `benchmark_ok`.

For 20,000 bounded entities across ten ticks, all 200,000 updates retained their
membership. Three alternating release runs reported median update p99 of
3.194 ms without materialization versus 7.914 ms when materializing 5,400,000
temporary cell coordinates. This isolates the removed list-construction work;
the cross-cell comparison below isolates membership mutation work separately.

Cross-cell mode exercises the incremental membership path against the previous
full remove-and-insert behavior:

```powershell
cargo run --release -q -p sectorsync-bench --example multi_cell_bounds -- `
  --cross-cell --entities=5000 --ticks=10
cargo run --release -q -p sectorsync-bench --example multi_cell_bounds -- `
  --cross-cell --entities=5000 --ticks=10 --force-full-rebuild
```

The 27-cell sphere moves one cell along X on every update. Sorted merge-diff
retains 18 overlapping cells, removes nine exited cells, and inserts nine new
cells; the full path removes and inserts all 27. `CellIndexUpdateScratch`
retains capacity for 27 coordinates and `CellIndexUpdateReport` exposes the
actual retained/removed/inserted counts. Final membership is independently
checked for every entity and included in a deterministic checksum.

Five alternating release A/B runs of 50,000 crossings retained the identical
`8307051827676118908` membership checksum. Median update p99 was 9.298 ms for
incremental diff versus 17.109 ms for full rebuild, about a 45.7% reduction on
this development host; one noisy pair reversed, so timing remains directional.
Exact 18/9/9 work counts, 27-cell scratch capacity, final membership equality,
stable checksum, and `benchmark_ok=true` are the portable acceptance signals.

### Single-Viewer Plan Output Measurement

`ReplicationExecutor` retains one `ReplicationPlan` output across configured
single-viewer calls. The guarded `single_viewer_planning` low-level benchmark
isolates reusable output from already reused spatial scratch.
`--fresh-plan-output` explicitly materializes a new plan at the benchmark
boundary:

```powershell
cargo run --release -q -p sectorsync-bench --example single_viewer_planning -- `
  --entities=32 --calls-per-tick=500 --ticks=30
cargo run --release -q -p sectorsync-bench --example single_viewer_planning -- `
  --entities=32 --calls-per-tick=500 --ticks=30 --fresh-plan-output
```

The default guard caps 4,000 entities, 500 calls per tick, and 30 ticks, with a
10-second execution budget. Output includes call/selection counts, fresh output
count, retained plan capacity, latency percentiles, path/workload verdicts, and
`benchmark_ok`.

For 32 candidates and 15,000 planning calls, five alternating release A/B runs
reported median tick p99 of 0.287 ms with retained output versus 0.364 ms with
fresh output. The retained path performed no fresh plan-output allocations and
held capacity for 32 handles; the comparison created 15,000 outputs. At 2,000
candidates and 4,000 calls, median p99 was effectively neutral (3.974 ms versus
3.994 ms) because query/filter work dominated, while output allocations were
still eliminated.

### Budgeted Priority Top-K Measurement

Priority planning uses the same total comparator as before: score descending,
distance ascending, then handle ascending. When the selected budget is less
than half of eligible candidates, it partitions the deterministic top-k set and
sorts only that prefix. At or above half it uses full sorting directly. The
guarded benchmark reuses one candidate buffer so the comparison contains no
allocation difference:

```powershell
cargo run --release -q -p sectorsync-bench --example priority_top_k
cargo run --release -q -p sectorsync-bench --example priority_top_k -- --full-sort
```

The default guard caps 10,000 candidates, 200 calls per tick, and 30 ticks, with
a 10-second execution budget. For 2,000 candidates, a limit of 32, and 2,000
selection calls, five alternating release runs produced identical selected
counts and checksums. Median tick p99 was 0.781 ms for top-k versus 4.691 ms for
full sorting, an 83% reduction in the isolated selection phase. Core tests
compare top-k output against full sorting across zero, small, boundary, equal,
and oversized limits. A `limit=1800` check reports `partition_applied=false`,
confirming the high-budget fallback.

### Parallel Multi-Station Output Measurement

`ParallelReplicationScratch` can retain both one planning lane per bounded
worker and one `ReplicationBatchScratch` output slot per observed Station batch.
The borrowed `ParallelReplicationView` path avoids rebuilding per-room batch
results and per-viewer selected-entity buffers on each planning call. Compare it
with the owned-result compatibility path using:

```powershell
cargo run --release -q -p sectorsync-bench --features parallel --example parallel_output_reuse
cargo run --release -q -p sectorsync-bench --features parallel --example parallel_output_reuse -- --fresh-output
```

The default workload is 12 rooms, six players and 128 entities per room, 20
planning calls per tick, and 20 ticks. Without `--allow-heavy`, guards cap 64
rooms, ten players, 512 entities per room, 100 calls per tick, and 30 ticks;
execution also has a 10-second budget. Output retains room/player/entity guard
metadata, selected checksum, fresh-output count, retained slot/entity capacity,
pool thread and active scratch-lane counts, latency percentiles,
path/workload/lane-utilization verdicts, and `benchmark_ok`. With 12 rooms and
the bounded two-thread pool, both lanes must be active.

Five alternating release A/B runs produced the same `2,764,800` selected
checksum. The reusable path created zero fresh results and retained 12 Station
slots with total selected-entity capacity of 6,912; the owned path created 400
fresh results per run. On this development host, median tick p99 was 1.986 ms
with retained output versus 3.363 ms with fresh output, about a 41% reduction.
Runtime tests also compare every active Station plan and aggregate statistic,
verify capacity retention after a smaller subsequent batch, and cover empty
input.

### Event Drain Output Measurement

`EventQueues::drain_ready_into` visits each priority queue only for its initial
length, moves ready events to caller-owned output, and rotates delayed events to
the same queue tail. This removes the previous delayed-event vector and requeue
pass while retaining Critical, Important, BestEffort and FIFO ordering.
`StationScheduler::drain_ready_events_into` additionally retains aggregate ready
output across ticks:

```powershell
cargo run --release -q -p sectorsync-bench --example event_drain_reuse
cargo run --release -q -p sectorsync-bench --example event_drain_reuse -- --fresh-output
```

The default workload uses 12 Stations, 16 events per Station, 20 drain calls per
tick, and 20 ticks. Half of each call's events target the next tick, continuously
exercising delayed rotation. Without `--allow-heavy`, guards cap 32 Stations, 64
events per Station, 50 calls per tick, and 20 ticks; execution has a 10-second
budget. Output includes routed/drained counts, fresh-output count, retained
capacity, latency percentiles, guard metadata, workload/path verdicts, and
`benchmark_ok`.

Five alternating default A/B runs produced identical `76,800` routed and
`76,704` drained counts. The reusable path created zero fresh outputs and
retained capacity for 256 events; the compatibility path created 400 owned
outputs. Latency was noisy and did not show a reliable advantage, so no timing
improvement is claimed. At the guarded 64-event, 50-call shape, both paths again
matched at `768,000` routed and `767,616` drained; median tick p99 was 1.438 ms
with reuse and 1.516 ms with fresh output. The durable acceptance evidence is
the removed intermediate allocation paths, exact event-count equivalence, core
priority/FIFO tests, and retained-capacity test rather than a host-specific
latency delta.

### Station Schedule Scratch Measurement

`StationScheduleScratch` retains the Station-to-score hash table and candidate
array across load-aware scheduling calls. `plan_loaded_into` and
`advance_loaded_into` return a borrowed ordered view. When the selected budget
is less than half the Station count, the scheduler uses the same total ordering
as before to partition top-k and sort only the selected prefix; at or above half
it uses full sorting:

```powershell
cargo run --release -q -p sectorsync-bench --example station_schedule_reuse
cargo run --release -q -p sectorsync-bench --example station_schedule_reuse -- --fresh-output
```

The default workload contains 512 Stations, a selection limit of 16, 100 calls
per tick, and 20 ticks. Without `--allow-heavy`, guards cap 2,000 Stations, 200
calls per tick, and 30 ticks; the limit is always clamped to the active Station
count and execution has a 10-second budget. Output includes the partition-path
flag, exact selection checksum, fresh-output count, retained score/candidate
capacity, latency percentiles, guard metadata, workload/path verdicts, and
`benchmark_ok`.

Five alternating release A/B runs produced the same `8,312,000` checksum. The
reusable path created zero fresh results and retained score/candidate capacity;
the comparison created 2,000 fresh scheduler results. Median tick p99 was
1.764 ms with reuse versus 3.206 ms with fresh storage, about a 45% reduction on
this development host. Core tests compare partition output with full sorting at
zero, small, half, full, and oversized budget edges. A `limit=400` run reports
`top_k_partition_applied=false` and the same `5,255,150` checksum for both output
modes, confirming the high-budget fallback.

### Hotspot Split Scratch Measurement

`HotspotSplitScratch` retains copied `CellLoadSample` candidates across hotspot
passes, while `propose_cell_split_into` also retains the proposal's coordinate
buffer. A move budget below half the sampled cells partitions the deterministic
top-k pressure set and sorts only that prefix; larger budgets use full sorting.
`SplitScheduler` exposes the same candidate reuse through explicit scratch APIs:

```powershell
cargo run --release -q -p sectorsync-bench --example hotspot_split_reuse
cargo run --release -q -p sectorsync-bench --example hotspot_split_reuse -- --fresh-output
```

The default workload contains 2,000 cells, a move limit of eight, 100 calls per
tick, and 20 ticks. Without `--allow-heavy`, guards cap 10,000 cells, 200 calls
per tick, and 30 ticks; the limit is clamped to the sampled cell count and the
run has a 10-second budget. Output includes partition-path state, exact moved-
pressure checksum, fresh-output count, retained candidate/proposal capacity,
latency percentiles, guard metadata, path/workload verdicts, and `benchmark_ok`.

Five alternating release A/B runs produced the same `823,782,000` checksum. The
reusable path created zero fresh results and retained 2,000 candidate plus eight
proposal slots; the comparison created 2,000 fresh results. Median tick p99 was
3.755 ms with reuse versus 4.022 ms with fresh storage, about a 7% reduction on
this development host. Core tests compare selected coordinates and moved score
with full sorting across zero, small, half, full, and oversized budgets. A
`limit=1500` run reports `top_k_partition_applied=false` and the same
`2,601,231,850` checksum in both modes, confirming high-budget fallback.

### Split Schedule Nested Output Measurement

`SplitSchedulerScratch` retains fixed decision and action slots while exposing
only active prefixes through `SplitScheduleView`. Each decision retains its
reason capacity, each action retains its proposal coordinate capacity, and the
same storage owns hotspot cell candidates and the pending proposal. Borrowed
views can be executed or recorded into cooldown state without materializing an
owned schedule:

Decisions remain aligned with input samples. Source severity and target
severity are read by index during planning, and the current target comparison
key is retained, avoiding repeated Station-id scans inside the target loop.

```powershell
cargo run --release -q -p sectorsync-bench --example split_schedule_reuse
cargo run --release -q -p sectorsync-bench --example split_schedule_reuse -- --fresh-output
```

The default workload contains 64 Stations, four hot sources, 128 cells per hot
source, four actions per pass, 100 calls per tick, and 20 ticks. Guards cap 256
Stations, 512 cells per hot source, 16 actions, 200 calls per tick, and 30 ticks
without `--allow-heavy`; execution has a 10-second budget. Output includes an
exact checksum covering decision severity/reasons, actions, pressure scores and
all skip counters, fresh-output count, every retained-capacity class, latency
percentiles, guard metadata, path/workload verdicts, and `benchmark_ok`.

Five alternating release A/B runs produced the same `270,716,000` checksum. The
reusable path created zero fresh schedules and retained 64 decision slots, four
action slots, reason/proposal capacity, and 128 hotspot candidates; the owned
path created 2,000 fresh schedules. Median tick p99 was effectively neutral at
2.466 ms with reuse versus 2.506 ms with fresh output because cell-pressure and
target scans dominated. A larger guarded Station/action shape was highly noisy,
so no latency improvement is claimed. Acceptance is based on exact full-field
equivalence, removed nested allocation paths, retained-capacity tests, and the
executable borrowed `split_migration` flow.

### Gateway Expiry Scan Measurement

`GatewaySessionTable::disconnect` records the first tick after the reconnect
grace window in a min-deadline heap. Caller-driven `expire_disconnected` pops
only due entries and verifies the current disconnect tick before removal, so a
stale entry cannot remove a reconnected session. No timer or background thread
is created. Repeated reconnects may leave stale entries, but the heap is rebuilt
from current disconnected sessions when it exceeds twice `max_sessions`.

Compare the production deadline index with the previous full map scan:

```powershell
cargo run --release -q -p sectorsync-bench --example gateway_deadline_expiry -- `
  --sessions=65536 --calls=10 --expired-every=1024
cargo run --release -q -p sectorsync-bench --example gateway_deadline_expiry -- `
  --sessions=65536 --calls=10 --expired-every=1024 --full-scan
```

Snapshot cloning and result checksum traversal occur outside the measured
operation. Guards cap 65,536 sessions and 50 calls without `--allow-heavy`;
execution has a 10-second budget. `--expired-every` controls sparse disconnect
density and retains one exact-grace-boundary record beside each expired record.
Output includes expired/remaining conservation, examined entries, stale and
capacity fields, deterministic checksum, latency percentiles, and
`benchmark_ok`.

At 65,536 sessions, 64 expired sessions per call, and ten calls, five
alternating release A/B runs produced the same `471817112713081856` aggregate
checksum. The deadline path examined 640 entries versus 655,360 for full scans.
Median operation p99 was 0.023 ms versus 0.944 ms, about a 97.6% reduction on
this development host. At 20,000 sessions with 2.5% expired per call, an early
sample was slower than retain scanning (0.526 ms versus 0.253 ms), while the
65,536-session form of that density was faster (0.710 ms versus 0.924 ms).
Therefore the accepted claim is bounded maintenance scaling near the configured
table limit, not universal latency improvement at every size and churn ratio.
Exact conservation/checksum equality, due-only work, grace-boundary retention,
stale-entry safety, bounded compaction, and `benchmark_ok=true` are portable.

The older `gateway_expiry_scan` example remains an algorithm record showing why
in-place retain was preferable to temporary id collection before deadline
indexing; it no longer represents the production expiry implementation.

### Gateway Session Lookup Measurement

`GatewaySessionTable` starts with ordered session storage and promotes once to
hash storage when adding the 1,024th distinct Client. The one-way transition
preserves route, generation, route epoch, sequence/rate state, connection state,
expiry behavior, capacity checks, and cumulative statistics. Gateway APIs do
not expose storage iteration order.

Compare Gateway-shaped records under identical route and admission operations:

```powershell
cargo run --release -q -p sectorsync-bench --example gateway_session_lookup
cargo run --release -q -p sectorsync-bench --example gateway_session_lookup -- --btree
cargo run --release -q -p sectorsync-bench --example gateway_session_lookup -- --double-lookup
```

At ten sessions, seven release runs produced median p50 values of 0.578 ms for
ordered lookup and 1.166 ms for hash lookup. At 1,024 sessions hash lookup was
1.326 ms versus 1.588 ms ordered; at the default 4,096 sessions it was 1.140 ms
versus 2.952 ms, about a 61% reduction. Each run performs one million mixed
route reads and admission-style updates with identical 125,000 admission counts
and checksums. Guards cap sessions, operations/tick, ticks, and total operations
unless `--allow-heavy` is present; output includes latency percentiles,
operation/admission/checksum fields, guard metadata, workload/admission/time
verdicts, and `benchmark_ok=true`.

The `--double-lookup` comparison models the previous existing-session connect
path, which checked membership and then fetched the same mutable record. Across
seven release runs at ten ordered sessions, removing the second probe reduced
median p50 from 1.022 ms to 0.543 ms. At 4,096 hashed sessions it reduced p50
from 2.183 ms to 1.351 ms. Both modes execute one million identical operations
and 125,000 admission-style updates with equal checksums; `map_probes` falls
from exactly two million to one million.

### Deployment Stale-Node Scan Measurement

`DeploymentRouteTable::mark_stale_offline` now scans mutable node records once,
marks only newly stale non-offline nodes, advances route epochs, and accumulates
both stale-detection and offline counters directly. `stale_nodes` remains an
ordered allocating query for callers that need IDs. The benchmark compares the
direct mark with the previous public `stale_nodes` plus `mark_offline` sequence:

```powershell
cargo run --release -q -p sectorsync-bench --example deployment_stale_scan
cargo run --release -q -p sectorsync-bench --example deployment_stale_scan -- --collect-mark
```

Snapshot cloning and final route checksum occur outside the measured operation.
The default workload maintains 5,000 mixed fresh, grace-boundary, stale, and
already-offline nodes across 500 calls. Guards cap 20,000 nodes, 100 calls per
tick, and 20 ticks without `--allow-heavy`; execution has a 10-second budget.
Output includes marked count, route-state/epoch checksum, temporary-id collection
count, operation percentiles, guard metadata, path/workload verdicts, and
`benchmark_ok`.

Five alternating release A/B runs marked `625,000` nodes with the same
`8,750,000` route checksum. The direct path created zero temporary ID collections;
the comparison created 500 per run. Median operation p99 was 0.069 ms direct
versus 0.175 ms for collect/mark, about a 61% reduction on this development host.
Runtime tests cover fresh, exact-boundary, stale, existing-offline, repeated-pass,
route-epoch, and counter behavior; benchmark tests compare every node route.

### Load Sampling Output Reuse Measurement

`StationLoadSampler::sample_all_into` retains subscriber aggregation, sorted
occupancy scratch, outer Station sample slots, and each Station's cell output.
`CellIndex::cell_occupancy_into` supplies deterministic caller-owned occupancy
storage. The owned `sample_all` and `cell_occupancy` APIs remain available for
results that must outlive scratch. Compare both sampling paths with:

```powershell
cargo run --release -q -p sectorsync-bench --example load_sampling_reuse
cargo run --release -q -p sectorsync-bench --example load_sampling_reuse -- --fresh-output
```

The default workload samples 256 rooms with 16 entities and two subscriber
records per room across 200 calls. Guards cap 1,000 rooms, 64 entities per room,
50 calls per tick, and 20 ticks without `--allow-heavy`; execution has a
10-second budget. Output retains machine-readable entity, cell, subscriber, and
sample checksums, fresh-output count, all scratch capacity classes, latency
percentiles, guard metadata, path/workload verdicts, and `benchmark_ok`.

Five alternating release A/B runs produced identical `819,200` entity and cell
checksums plus a `482,800` subscriber checksum. The reusable path created zero
fresh outputs and retained 256 Station slots plus 4,096 cell slots; the owned
path created 200 fresh outputs. Median tick p99 was 3.782 ms reusable versus
6.429 ms owned, about a 41% reduction on this development host. Core/runtime
tests verify deterministic occupancy, exact sample equivalence, duplicate
subscriber aggregation, and retained outer and nested storage; benchmark tests
verify guard enforcement and cross-path checksums.

### Reliable Frame And Retry Reuse Measurement

`ReliableClientFrame::encode_data` and `ReliableStationFrame::encode_data`
append borrowed payloads directly to the final wire buffer. Reliable senders use
these paths for initial sends and retries, avoiding the temporary owned-frame
payload copy. `ReliableClientEndpoint` and `ReliableStationEndpoint` retain
due-entry scratch behind their normal `retry_due` method. Low-level senders
expose only `retry_due_into` with caller-owned scratch.

Deadline indexing is covered separately by the fixed smoke-safe
`reliable_retry` workload. It keeps 1,024 packets in flight, performs 32
non-due polls, then advances to the common deadline. The measured run examined
zero entries during non-due polls and exactly 1,024 at the deadline, retrying
all of them in 113 microseconds on the development host. The work-count verdict,
not host timing, proves polling no longer scans the complete window.

Run the isolated encoding and real Station sender comparisons with:

```powershell
cargo run --release -q -p sectorsync-bench --example reliable_frame_encode
cargo run --release -q -p sectorsync-bench --example reliable_frame_encode -- --owned-frame
cargo run --release -q -p sectorsync-bench --example reliable_retry_reuse
cargo run --release -q -p sectorsync-bench --example reliable_retry_reuse -- --fresh-scan
```

The encoding workload writes 2,000 frames per tick with 2 KiB payloads for ten
ticks. Guards cap 4,000 frames per tick, 4 KiB payloads, and 20 ticks without
`--allow-heavy`. Five alternating release A/B runs produced identical 20,000
frames, `41,300,000` wire bytes, and a `44,760,000` checksum. Borrowed encoding
created zero temporary payload copies versus 20,000 for owned frames. Median
tick p99 was 0.253 ms borrowed versus 0.455 ms owned, about a 44% reduction on
this development host.

The retry workload keeps 512 packets with 512-byte payloads in flight across
200 retry calls. It caps 2,000 packets, 4 KiB payloads, 25 calls per tick, and
ten ticks, plus a 10-second execution budget. Both paths retried 102,400 packets
and emitted `54,169,600` wire bytes with a `72,192,000` checksum and zero
wire divergence. Reusable scans retained 512 keys and created zero fresh key
collections; compatibility scans created 200. Five-run p99 medians were 1.184 ms
reusable and 1.140 ms fresh, so no host-latency improvement is claimed for scan
reuse alone. Its acceptance criterion is allocation removal and exact behavior,
not a timing threshold. Transport tests cover byte equality, capacity retention,
duplicate suppression, failed-send attempt preservation, and ordered timeout
behavior.

### Reliable Receive Payload Ownership Measurement

`ReliableClientFrame::decode_ref` and `ReliableStationFrame::decode_ref`
validate frame structure while borrowing data payload bytes. Standard reliable
endpoints use those views, move the payload to the front of the already-owned
wire Vec, and truncate it before delivery. Compatible owned frame decoders
materialize a new payload Vec.

Compare wire reuse with owned payload decoding using:

```powershell
cargo run --release -q -p sectorsync-bench --example reliable_receive_ownership
cargo run --release -q -p sectorsync-bench --example reliable_receive_ownership -- --owned-decode
cargo run --release -q -p sectorsync-bench --example reliable_receive_ownership -- --station
cargo run --release -q -p sectorsync-bench --example reliable_receive_ownership -- --station --owned-decode
```

The default preloaded workload receives 2,000 frames per tick with 1 KiB
payloads for ten ticks. Guards cap 4,000 frames per tick, 4 KiB payloads, 20
ticks, and 64 MiB aggregate payload work without `--allow-heavy`; execution has
a 10-second budget.

Five alternating release A/B runs per frame kind each processed 20,000 frames
and `20,480,000` payload bytes with a `20,880,000` checksum. Wire reuse retained
the original Vec pointer for all 20,000 payloads and created zero fresh payloads;
owned decoding created 20,000. Client median tick p99 was 0.494 ms reuse versus
0.623 ms owned, about a 20.7% reduction. Station median tick p99 was 0.527 ms
reuse versus 0.672 ms owned, about a 21.6% reduction. Exact bytes/checksums,
full pointer reuse, zero fresh payloads, endpoint ACK/duplicate tests, guard
metadata, and `benchmark_ok=true` are the portable acceptance signals.

### Reliable Window Count Measurement

Reliable Client and Station senders maintain a `BTreeMap` count per active peer
or target. `in_flight_for` and send-window admission therefore perform one
O(log active peers) lookup instead of scanning the full in-flight packet map.
New-key insertion increments the count only when the packet map actually grows;
ACK and timeout removal decrement it and remove zero-count entries. Saturated
`u64::MAX` sequence replacement retains the existing count.

Compare the indexed query against the previous full-key scan with:

```powershell
cargo run --release -q -p sectorsync-bench --example reliable_window_lookup
cargo run --release -q -p sectorsync-bench --example reliable_window_lookup -- --full-scan
```

The default workload keeps 4,096 packets across 256 peers and performs 10,000
window queries. Guards cap 8,000 packets, 2,000 peers, 2,000 queries per tick,
and ten ticks without `--allow-heavy`; execution has a 10-second budget. Output
includes query count/checksum, full-scan count, in-flight conservation, latency
percentiles, guard metadata, path/workload verdicts, and `benchmark_ok`.

Five alternating release A/B runs produced the same `160,000` count checksum.
Indexed lookup performed zero full scans; the comparison performed 10,000.
Median tick p99 was 0.034 ms indexed versus 0.947 ms scanning, about a 96%
reduction on this development host. Client and Station tests cover independent
peer counts, ACK decrement, zero-count cleanup, timeout cleanup, send failure,
window limits, and saturated-sequence replacement.

### Bounded Duplicate Index Measurement

Security replay and reliable Client/Station duplicate histories retain FIFO
eviction but select their membership index from the configured bound. Histories
below 256 entries use `BTreeSet`; histories of 256 or more use `HashSet` without
preallocating the configured maximum at construction.

Compare the hash and ordered index shapes with:

```powershell
cargo run --release -q -p sectorsync-bench --example bounded_dedup_index
cargo run --release -q -p sectorsync-bench --example bounded_dedup_index -- --btree
```

The default workload retains 4,096 keys and performs 1,000,000 mixed insert,
evict, and duplicate lookup operations. Five release runs produced median p50
values of 2.358 ms for hash lookup and 5.352 ms for ordered lookup on this
development host, about a 56% reduction. At 16 retained keys, ordered lookup was
faster (1.863 ms versus 2.855 ms); checks at 32, 64, 128, and 256 placed the
observed crossover between 128 and 256, which defines the conservative switch
threshold. Output retains operation conservation, retained count, checksum,
latency percentiles, guard metadata, workload/time verdicts, and
`benchmark_ok=true` as portable acceptance signals.

### Replication Tracker Capacity Guard Measurement

`ReplicationTracker::record_plan_sent` first checks whether current entries plus
the full plan length fit under `max_entries`. When they do, capacity is proven
even if every entity is new, so insertion proceeds without per-entity lookups.
Only plans near the bound perform the exact existing/new scan; capacity failure
still occurs before any record or statistic changes.

Compare the O(1) conservative guard with an unconditional exact scan using:

```powershell
cargo run --release -q -p sectorsync-bench --example tracker_capacity_guard
cargo run --release -q -p sectorsync-bench --example tracker_capacity_guard -- --exact-scan
```

The default workload keeps 4,096 records and repeatedly updates a 256-entity
plan across 1,000 calls. Seven release runs reduced median p50 from 0.818 ms
exact-scan to 0.380 ms fast-guard, about 54%, while capacity probes fell from
256,000 to zero. Both paths perform 256,000 identical record updates and produce
the same final count and checksum. Guards cap initial records, plan entities,
calls/tick, ticks, and total updates unless `--allow-heavy` is present; output
includes latency percentiles, call/update/probe/count/checksum fields, guard
metadata, workload/capacity/time verdicts, and `benchmark_ok=true`.

### Replication Tracker Map Measurement

`ReplicationTracker` starts with ordered record storage and promotes once to a
hash map when adding the 2,048th distinct client/entity key. The one-way
transition preserves last-sent and ACK ticks, capacity accounting, explicit
client cleanup, tick pruning, and cumulative statistics. Tracker APIs do not
expose storage iteration order.

Compare identical keyed reads and ACK-style updates using:

```powershell
cargo run --release -q -p sectorsync-bench --example tracker_map_lookup
cargo run --release -q -p sectorsync-bench --example tracker_map_lookup -- --btree
```

At ten records, seven release runs produced median p50 values of 0.771 ms for
ordered lookup and 1.945 ms for hash lookup. At 1,024 records ordered lookup
remained faster (1.861 ms versus 2.586 ms); at 2,048 records hash lookup was
1.745 ms versus 2.120 ms ordered. At the default 4,096 records hash lookup was
2.222 ms versus 3.923 ms, about a 43% reduction. Each run performs one million
mixed reads and 125,000 ACK updates with identical final record counts and
checksums. Guards cap records, operations/tick, ticks, and total operations
unless `--allow-heavy` is present; output includes latency percentiles,
operation/ACK/count/checksum fields, guard metadata, workload/count/time
verdicts, and `benchmark_ok=true`.

### Station Registry Lookup Measurement

`StationSet` and `StationIndexSet` retain deterministic Vec iteration while
switching ID lookup adaptively. Below 64 slots they scan the small Vec without
allocating a lookup table; at 64 slots they build a `HashMap<StationId, usize>`
and use it for immutable, mutable, and paired lookup. `with_capacity` and
`reserve` preallocate ordered storage and, for larger requested sizes, lookup
capacity. Duplicate `StationSet` ids retain first-match behavior, while
`StationIndexSet::insert` still replaces in place without changing order.

Compare the indexed registry against the previous two Vec scans with:

```powershell
cargo run --release -q -p sectorsync-bench --example station_registry_lookup
cargo run --release -q -p sectorsync-bench --example station_registry_lookup -- --full-scan
```

The default workload registers 4,096 Stations and indexes, then performs 10,000
paired lookups. Guards cap 8,000 Stations, 2,000 queries per tick, and ten ticks
without `--allow-heavy`; execution has a 10-second budget. Output includes
lookup checksum, full-scan count, both lookup capacities and active-path flags,
latency percentiles, guard metadata, path/workload verdicts, and `benchmark_ok`.

Five alternating release A/B runs produced the same `39,687,200` checksum.
Indexed lookup performed zero full scans; the comparison performed 20,000.
Median tick p99 was 0.054 ms indexed versus 1.673 ms scanning, about a 97%
reduction on this development host. A separate four-Station check kept both
indexes inactive and reported 0.029 ms p99 versus 0.027 ms for the isolated
scan comparison, avoiding the earlier small-set hash overhead. Runtime tests
cover threshold activation, first-duplicate semantics, replacement order,
paired mutable lookup, and both capacity classes.

### Packet Security Seal Scratch Measurement

`PacketSecurityEnvelope::encode_parts` writes borrowed ciphertext/tag slices
with the same limits and wire format as the owned envelope. `PacketSecurityBox`
`seal_into`, `seal_with_nonce_into`, and `seal_with_key_ring_into` reuse a
caller-owned `PacketSecurityScratch` for ciphertext and authentication tags.
The final wire Vec remains caller-owned for transport handoff. Validation,
cipher, or authenticator failure appends no partial envelope bytes.

Compare reusable scratch with the compatible fresh-scratch path using:

```powershell
cargo run --release -q -p sectorsync-bench --example security_seal_reuse
cargo run --release -q -p sectorsync-bench --example security_seal_reuse -- --fresh-scratch
```

The default workload seals 2,000 packets per tick with 1 KiB payloads for ten
ticks. Guards cap 4,000 packets per tick, 4 KiB payloads, and 20 ticks without
`--allow-heavy`; execution has a 10-second budget. Both paths allocate the final
owned wire packet. The benchmark authenticator emits a fixed 16-byte illustrative
tag and `PlaintextPacketCipher` isolates framework buffer cost; these are not
production cryptography or algorithm-throughput measurements.

Five alternating release A/B runs produced identical 20,000 packets,
`21,240,000` wire bytes, and a `22,900,000` checksum. Reusable sealing created
zero fresh scratch sets and retained 1,024 payload plus 16 tag bytes; the
comparison created 20,000 scratch sets. Median tick p99 was 0.420 ms reusable
versus 0.640 ms fresh, about a 34% reduction on this development host. Transport
tests cover owned/borrowed byte equality, repeated pointer/capacity retention,
automatic nonce reporting, payload/tag limits, and failure atomicity.

### Packet Security Open Scratch Measurement

`PacketSecurityEnvelopeRef::decode` validates the bounded wire envelope while
borrowing its payload and tag. `PacketSecurityBox::open_with_scratch` and
`open_with_key_ring_and_scratch` authenticate those borrowed slices and reuse a
caller-owned `PacketSecurityOpenScratch` for decrypted plaintext. Compatible
owned opening remains available when the result must move or outlive scratch.

Compare reusable output with the compatible owned path using:

```powershell
cargo run --release -q -p sectorsync-bench --example security_open_reuse
cargo run --release -q -p sectorsync-bench --example security_open_reuse -- --fresh-output
```

The default workload opens 2,000 packets per tick with 1 KiB payloads for ten
ticks. Guards cap 4,000 packets per tick, 4 KiB payloads, and 20 ticks without
`--allow-heavy`; execution has a 10-second budget. The illustrative fixed tag
and `PlaintextPacketCipher` isolate framework buffer cost and are not production
cryptography or algorithm-throughput measurements.

Five alternating release A/B runs each produced 20,000 packets, `20,480,000`
opened payload bytes, and a `3,600,000` checksum. Reusable opening created zero
fresh outputs and retained 1,024 plaintext bytes; the owned comparison created
20,000 fresh output Vecs. Median tick p99 was 0.083 ms reusable versus 0.331 ms
owned, about a 75% reduction on this development host. Individual runs showed
host scheduling variance, so allocation count and workload equality are the
portable acceptance signals; timings are local evidence only. Transport tests
cover borrowed offsets and limits, owned/scratch output equality, repeated
pointer/capacity retention, key-ring opening, replay order, and authenticated
failure behavior.

### Borrowed Replication Decode Measurement

`BinaryFrameDecoder::decode_replication` validates the complete wire frame
and exposes exact-size borrowed entity and component iterators. Component bytes
remain slices of the immutable input packet. The compatible `FrameDecoder`
path materializes the same data as owned nested Vecs for retention or transfer.

Compare immediate borrowed consumption with owned materialization using:

```powershell
cargo run --release -q -p sectorsync-bench --example replication_decode_borrowed
cargo run --release -q -p sectorsync-bench --example replication_decode_borrowed -- --owned
```

The default workload decodes 100 frames per tick for ten ticks. Each frame has
64 entities, four components per entity, and 64 payload bytes per component.
Individual guards cap 500 frames per tick, 256 entities, eight components,
1 KiB component payloads, and 20 ticks; a composite guard additionally limits
decoded payload work to 64 MiB without `--allow-heavy`. Execution has a
10-second budget.

Five alternating release A/B runs each decoded 1,000 frames, 64,000 entities,
256,000 components, and `16,384,000` payload bytes with a `46,080,000`
checksum. Borrowed decoding materialized zero owned frames; the comparison
materialized 1,000 owned nested frames. After restoring single-pass parsing for
the compatible owned decoder, median tick p99 was 0.754 ms borrowed versus
2.511 ms owned, about a 70% reduction on this development host. Timings
are local evidence; identical workload/checksum, complete validation, borrowed
payload pointers, and zero owned materializations are the portable acceptance
signals.

### Borrowed UDP Receive Measurement

`UdpTransport::try_recv_ref` and
`UdpStationTransport::try_recv_station_ref` borrow datagram bytes from the
adapter's configured reusable receive buffer. The compatible receiver traits
materialize owned payload Vecs when packets must leave that lifetime.

Compare borrowed and owned localhost receive paths using:

```powershell
cargo run --release -q -p sectorsync-bench --example udp_receive_borrowed
cargo run --release -q -p sectorsync-bench --example udp_receive_borrowed -- --owned
```

The default workload sends and receives 500 localhost datagrams per tick with
1 KiB payloads for ten ticks. Guards cap 2,000 packets per tick, 4 KiB payloads,
20 ticks, and 64 MiB aggregate payload work without `--allow-heavy`. Every
packet has at most 1,000 non-blocking polls and the run has a 10-second budget.

Five alternating release A/B runs each received 5,000 packets and `5,120,000`
payload bytes with a `900,000` checksum and zero poll misses. Borrowed receive
materialized zero owned packets and reused the same buffer 4,999 times; the
comparison materialized 5,000 owned packets. Median tick p99 was 5.667 ms
borrowed versus 5.259 ms owned, so this syscall-dominated localhost measurement
does not show a latency improvement. The portable benefit is removal of
per-datagram owned payload allocation for immediate consumers, not a claim that
UDP syscall latency decreases.

### In-Memory Queue Capacity Measurement

`InMemoryTransportHub` and `InMemoryStationTransport` register empty queues
without reserving their configured packet maximum. Queues grow on demand,
retain reached capacity after draining, and continue to reject packets at the
same explicit queue limit. Per-endpoint `queued_capacity` and aggregate
`retained_queue_capacity` expose retained packet slots without estimating heap
bytes from allocator-specific details.

Run the guarded multi-room capacity workload with:

```powershell
cargo run --release -q -p sectorsync-bench --example in_memory_queue_capacity
```

The default workload models 100 rooms with ten Client queues and one Station
queue each, a 4,096-packet bound, and an eight-packet burst per target. It
delivered all 8,800 packets and retained 8,800 queue slots. Reserving every
configured maximum at registration would retain 4,509,696 slots, so lazy
registration avoided 4,500,896 slots (99.805%) on this workload. A zero-burst
run retains zero slots. Guards cap rooms, clients/room, stations/room, queue
limits, and bursts unless `--allow-heavy` is present; output includes room
latency percentiles, exact queue/packet conservation, guard metadata,
capacity/workload/lazy/time verdicts, and `benchmark_ok=true`.

### Core Queue Capacity Measurement

`CommandQueues` and `EventQueues` construct every priority queue with zero
retained slots, grow only on accepted traffic, and retain reached capacity
after draining. Command ready priorities, the barrier buffer, Event priorities,
and both aggregate ready capacities are directly observable. Queue limits and
their existing full/drop behavior are unchanged.

Run the guarded multi-room workload with:

```powershell
cargo run --release -q -p sectorsync-bench --example core_queue_capacity
```

The default workload models 100 rooms with one Station each and queues eight
normal Commands plus eight important Events per Station. It retains 1,600
slots. Reserving all default priority limits would retain 2,662,400 slots, so
lazy construction avoids 2,660,800 slots (99.940%) on this workload; a
zero-burst run retains zero. Guards cap rooms, stations/room, burst size, and
total queued items unless `--allow-heavy` is present. Output includes exact
Command/Event counts and capacities, avoided slots/percentage, room latency
percentiles, guard metadata, workload/capacity/lazy/time verdicts, and
`benchmark_ok=true`.

### In-Memory Batch Send Measurement

`InMemoryTransportEndpoint::send_batch` processes packets under bounded
64-packet lock segments. This amortizes shared Hub locking without allowing an
arbitrarily large caller batch to monopolize the Hub. Packet validation and
enqueue order match repeated `send` calls: the first error stops processing,
retains the successful prefix, and leaves the suffix untouched.

Compare bounded batch locking with per-packet sends using:

```powershell
cargo run --release -q -p sectorsync-bench --example in_memory_batch_send
cargo run --release -q -p sectorsync-bench --example in_memory_batch_send -- --per-packet
```

The default workload prebuilds 100 batches of 1,000 eight-byte packets so
outbound packet construction stays outside the timed send interval. Target
queue growth remains part of both measured paths. Seven release runs reduced
median batch p50 from 0.022 ms per-packet to 0.016 ms segmented, about 27%, while
the structural expected lock count fell from 100,000 to 1,600. Both modes enqueue exactly
100,000 packets and 800,000 payload bytes. Guards cap batches, packets/batch,
payload bytes, total packets, and aggregate payload work unless
`--allow-heavy` is present; output includes latency percentiles, send/expected-lock
counts, queue and byte conservation, guard metadata, workload/call/time
verdicts, and `benchmark_ok=true`.

### Budget Batch Scan Measurement

`BudgetedTransport::send_batch` accumulates saturated aggregate bytes and
records the first oversized packet during one metadata scan. It checks the
aggregate result first, preserving the previous error priority when both batch
and packet limits are exceeded, and never forwards a rejected batch.

Compare the production scan with the previous two-pass shape using:

```powershell
cargo run --release -q -p sectorsync-bench --example budget_batch_scan
cargo run --release -q -p sectorsync-bench --example budget_batch_scan -- --double-scan
```

The default workload revalidates one prebuilt 100,000-packet batch for ten
ticks. Seven release runs reduced median p50 from 0.065 ms double-scan to
0.056 ms single-scan, about 14%, while metadata inspections fell from two
million to one million. Both paths produce identical aggregate bytes,
oversized-packet state, and checksum. Guards cap packet count, payload bytes,
ticks, total inspections, and resident payload work unless `--allow-heavy` is
present; output includes latency percentiles, inspection/byte/checksum fields,
guard metadata, workload/inspection/validation/time verdicts, and
`benchmark_ok=true`.

### In-Memory Endpoint Lookup Measurement

Client and Station in-memory transport registries use ordered maps below 2,048
entries and promote once to hash maps when adding the 2,048th distinct key.
This keeps the lower constant cost of ordered lookup for normal small rooms and
removes logarithmic lookup growth when one process aggregates thousands of
endpoints. Replacement does not promote early, and migration preserves every
key, queue, and retained capacity. Promotion is intentionally one-way so
registration churn around the threshold cannot repeatedly migrate storage.

Compare identical mixed immutable/mutable lookup streams with:

```powershell
cargo run --release -q -p sectorsync-bench --example endpoint_map_lookup
cargo run --release -q -p sectorsync-bench --example endpoint_map_lookup -- --btree
cargo run --release -q -p sectorsync-bench --example endpoint_map_lookup -- --double-lookup
```

Seven release runs at 1,024 endpoints produced median p50 values of 0.914 ms
for ordered lookup and 0.953 ms for hash lookup. At 2,048 endpoints hash lookup
measured 0.930 ms versus 1.103 ms ordered; at the default 4,096 endpoints it
measured 0.936 ms versus 2.904 ms, about a 68% reduction. Each run performs one
million identical lookups with one mutation per eight operations. Guards cap
endpoint count, lookups/tick, ticks, and total lookups unless `--allow-heavy`
is present; output retains checksum/workload verdicts, latency percentiles,
guard metadata, time-budget state, and `benchmark_ok=true`.

The `--double-lookup` comparison models the previous Client send path, which
queried target length and then queried the same target again for mutation. In
seven release runs at ten ordered endpoints, removing the second probe reduced
median p50 from 0.968 ms to 0.547 ms. At 4,096 hashed endpoints it reduced p50
from 2.063 ms to 1.077 ms. Both modes execute one million logical operations
with identical mutations and checksums; `map_probes` is exactly two million for
the comparison and one million for the optimized shape.

### Replication Receive Visitor Measurement

`ReplicationReceiveBridge::pump` consumes transport packets, validates
expected source and frame target, performs complete borrowed wire validation,
updates bridge statistics, and invokes a fallible caller visitor without
materializing nested owned replication frames. The explicit `pump_owned` path
returns owned frames for retention and transfer.

Compare immediate visitor application with owned pumping using:

```powershell
cargo run --release -q -p sectorsync-bench --example replication_receive_visit
cargo run --release -q -p sectorsync-bench --example replication_receive_visit -- --owned
```

The default preloaded in-memory workload receives 100 frames per tick for ten
ticks. Each frame has 64 entities, four components per entity, and 64 payload
bytes per component. Guards cap 500 frames per tick, 256 entities, eight
components, 1 KiB payloads, 20 ticks, and 64 MiB aggregate payload work without
`--allow-heavy`; execution has a 10-second budget.

Five alternating release A/B runs each accepted 1,000 packets/frames, 64,000
entities, 256,000 components, and `16,384,000` payload bytes with a `46,080,000`
checksum. Visitor receive materialized zero owned frames; owned pumping
materialized 1,000. Median tick p99 was 1.539 ms visitor versus 4.220 ms owned,
about a 64% reduction on this development host. Identical bridge counters,
workload/checksum, zero visitor materializations, visitor-error propagation,
and complete source/wire/target validation are the portable acceptance signals.

### Mixed Client Receive Visitor Measurement

`ClientTransportBridge::pump` accepts command ACKs, borrowed replication
frames, and barrier notifications in one fallible visitor loop. It shares
expected-source, target, complete wire validation, and cumulative statistics
with explicit `pump_owned` mixed pumping while avoiding nested replication frame
materialization.

Compare visitor and owned mixed client receive using:

```powershell
cargo run --release -q -p sectorsync-bench --example client_mixed_receive_visit
cargo run --release -q -p sectorsync-bench --example client_mixed_receive_visit -- --owned
```

The default preloaded workload processes 100 replication frames, 20 ACKs, and
one barrier per tick for ten ticks. Each replication frame has 64 entities,
four components per entity, and 64-byte component payloads. Guards cap 500
replication frames, 500 ACKs, 20 barriers per tick, 256 entities, eight
components, 1 KiB component payloads, 20 ticks, and 64 MiB aggregate
replication payload work without `--allow-heavy`; execution has a 10-second
budget.

Five alternating release A/B runs each consumed 1,210 packets: 1,000
replication frames, 200 ACKs, and ten barriers. Both paths visited 64,000
entities, 256,000 components, `16,384,000` payload bytes, and produced a
`46,080,220` checksum with identical bridge statistics. Visitor pumping
materialized zero owned replication frames; mixed owned pumping materialized
1,000. Median tick p99 was 1.206 ms visitor versus 3.917 ms owned, about a
69.2% reduction on this development host. Exact mixed counts, checksum,
statistics, visitor-error propagation, zero materializations, guard metadata,
and `benchmark_ok=true` are the portable acceptance signals.

### Gateway ACK Ownership Measurement

`GatewayClientTransportBridge::pump_ingress_compact` shares packet decoding,
source validation, gateway admission, queueing, ACK encoding, transport send,
and cumulative statistics with the compatible full pump. The compact path moves
each ACK Vec into transport and returns fixed-size counts; full pumping clones
the ACK for transport while retaining the original in each detailed report.

Compare compact ownership transfer with retained reports using:

```powershell
cargo run --release -q -p sectorsync-bench --example gateway_ack_ownership
cargo run --release -q -p sectorsync-bench --example gateway_ack_ownership -- --retain-reports
```

The default preloaded workload processes 2,000 commands per tick with 64-byte
payloads for ten ticks. Guards cap 4,000 commands per tick, 4 KiB payloads, 20
ticks, and 64 MiB aggregate command payload work without `--allow-heavy`;
execution has a 10-second budget. The station command queue and gateway rate
limit are explicitly bounded to the guarded command count.

Five alternating release A/B runs each accepted and queued 20,000 commands,
sent 20,000 ACKs totaling `560,000` bytes, and produced identical bridge,
pipeline, transport, and checksum fields. Compact pumping retained zero reports
and zero ACK payloads; full pumping retained 20,000 of each. Median tick p99 was
0.874 ms compact versus 1.722 ms full, about a 49% reduction on this development
host. Identical admission/queue/send results and removal of retained ACK clones
are the portable acceptance signals; detailed-report retention remains an
explicit compatibility option.

### Component Entity Cleanup Measurement

`ComponentStore::remove_entity_into` clears and reuses caller-owned removed
value storage. The compatible `remove_entity` path returns a fresh Vec, while
`clear_entity` discards removed values for teardown paths that only need the
component count.

Compare reusable and fresh owned output using:

```powershell
cargo run --release -q -p sectorsync-bench --example component_remove_reuse
cargo run --release -q -p sectorsync-bench --example component_remove_reuse -- --fresh-output
cargo run --release -q -p sectorsync-bench --example component_remove_reuse -- --discard
```

The default preloaded workload removes eight 32-byte components from 1,000
entities per tick for ten ticks. Guards cap 2,000 entities per tick, 32
components, 4 KiB payloads, 20 ticks, and 64 MiB aggregate payload work without
`--allow-heavy`; execution has a 10-second budget.

Five alternating release A/B runs each removed 10,000 entities and 80,000
component blobs totaling `2,560,000` bytes with identical checksums. Reusable
output created zero fresh result Vecs and retained capacity for eight entries;
the compatible path created 10,000 fresh results. Median tick p99 was 1.881 ms
reusable versus 2.031 ms fresh, about a 7.4% reduction on this development
host. A discard run reported 1.741 ms p99. Identical removal counts, bytes, and
checksums plus zero fresh outputs are the portable acceptance signals; host
timings are directional rather than a universal guarantee.

### Cell Migration Storage Measurement

`CellMigrationExecutor::migrate_cells` now scans `handles_in_cell_slice`
directly, avoiding one temporary handle Vec per scanned cell. Repeated passes
can additionally call `migrate_cells_into` with caller-owned
`CellMigrationScratch` and `CellMigrationReport` storage; the compatible owned
API constructs those buffers for occasional migrations.

Compare retained and fresh migration storage using:

```powershell
cargo run --release -q -p sectorsync-bench --example cell_migration_reuse
cargo run --release -q -p sectorsync-bench --example cell_migration_reuse -- --fresh-storage
```

The default preloaded workload migrates 500 point entities from each of ten
distinct cells. Station and index capacity are reserved before timing. Guards
cap 2,000 entities per cell, 20 ticks, and 20,000 aggregate entities without
`--allow-heavy`; execution has a 10-second budget.

Five alternating release A/B runs each migrated 5,000 entities with a
`12,502,500` entity-id checksum and identical final target-index counts.
Reusable migration performed zero fresh-storage passes; the compatible path
performed ten. Median tick p99 was 0.531 ms reusable versus 0.563 ms fresh,
about a 5.7% reduction on this development host. Identical migration counts,
checksums, final index membership, zero fresh-storage passes, and retained
scratch/report capacities are the portable acceptance signals; host timings
are directional rather than a universal guarantee.

### Multi-Room Split Execution Storage Measurement

`SplitScheduler::execute_into` retains outer ownership and migration report
slots, moved-cell and nested entity-migration capacity, and one shared
`CellMigrationScratch` across actions and rooms. The fresh benchmark branch
constructs new scratch inside the unpublished benchmark crate; there is no
owned compatibility execution method in the public runtime API.

Compare retained and fresh execution storage using:

```powershell
cargo run --release -q -p sectorsync-bench --example split_execution_reuse
cargo run --release -q -p sectorsync-bench --example split_execution_reuse -- --fresh-storage
```

The default preloaded workload executes four one-cell split actions in each of
ten independent rooms, with 128 entities per action. Stations, indexes,
schedules, and ownership are prepared before timing. Guards cap 20 rooms,
eight actions per room, 512 entities per action, and 20,000 aggregate entities
without `--allow-heavy`; execution has a 10-second budget.

Five alternating release A/B runs each executed 40 actions, applied 40
ownership updates, and migrated 5,120 entities with a `13,109,760` entity-id
checksum and identical target-index totals. Reusable execution created zero
fresh execution reports; the benchmark's fresh-storage branch created ten.
Median room p99 was
0.361 ms reusable versus 0.528 ms fresh, about a 31.6% reduction on this
development host. Identical action/update/entity counts, checksums, target
indexes, zero fresh reports, and retained nested capacities are the portable
acceptance signals; host timings are directional rather than universal.

### Multi-Room Barrier Snapshot Storage Measurement

`Station::snapshot_into` reuses one snapshot's entity Vec, while
`BarrierController::export_snapshots_into` retains Station snapshot slots and
nested entity capacity in caller-owned `BarrierSnapshotScratch`. Compatible
owned snapshot APIs remain available for transfer or long-lived storage.

Compare retained and fresh frozen snapshot batches using:

```powershell
cargo run --release -q -p sectorsync-bench --example barrier_snapshot_reuse
cargo run --release -q -p sectorsync-bench --example barrier_snapshot_reuse -- --fresh-storage
```

The default preloaded workload freezes 50 rooms with 256 entities each and
exports ten snapshot batches. World construction and barrier alignment occur
outside timing. Guards cap 100 rooms, 1,000 entities per room, 20 exports, and
1,000,000 aggregate entity copies without `--allow-heavy`; execution has a
10-second budget.

Five alternating release A/B runs each exported 500 Station snapshots and
128,000 entity records with an `819,264,000` entity-id checksum and matching
barrier metrics. Reusable export created zero fresh batches; compatible export
created ten. Median export p99 was 0.259 ms reusable versus 0.690 ms fresh,
about a 62.5% reduction on this development host. Identical snapshot/entity
counts, checksums, barrier metrics, zero fresh batches, and retained nested
capacity are the portable acceptance signals; host timings are directional
rather than universal.

### Multi-Room Station Restore Capacity Measurement

`Station::restore` constructs record, generation, and entity-id lookup storage
with the snapshot entity count before inserting records. `restore_tracked`
returns the same Station plus `StationRestoreStats` so integrations and the
benchmark can verify initial/final capacities and growth behavior directly.

Run the guarded restore measurement using:

```powershell
cargo run --release -q -p sectorsync-bench --example station_restore_capacity
```

The default preloaded workload restores 20 rooms with 512 entities each for
ten batches. All consumed snapshot copies are built before timing. Guards cap
100 rooms, 2,000 entities per room, 20 ticks, and 500,000 aggregate entity
restores without `--allow-heavy`; execution has a 10-second budget.

Five release runs each restored 200 Stations and 102,400 entities. Preallocated
restore reported zero initial record/index capacity shortfalls and zero
record/index growth events. A temporary local comparison using the previous
zero-capacity constructor reported 200 shortfalls and 200 growth events in both
storage classes. Median tick p99 was effectively neutral at 1.031 ms
preallocated versus 1.026 ms previous, so no latency improvement is claimed.
Zero growth, exact Station/entity counts, checksum stability, capacity totals,
guard metadata, and `benchmark_ok=true` are the portable acceptance signals.

## Heavy Calibration

These runs are manual, explicitly admitted, and unsuitable for routine CI.

### Optional Heavy Calibration

Medium, large, and manual scales never run implicitly. They require explicit
opt-in:

```bash
cargo run -q -p sectorsync-bench -- --profile=medium --allow-heavy
cargo run -q -p sectorsync-bench -- --profile=large --allow-heavy
cargo run -q -p sectorsync-bench -- --entities=100000 --clients=2000 \
  --stations=16 --ticks=20 --allow-heavy
```

Without `--allow-heavy`, medium/large requests remain smoke-sized and print
`heavy_profile_denied=true`; oversized manual values are clamped and print
`default_resource_guard_applied=true`.

Do not promote heavy-run numbers into committed defaults from a single
development host. Record hardware, profile, baseline, full output, and repeated
run variance before changing thresholds.

### Hotspot Calibration

Use the deterministic examples before any heavier threshold experiment:

```bash
cargo run -p sectorsync-bench --example split_tuning
cargo run -p sectorsync-bench --example split_migration
cargo run -p sectorsync-bench --example load_scheduler
```

`split_tuning` must report Normal/Warm/Hot classification and positive coverage
for cooldown, target-capacity, and insufficient-improvement guards. It also
reports proposed source/target pressure before/after and proposed cell/entity
counts. `split_migration` reports the corresponding before/after pressure plus
actual migrated cell/entity counts from execution.

Use `warm_stations`, `hotspot_stations`, `split_candidate_cells`, and scheduler
decision fields from the guarded runner when calibrating larger synthetic
profiles. Production thresholds must be chosen from caller telemetry and host
budgets; SectorSync does not auto-tune them or collect production telemetry.
