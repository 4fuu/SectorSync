# SectorSync Performance Acceptance Matrix

This document maps SectorSync performance claims to reproducible commands,
machine-readable output fields, and default pass/fail gates. It is a development
acceptance matrix, not a production capacity promise for arbitrary hardware.

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
| Replication selection | `replication_scratch_queries`, grid/occupied strategy query counts, probed/scanned/matched cell counts, `replication_scratch_candidates`, `replication_candidates_selected` |
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

## Guarded Local Host Measurement

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

## Optional SIMD, Parallel, and 128 Hz Modes

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

## Many-Room Single-Thread Measurement

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

## Multi-Cell Bounds Update Measurement

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
real boundary crossings still rebuild persistent membership as required.

## Single-Viewer Plan Output Measurement

`ReplicationTransportBridge` retains one `ReplicationPlan` output across
normal, cadence, priority, and priority/cadence sends. The guarded
`single_viewer_planning` benchmark isolates that output from the already reused
spatial scratch. `--fresh-plan-output` recreates the previous owned-plan path:

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

## Budgeted Priority Top-K Measurement

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

## Parallel Multi-Station Output Measurement

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
latency percentiles, path/workload verdicts, and `benchmark_ok`.

Five alternating release A/B runs produced the same `2,764,800` selected
checksum. The reusable path created zero fresh results and retained 12 Station
slots with total selected-entity capacity of 6,912; the owned path created 400
fresh results per run. On this development host, median tick p99 was 1.986 ms
with retained output versus 3.363 ms with fresh output, about a 41% reduction.
Runtime tests also compare every active Station plan and aggregate statistic,
verify capacity retention after a smaller subsequent batch, and cover empty
input.

## Event Drain Output Measurement

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

## Station Schedule Scratch Measurement

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

## Hotspot Split Scratch Measurement

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

## Split Schedule Nested Output Measurement

`SplitSchedulerScratch` retains fixed decision and action slots while exposing
only active prefixes through `SplitScheduleView`. Each decision retains its
reason capacity, each action retains its proposal coordinate capacity, and the
same storage owns hotspot cell candidates and the pending proposal. Borrowed
views can be executed or recorded into cooldown state without materializing an
owned schedule:

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

## Gateway Expiry Scan Measurement

`GatewaySessionTable::expire_disconnected` now uses one `BTreeMap::retain` pass.
Connected sessions and disconnected sessions at or inside the grace boundary
remain; stale sessions are removed in place and counted from the map length
delta. The isolated benchmark compares this production algorithm shape with the
previous collect-client-ids then remove-each pattern:

```powershell
cargo run --release -q -p sectorsync-bench --example gateway_expiry_scan
cargo run --release -q -p sectorsync-bench --example gateway_expiry_scan -- --collect-remove
```

Map snapshot cloning occurs outside the measured operation interval. The
default workload scans 5,000 sessions 500 times with connected, grace-boundary,
and expired records. Guards cap 20,000 sessions, 100 calls per tick, and 20
ticks without `--allow-heavy`; total execution has a 10-second budget. Output
includes expired/remaining conservation counts, temporary-id collection count,
operation p50/p95/p99/max, guard metadata, path/workload verdicts, and
`benchmark_ok`.

Five alternating release A/B runs expired and retained `1,250,000` records each.
The retain path created zero temporary id collections; the comparison created
500 per run. Median operation p99 was 0.080 ms for retain versus 0.241 ms for
collect/remove, about a 67% reduction on this development host. Core tests cover
connected sessions, the exact grace boundary, stale removal, repeated expiry,
and cumulative statistics; the benchmark test compares final maps exactly.

## Deployment Stale-Node Scan Measurement

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

## Optional Heavy Calibration

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

## Hotspot Calibration

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
