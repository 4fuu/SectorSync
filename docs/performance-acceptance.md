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
`batch_entity_capacity_max` separately:

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

This workload does not include gameplay logic, room creation/destruction churn,
idle-room scheduling, command/event pumps, kernel networking, persistence, or
matchmaking. Treat it as evidence for active-room spatial planning and encoding,
not a complete room-server capacity promise.

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
