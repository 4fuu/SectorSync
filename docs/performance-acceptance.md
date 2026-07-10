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
| Workload/guard | `requested_profile`, `profile`, `allow_heavy`, `heavy_profile_denied`, `default_resource_guard_applied`, guard limits, entity/client/station/tick counts |
| Tick latency | `tick_ms_p50`, `tick_ms_p95`, `tick_ms_p99`, `tick_ms_max`, `elapsed_ms` |
| Replication selection | `replication_scratch_queries`, `replication_scratch_candidates`, `replication_candidates_selected` |
| Encoded payload | `encoded_packets`, `encoded_bytes`, `payload_entity_deltas`, `payload_component_deltas`, `estimated_payload_bytes` |
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

Compare at least selected candidates, estimated payload bytes, encoded bytes,
and tick percentiles. The benchmark intentionally materializes at most 16 sample
entity deltas per frame to keep routine runs bounded, so
`estimated_payload_bytes` is the comparable logical bandwidth estimate while
`encoded_bytes` measures the bounded wire-codec workload actually executed.

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
