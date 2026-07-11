//! Guarded A/B benchmark for replication tracker capacity prechecks.

use std::collections::BTreeMap;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    ClientId, EntityHandle, ReplicationTrackKey, ReplicationTrackRecord, Tick,
};

const DEFAULT_RECORDS: usize = 4_096;
const DEFAULT_PLAN_ENTITIES: usize = 256;
const DEFAULT_CALLS_PER_TICK: usize = 100;
const DEFAULT_TICKS: usize = 10;
const DEFAULT_MAX_ENTRIES: usize = 65_536;
const GUARD_MAX_RECORDS: usize = 20_000;
const GUARD_MAX_PLAN_ENTITIES: usize = 1_000;
const GUARD_MAX_CALLS_PER_TICK: usize = 1_000;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_UPDATES: usize = 2_000_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug)]
struct Config {
    records: usize,
    plan_entities: usize,
    calls_per_tick: usize,
    ticks: usize,
    exact_scan: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    calls: usize,
    updates: usize,
    capacity_probes: usize,
    final_records: usize,
    checksum: u64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    workload_ok: bool,
    capacity_ok: bool,
    time_budget_exhausted: bool,
    benchmark_ok: bool,
}

fn main() {
    let config = parse_config();
    let result = run(config);
    print_result(config, &result);
    if !result.benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> ResultSummary {
    let client_id = ClientId::new(1);
    let mut records = BTreeMap::new();
    for index in 0..config.records {
        let entity = handle(index);
        records.insert(
            ReplicationTrackKey { client_id, entity },
            record(client_id, entity, Tick::new(0)),
        );
    }
    let plan: Vec<_> = (0..config.plan_entities).map(handle).collect();
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut calls = 0_usize;
    let mut updates = 0_usize;
    let mut capacity_probes = 0_usize;
    let mut time_budget_exhausted = false;
    for tick in 0..config.ticks {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        for call in 0..config.calls_per_tick {
            if config.exact_scan {
                let mut needed = 0_usize;
                for entity in &plan {
                    if !records.contains_key(&ReplicationTrackKey {
                        client_id,
                        entity: *entity,
                    }) {
                        needed = needed.saturating_add(1);
                    }
                    capacity_probes = capacity_probes.saturating_add(1);
                }
                assert!(records.len().saturating_add(needed) <= DEFAULT_MAX_ENTRIES);
            } else {
                assert!(records.len().saturating_add(plan.len()) <= DEFAULT_MAX_ENTRIES);
            }
            let sent_at = Tick::new(
                u64::try_from(
                    tick.saturating_mul(config.calls_per_tick)
                        .saturating_add(call),
                )
                .expect("guarded tick fits u64"),
            );
            for entity in &plan {
                records.insert(
                    ReplicationTrackKey {
                        client_id,
                        entity: *entity,
                    },
                    record(client_id, *entity, sent_at),
                );
                updates = updates.saturating_add(1);
            }
            calls = calls.saturating_add(1);
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        ticks_completed = ticks_completed.saturating_add(1);
    }
    let checksum = records.values().fold(0_u64, |checksum, value| {
        checksum
            .wrapping_add(u64::from(value.entity.index()))
            .wrapping_add(value.last_sent.get())
    });
    black_box(checksum);
    latencies.sort_by(f64::total_cmp);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let expected_updates = expected_calls.saturating_mul(config.plan_entities);
    let expected_probes = if config.exact_scan {
        expected_updates
    } else {
        0
    };
    let workload_ok =
        ticks_completed == config.ticks && calls == expected_calls && updates == expected_updates;
    let capacity_ok = records.len() == config.records.max(config.plan_entities)
        && records.len() <= DEFAULT_MAX_ENTRIES
        && capacity_probes == expected_probes;
    let benchmark_ok = workload_ok && capacity_ok && !time_budget_exhausted;
    ResultSummary {
        ticks_completed,
        calls,
        updates,
        capacity_probes,
        final_records: records.len(),
        checksum,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        workload_ok,
        capacity_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn handle(index: usize) -> EntityHandle {
    EntityHandle::new(
        u32::try_from(index).expect("guarded entity index fits u32"),
        1,
    )
}

fn record(client_id: ClientId, entity: EntityHandle, last_sent: Tick) -> ReplicationTrackRecord {
    ReplicationTrackRecord {
        client_id,
        entity,
        last_sent,
        last_acked: None,
    }
}

fn parse_config() -> Config {
    let mut records = DEFAULT_RECORDS;
    let mut plan_entities = DEFAULT_PLAN_ENTITIES;
    let mut calls_per_tick = DEFAULT_CALLS_PER_TICK;
    let mut ticks = DEFAULT_TICKS;
    let mut exact_scan = false;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--records=") {
            records = value.parse().unwrap_or(records);
        } else if let Some(value) = arg.strip_prefix("--plan-entities=") {
            plan_entities = value.parse().unwrap_or(plan_entities);
        } else if let Some(value) = arg.strip_prefix("--calls-per-tick=") {
            calls_per_tick = value.parse().unwrap_or(calls_per_tick);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--exact-scan" {
            exact_scan = true;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (records, plan_entities, calls_per_tick, ticks);
    records = records.max(1);
    plan_entities = plan_entities.max(1);
    calls_per_tick = calls_per_tick.max(1);
    ticks = ticks.max(1);
    if !allow_heavy {
        records = records.min(GUARD_MAX_RECORDS);
        plan_entities = plan_entities.min(GUARD_MAX_PLAN_ENTITIES);
        calls_per_tick = calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
        ticks = ticks.min(GUARD_MAX_TICKS);
        let calls = calls_per_tick.saturating_mul(ticks);
        if calls.saturating_mul(plan_entities) > GUARD_MAX_UPDATES {
            calls_per_tick = (GUARD_MAX_UPDATES / ticks / plan_entities).max(1);
        }
    }
    Config {
        records,
        plan_entities,
        calls_per_tick,
        ticks,
        exact_scan,
        allow_heavy,
        guard_applied: requested != (records, plan_entities, calls_per_tick, ticks),
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * quantile).ceil() as usize;
    values[index.min(values.len() - 1)]
}

fn print_result(config: Config, result: &ResultSummary) {
    println!("SectorSync replication tracker capacity guard benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_records={GUARD_MAX_RECORDS}");
    println!("guard_max_plan_entities={GUARD_MAX_PLAN_ENTITIES}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_updates={GUARD_MAX_UPDATES}");
    println!("records={}", config.records);
    println!("plan_entities={}", config.plan_entities);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!("exact_scan={}", config.exact_scan);
    println!("calls={}", result.calls);
    println!("updates={}", result.updates);
    println!("capacity_probes={}", result.capacity_probes);
    println!("final_records={}", result.final_records);
    println!("tracker_checksum={}", result.checksum);
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("threshold_capacity_ok={}", result.capacity_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
