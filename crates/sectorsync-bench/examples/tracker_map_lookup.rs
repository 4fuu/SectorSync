//! Guarded A/B benchmark for replication tracker record maps.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::hash::Hash;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    ClientId, EntityHandle, ReplicationTrackKey, ReplicationTrackRecord, Tick,
};

const DEFAULT_RECORDS: usize = 4_096;
const DEFAULT_OPERATIONS_PER_TICK: usize = 100_000;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_RECORDS: usize = 65_536;
const GUARD_MAX_OPERATIONS_PER_TICK: usize = 200_000;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_OPERATIONS: usize = 2_000_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug)]
struct Config {
    records: usize,
    operations_per_tick: usize,
    ticks: usize,
    hash_map: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

trait RecordMap<K> {
    fn insert_record(&mut self, key: K, value: ReplicationTrackRecord);
    fn record(&self, key: &K) -> Option<&ReplicationTrackRecord>;
    fn record_mut(&mut self, key: &K) -> Option<&mut ReplicationTrackRecord>;
    fn len(&self) -> usize;
}

impl<K: Ord> RecordMap<K> for BTreeMap<K, ReplicationTrackRecord> {
    fn insert_record(&mut self, key: K, value: ReplicationTrackRecord) {
        self.insert(key, value);
    }

    fn record(&self, key: &K) -> Option<&ReplicationTrackRecord> {
        self.get(key)
    }

    fn record_mut(&mut self, key: &K) -> Option<&mut ReplicationTrackRecord> {
        self.get_mut(key)
    }

    fn len(&self) -> usize {
        BTreeMap::len(self)
    }
}

impl<K: Eq + Hash> RecordMap<K> for HashMap<K, ReplicationTrackRecord> {
    fn insert_record(&mut self, key: K, value: ReplicationTrackRecord) {
        self.insert(key, value);
    }

    fn record(&self, key: &K) -> Option<&ReplicationTrackRecord> {
        self.get(key)
    }

    fn record_mut(&mut self, key: &K) -> Option<&mut ReplicationTrackRecord> {
        self.get_mut(key)
    }

    fn len(&self) -> usize {
        HashMap::len(self)
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    operations: usize,
    acknowledgements: usize,
    final_records: usize,
    checksum: u64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    workload_ok: bool,
    record_count_ok: bool,
    time_budget_exhausted: bool,
    benchmark_ok: bool,
}

fn main() {
    let config = parse_config();
    let result = if config.hash_map {
        run(config, HashMap::new())
    } else {
        run(config, BTreeMap::new())
    };
    print_result(config, &result);
    if !result.benchmark_ok {
        std::process::exit(1);
    }
}

fn run<M: RecordMap<ReplicationTrackKey>>(config: Config, mut records: M) -> ResultSummary {
    for index in 0..config.records {
        let key = key(index);
        records.insert_record(
            key,
            ReplicationTrackRecord {
                client_id: key.client_id,
                entity: key.entity,
                last_sent: Tick::new(1),
                last_acked: None,
            },
        );
    }
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut operations = 0_usize;
    let mut acknowledgements = 0_usize;
    let mut checksum = 0_u64;
    let mut time_budget_exhausted = false;
    for tick in 0..config.ticks {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        for operation in 0..config.operations_per_tick {
            let index = operation
                .saturating_mul(17)
                .saturating_add(tick.saturating_mul(31))
                % config.records;
            let key = key(index);
            if operation % 8 == 0 {
                let record = records
                    .record_mut(&key)
                    .expect("inserted tracker record should exist");
                let acked_at = Tick::new(
                    u64::try_from(tick.saturating_add(2)).expect("guarded tick fits u64"),
                );
                record.last_acked = Some(acked_at);
                checksum = checksum.wrapping_add(acked_at.get());
                acknowledgements = acknowledgements.saturating_add(1);
            } else {
                let record = records
                    .record(&key)
                    .expect("inserted tracker record should exist");
                checksum = checksum
                    .wrapping_add(record.last_sent.get())
                    .wrapping_add(record.last_acked.map_or(0, Tick::get));
            }
            operations = operations.saturating_add(1);
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        ticks_completed = ticks_completed.saturating_add(1);
    }
    black_box(checksum);
    latencies.sort_by(f64::total_cmp);
    let expected_operations = config.operations_per_tick.saturating_mul(config.ticks);
    let expected_acks = config
        .operations_per_tick
        .div_ceil(8)
        .saturating_mul(config.ticks);
    let workload_ok = ticks_completed == config.ticks
        && operations == expected_operations
        && acknowledgements == expected_acks;
    let record_count_ok = records.len() == config.records;
    let benchmark_ok = workload_ok && record_count_ok && !time_budget_exhausted;
    ResultSummary {
        ticks_completed,
        operations,
        acknowledgements,
        final_records: records.len(),
        checksum,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        workload_ok,
        record_count_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn key(index: usize) -> ReplicationTrackKey {
    ReplicationTrackKey {
        client_id: ClientId::new(u64::try_from(index % 256).expect("bounded client id fits u64")),
        entity: EntityHandle::new(
            u32::try_from(index).expect("guarded entity index fits u32"),
            1,
        ),
    }
}

fn parse_config() -> Config {
    let mut records = DEFAULT_RECORDS;
    let mut operations_per_tick = DEFAULT_OPERATIONS_PER_TICK;
    let mut ticks = DEFAULT_TICKS;
    let mut hash_map = true;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--records=") {
            records = value.parse().unwrap_or(records);
        } else if let Some(value) = arg.strip_prefix("--operations-per-tick=") {
            operations_per_tick = value.parse().unwrap_or(operations_per_tick);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--btree" {
            hash_map = false;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (records, operations_per_tick, ticks);
    records = records.max(1);
    operations_per_tick = operations_per_tick.max(1);
    ticks = ticks.max(1);
    if !allow_heavy {
        records = records.min(GUARD_MAX_RECORDS);
        operations_per_tick = operations_per_tick.min(GUARD_MAX_OPERATIONS_PER_TICK);
        ticks = ticks.min(GUARD_MAX_TICKS);
        if operations_per_tick.saturating_mul(ticks) > GUARD_MAX_OPERATIONS {
            operations_per_tick = (GUARD_MAX_OPERATIONS / ticks).max(1);
        }
    }
    Config {
        records,
        operations_per_tick,
        ticks,
        hash_map,
        allow_heavy,
        guard_applied: requested != (records, operations_per_tick, ticks),
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
    println!("SectorSync replication tracker map benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_records={GUARD_MAX_RECORDS}");
    println!("guard_max_operations_per_tick={GUARD_MAX_OPERATIONS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_operations={GUARD_MAX_OPERATIONS}");
    println!("records={}", config.records);
    println!("operations_per_tick={}", config.operations_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!("hash_map={}", config.hash_map);
    println!("operations={}", result.operations);
    println!("acknowledgements={}", result.acknowledgements);
    println!("final_records={}", result.final_records);
    println!("tracker_checksum={}", result.checksum);
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("threshold_record_count_ok={}", result.record_count_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
