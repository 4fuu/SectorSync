//! Guarded A/B benchmark for bounded duplicate-suppression indexes.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::env;
use std::hash::Hash;
use std::hint::black_box;
use std::time::{Duration, Instant};

const DEFAULT_WINDOW: usize = 4_096;
const DEFAULT_OPERATIONS_PER_TICK: usize = 100_000;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_WINDOW: usize = 50_000;
const GUARD_MAX_OPERATIONS_PER_TICK: usize = 200_000;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_OPERATIONS: usize = 2_000_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug)]
struct Config {
    window: usize,
    operations_per_tick: usize,
    ticks: usize,
    hash_index: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    operations: usize,
    accepted: usize,
    duplicates: usize,
    retained: usize,
    checksum: u64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    conservation_ok: bool,
    workload_ok: bool,
    time_budget_exhausted: bool,
    benchmark_ok: bool,
}

trait SetIndex<K> {
    fn contains_key(&self, key: &K) -> bool;
    fn insert_key(&mut self, key: K);
    fn remove_key(&mut self, key: &K);
    fn retained_len(&self) -> usize;
}

impl<K: Ord> SetIndex<K> for BTreeSet<K> {
    fn contains_key(&self, key: &K) -> bool {
        self.contains(key)
    }

    fn insert_key(&mut self, key: K) {
        self.insert(key);
    }

    fn remove_key(&mut self, key: &K) {
        self.remove(key);
    }

    fn retained_len(&self) -> usize {
        self.len()
    }
}

impl<K: Eq + Hash> SetIndex<K> for HashSet<K> {
    fn contains_key(&self, key: &K) -> bool {
        self.contains(key)
    }

    fn insert_key(&mut self, key: K) {
        self.insert(key);
    }

    fn remove_key(&mut self, key: &K) {
        self.remove(key);
    }

    fn retained_len(&self) -> usize {
        self.len()
    }
}

fn main() {
    let config = parse_config();
    let result = if config.hash_index {
        run(config, HashSet::new())
    } else {
        run(config, BTreeSet::new())
    };
    print_result(config, &result);
    if !result.benchmark_ok {
        std::process::exit(1);
    }
}

fn run<I>(config: Config, mut index: I) -> ResultSummary
where
    I: SetIndex<(u32, u64)>,
{
    let mut order = VecDeque::with_capacity(config.window.saturating_add(1));
    for nonce in 0..config.window {
        let key = (1, u64::try_from(nonce).expect("guarded nonce fits u64"));
        index.insert_key(key);
        order.push_back(key);
    }
    let mut next_nonce = u64::try_from(config.window).expect("guarded window fits u64");
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut operations = 0_usize;
    let mut accepted = 0_usize;
    let mut duplicates = 0_usize;
    let mut checksum = 0_u64;
    let mut time_budget_exhausted = false;

    for _ in 0..config.ticks {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        for operation in 0..config.operations_per_tick {
            let key = if operation % 4 == 0 {
                let key = (1, next_nonce);
                next_nonce = next_nonce.saturating_add(1);
                key
            } else {
                let offset =
                    u64::try_from(operation % config.window).expect("guarded offset fits u64");
                (1, next_nonce.saturating_sub(1).saturating_sub(offset))
            };
            if index.contains_key(&key) {
                duplicates = duplicates.saturating_add(1);
                checksum = checksum.saturating_add(key.1);
            } else {
                index.insert_key(key);
                order.push_back(key);
                accepted = accepted.saturating_add(1);
                checksum = checksum.saturating_add(key.1).saturating_add(1);
                while order.len() > config.window {
                    if let Some(old) = order.pop_front() {
                        index.remove_key(&old);
                    }
                }
            }
            operations = operations.saturating_add(1);
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        ticks_completed = ticks_completed.saturating_add(1);
    }

    black_box(checksum);
    latencies.sort_by(f64::total_cmp);
    let retained = index.retained_len();
    let conservation_ok = operations == accepted.saturating_add(duplicates)
        && retained == config.window
        && retained == order.len();
    let workload_ok = ticks_completed == config.ticks
        && operations == config.operations_per_tick.saturating_mul(config.ticks);
    let benchmark_ok = conservation_ok && workload_ok && !time_budget_exhausted;

    ResultSummary {
        ticks_completed,
        operations,
        accepted,
        duplicates,
        retained,
        checksum,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        conservation_ok,
        workload_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn parse_config() -> Config {
    let mut window = DEFAULT_WINDOW;
    let mut operations_per_tick = DEFAULT_OPERATIONS_PER_TICK;
    let mut ticks = DEFAULT_TICKS;
    let mut hash_index = true;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--window=") {
            window = value.parse().unwrap_or(window);
        } else if let Some(value) = arg.strip_prefix("--operations-per-tick=") {
            operations_per_tick = value.parse().unwrap_or(operations_per_tick);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--btree" {
            hash_index = false;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    window = window.max(1);
    operations_per_tick = operations_per_tick.max(1);
    ticks = ticks.max(1);
    let requested = (window, operations_per_tick, ticks);
    if !allow_heavy {
        window = window.min(GUARD_MAX_WINDOW);
        operations_per_tick = operations_per_tick.min(GUARD_MAX_OPERATIONS_PER_TICK);
        ticks = ticks.min(GUARD_MAX_TICKS);
        if operations_per_tick.saturating_mul(ticks) > GUARD_MAX_OPERATIONS {
            operations_per_tick = (GUARD_MAX_OPERATIONS / ticks).max(1);
        }
    }
    Config {
        window,
        operations_per_tick,
        ticks,
        hash_index,
        allow_heavy,
        guard_applied: requested != (window, operations_per_tick, ticks),
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
    println!("SectorSync bounded dedup index benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_window={GUARD_MAX_WINDOW}");
    println!("guard_max_operations_per_tick={GUARD_MAX_OPERATIONS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_operations={GUARD_MAX_OPERATIONS}");
    println!("window={}", config.window);
    println!("operations_per_tick={}", config.operations_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!("hash_index={}", config.hash_index);
    println!("operations={}", result.operations);
    println!("accepted={}", result.accepted);
    println!("duplicates={}", result.duplicates);
    println!("retained={}", result.retained);
    println!("dedup_checksum={}", result.checksum);
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_conservation_ok={}", result.conservation_ok);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
