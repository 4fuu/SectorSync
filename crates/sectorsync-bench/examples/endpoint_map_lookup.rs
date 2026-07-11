//! Guarded A/B benchmark for in-memory transport endpoint maps.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::hash::Hash;
use std::hint::black_box;
use std::time::{Duration, Instant};

const DEFAULT_ENDPOINTS: usize = 4_096;
const DEFAULT_LOOKUPS_PER_TICK: usize = 100_000;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_ENDPOINTS: usize = 20_000;
const GUARD_MAX_LOOKUPS_PER_TICK: usize = 200_000;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_LOOKUPS: usize = 2_000_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug)]
struct Config {
    endpoints: usize,
    lookups_per_tick: usize,
    ticks: usize,
    hash_map: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

trait EndpointMap<K> {
    fn insert_endpoint(&mut self, key: K, value: usize);
    fn endpoint(&self, key: &K) -> Option<&usize>;
    fn endpoint_mut(&mut self, key: &K) -> Option<&mut usize>;
}

impl<K: Ord> EndpointMap<K> for BTreeMap<K, usize> {
    fn insert_endpoint(&mut self, key: K, value: usize) {
        self.insert(key, value);
    }

    fn endpoint(&self, key: &K) -> Option<&usize> {
        self.get(key)
    }

    fn endpoint_mut(&mut self, key: &K) -> Option<&mut usize> {
        self.get_mut(key)
    }
}

impl<K: Eq + Hash> EndpointMap<K> for HashMap<K, usize> {
    fn insert_endpoint(&mut self, key: K, value: usize) {
        self.insert(key, value);
    }

    fn endpoint(&self, key: &K) -> Option<&usize> {
        self.get(key)
    }

    fn endpoint_mut(&mut self, key: &K) -> Option<&mut usize> {
        self.get_mut(key)
    }
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

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    lookups: usize,
    mutations: usize,
    checksum: usize,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    workload_ok: bool,
    checksum_ok: bool,
    time_budget_exhausted: bool,
    benchmark_ok: bool,
}

fn run<M: EndpointMap<usize>>(config: Config, mut map: M) -> ResultSummary {
    for endpoint in 0..config.endpoints {
        map.insert_endpoint(endpoint, endpoint);
    }
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut lookups = 0_usize;
    let mut mutations = 0_usize;
    let mut checksum = 0_usize;
    let mut time_budget_exhausted = false;
    for tick in 0..config.ticks {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        for operation in 0..config.lookups_per_tick {
            let key = operation
                .saturating_mul(17)
                .saturating_add(tick.saturating_mul(31))
                % config.endpoints;
            if operation % 8 == 0 {
                let value = map
                    .endpoint_mut(&key)
                    .expect("inserted endpoint should exist");
                *value = value.wrapping_add(1);
                checksum = checksum.wrapping_add(*value);
                mutations = mutations.saturating_add(1);
            } else {
                checksum = checksum
                    .wrapping_add(*map.endpoint(&key).expect("inserted endpoint should exist"));
            }
            lookups = lookups.saturating_add(1);
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        ticks_completed = ticks_completed.saturating_add(1);
    }
    black_box(checksum);
    latencies.sort_by(f64::total_cmp);
    let expected_lookups = config.lookups_per_tick.saturating_mul(config.ticks);
    let workload_ok = ticks_completed == config.ticks && lookups == expected_lookups;
    let expected_mutations = config
        .lookups_per_tick
        .div_ceil(8)
        .saturating_mul(config.ticks);
    let checksum_ok = checksum != 0 && mutations == expected_mutations;
    let benchmark_ok = workload_ok && checksum_ok && !time_budget_exhausted;
    ResultSummary {
        ticks_completed,
        lookups,
        mutations,
        checksum,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        workload_ok,
        checksum_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn parse_config() -> Config {
    let mut endpoints = DEFAULT_ENDPOINTS;
    let mut lookups_per_tick = DEFAULT_LOOKUPS_PER_TICK;
    let mut ticks = DEFAULT_TICKS;
    let mut hash_map = true;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--endpoints=") {
            endpoints = value.parse().unwrap_or(endpoints);
        } else if let Some(value) = arg.strip_prefix("--lookups-per-tick=") {
            lookups_per_tick = value.parse().unwrap_or(lookups_per_tick);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--btree" {
            hash_map = false;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (endpoints, lookups_per_tick, ticks);
    endpoints = endpoints.max(1);
    lookups_per_tick = lookups_per_tick.max(1);
    ticks = ticks.max(1);
    if !allow_heavy {
        endpoints = endpoints.min(GUARD_MAX_ENDPOINTS);
        lookups_per_tick = lookups_per_tick.min(GUARD_MAX_LOOKUPS_PER_TICK);
        ticks = ticks.min(GUARD_MAX_TICKS);
        if lookups_per_tick.saturating_mul(ticks) > GUARD_MAX_LOOKUPS {
            lookups_per_tick = (GUARD_MAX_LOOKUPS / ticks).max(1);
        }
    }
    Config {
        endpoints,
        lookups_per_tick,
        ticks,
        hash_map,
        allow_heavy,
        guard_applied: requested != (endpoints, lookups_per_tick, ticks),
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
    println!("SectorSync endpoint map lookup benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_endpoints={GUARD_MAX_ENDPOINTS}");
    println!("guard_max_lookups_per_tick={GUARD_MAX_LOOKUPS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_lookups={GUARD_MAX_LOOKUPS}");
    println!("endpoints={}", config.endpoints);
    println!("lookups_per_tick={}", config.lookups_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!("hash_map={}", config.hash_map);
    println!("lookups={}", result.lookups);
    println!("mutations={}", result.mutations);
    println!("lookup_checksum={}", result.checksum);
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("threshold_checksum_ok={}", result.checksum_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
