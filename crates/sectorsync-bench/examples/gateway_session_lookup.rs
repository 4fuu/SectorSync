//! Guarded A/B benchmark for Gateway session map lookup and admission updates.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::hash::Hash;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::ClientId;

const DEFAULT_SESSIONS: usize = 4_096;
const DEFAULT_OPERATIONS_PER_TICK: usize = 100_000;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_SESSIONS: usize = 65_536;
const GUARD_MAX_OPERATIONS_PER_TICK: usize = 200_000;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_OPERATIONS: usize = 2_000_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug)]
struct Config {
    sessions: usize,
    operations_per_tick: usize,
    ticks: usize,
    hash_map: bool,
    double_lookup: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

#[derive(Clone, Copy, Debug)]
struct SessionRecord {
    station: u32,
    generation: u64,
    route_epoch: u64,
    last_sequence: u64,
    command_tick: u64,
    commands_this_tick: usize,
    last_seen: u64,
}

trait SessionMap<K> {
    fn insert_session(&mut self, key: K, value: SessionRecord);
    fn session(&self, key: &K) -> Option<&SessionRecord>;
    fn session_mut(&mut self, key: &K) -> Option<&mut SessionRecord>;
}

impl<K: Ord> SessionMap<K> for BTreeMap<K, SessionRecord> {
    fn insert_session(&mut self, key: K, value: SessionRecord) {
        self.insert(key, value);
    }

    fn session(&self, key: &K) -> Option<&SessionRecord> {
        self.get(key)
    }

    fn session_mut(&mut self, key: &K) -> Option<&mut SessionRecord> {
        self.get_mut(key)
    }
}

impl<K: Eq + Hash> SessionMap<K> for HashMap<K, SessionRecord> {
    fn insert_session(&mut self, key: K, value: SessionRecord) {
        self.insert(key, value);
    }

    fn session(&self, key: &K) -> Option<&SessionRecord> {
        self.get(key)
    }

    fn session_mut(&mut self, key: &K) -> Option<&mut SessionRecord> {
        self.get_mut(key)
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    operations: usize,
    map_probes: usize,
    admissions: usize,
    checksum: u64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    workload_ok: bool,
    admission_count_ok: bool,
    probe_count_ok: bool,
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

#[allow(clippy::too_many_lines)]
fn run<M: SessionMap<ClientId>>(config: Config, mut sessions: M) -> ResultSummary {
    for index in 0..config.sessions {
        let numeric = u64::try_from(index).expect("guarded session id fits u64");
        sessions.insert_session(
            ClientId::new(numeric),
            SessionRecord {
                station: u32::try_from(index % 256).expect("bounded station fits u32"),
                generation: 1,
                route_epoch: 1,
                last_sequence: 0,
                command_tick: 0,
                commands_this_tick: 0,
                last_seen: 0,
            },
        );
    }
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut operations = 0_usize;
    let mut map_probes = 0_usize;
    let mut admissions = 0_usize;
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
                % config.sessions;
            let client_id =
                ClientId::new(u64::try_from(index).expect("guarded session id fits u64"));
            if config.double_lookup {
                black_box(
                    sessions
                        .session(&client_id)
                        .expect("inserted session should exist"),
                );
                map_probes = map_probes.saturating_add(1);
            }
            if operation % 8 == 0 {
                let session = sessions
                    .session_mut(&client_id)
                    .expect("inserted session should exist");
                if session.command_tick != u64::try_from(tick).expect("guarded tick fits u64") {
                    session.command_tick = u64::try_from(tick).expect("guarded tick fits u64");
                    session.commands_this_tick = 0;
                }
                session.last_sequence = session.last_sequence.saturating_add(1);
                session.commands_this_tick = session.commands_this_tick.saturating_add(1);
                session.last_seen = session.last_seen.saturating_add(1);
                checksum = checksum
                    .wrapping_add(session.last_sequence)
                    .wrapping_add(session.last_seen);
                admissions = admissions.saturating_add(1);
            } else {
                let session = sessions
                    .session(&client_id)
                    .expect("inserted session should exist");
                checksum = checksum
                    .wrapping_add(u64::from(session.station))
                    .wrapping_add(session.generation)
                    .wrapping_add(session.route_epoch);
            }
            map_probes = map_probes.saturating_add(1);
            operations = operations.saturating_add(1);
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        ticks_completed = ticks_completed.saturating_add(1);
    }
    black_box(checksum);
    latencies.sort_by(f64::total_cmp);
    let expected_operations = config.operations_per_tick.saturating_mul(config.ticks);
    let expected_admissions = config
        .operations_per_tick
        .div_ceil(8)
        .saturating_mul(config.ticks);
    let workload_ok = ticks_completed == config.ticks && operations == expected_operations;
    let admission_count_ok = admissions == expected_admissions;
    let expected_probes =
        expected_operations.saturating_mul(if config.double_lookup { 2 } else { 1 });
    let probe_count_ok = map_probes == expected_probes;
    let benchmark_ok =
        workload_ok && admission_count_ok && probe_count_ok && !time_budget_exhausted;
    ResultSummary {
        ticks_completed,
        operations,
        map_probes,
        admissions,
        checksum,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        workload_ok,
        admission_count_ok,
        probe_count_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn parse_config() -> Config {
    let mut sessions = DEFAULT_SESSIONS;
    let mut operations_per_tick = DEFAULT_OPERATIONS_PER_TICK;
    let mut ticks = DEFAULT_TICKS;
    let mut hash_map = true;
    let mut double_lookup = false;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--sessions=") {
            sessions = value.parse().unwrap_or(sessions);
        } else if let Some(value) = arg.strip_prefix("--operations-per-tick=") {
            operations_per_tick = value.parse().unwrap_or(operations_per_tick);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--btree" {
            hash_map = false;
        } else if arg == "--double-lookup" {
            double_lookup = true;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (sessions, operations_per_tick, ticks);
    sessions = sessions.max(1);
    operations_per_tick = operations_per_tick.max(1);
    ticks = ticks.max(1);
    if !allow_heavy {
        sessions = sessions.min(GUARD_MAX_SESSIONS);
        operations_per_tick = operations_per_tick.min(GUARD_MAX_OPERATIONS_PER_TICK);
        ticks = ticks.min(GUARD_MAX_TICKS);
        if operations_per_tick.saturating_mul(ticks) > GUARD_MAX_OPERATIONS {
            operations_per_tick = (GUARD_MAX_OPERATIONS / ticks).max(1);
        }
    }
    Config {
        sessions,
        operations_per_tick,
        ticks,
        hash_map,
        double_lookup,
        allow_heavy,
        guard_applied: requested != (sessions, operations_per_tick, ticks),
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
    println!("SectorSync Gateway session lookup benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_sessions={GUARD_MAX_SESSIONS}");
    println!("guard_max_operations_per_tick={GUARD_MAX_OPERATIONS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_operations={GUARD_MAX_OPERATIONS}");
    println!("sessions={}", config.sessions);
    println!("operations_per_tick={}", config.operations_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!("hash_map={}", config.hash_map);
    println!("double_lookup={}", config.double_lookup);
    println!("operations={}", result.operations);
    println!("map_probes={}", result.map_probes);
    println!("admissions={}", result.admissions);
    println!("session_checksum={}", result.checksum);
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("threshold_admission_count_ok={}", result.admission_count_ok);
    println!("threshold_probe_count_ok={}", result.probe_count_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
