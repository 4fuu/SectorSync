//! Guarded algorithm benchmark for allocation-free gateway expiry scans.

use std::collections::BTreeMap;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

const DEFAULT_SESSIONS: usize = 5_000;
const DEFAULT_CALLS_PER_TICK: usize = 50;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_SESSIONS: usize = 20_000;
const GUARD_MAX_CALLS_PER_TICK: usize = 100;
const GUARD_MAX_TICKS: usize = 20;
const TIME_BUDGET_MS: u64 = 10_000;
const NOW: u64 = 100;
const GRACE: u64 = 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ExpiryMode {
    #[default]
    Retain,
    CollectRemove,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    sessions: usize,
    calls_per_tick: usize,
    ticks: usize,
    mode: ExpiryMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sessions: DEFAULT_SESSIONS,
            calls_per_tick: DEFAULT_CALLS_PER_TICK,
            ticks: DEFAULT_TICKS,
            mode: ExpiryMode::Retain,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--collect-remove") {
                ExpiryMode::CollectRemove
            } else {
                ExpiryMode::Retain
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--sessions=") {
                config.sessions = value.parse().unwrap_or(config.sessions);
            } else if let Some(value) = arg.strip_prefix("--calls-per-tick=") {
                config.calls_per_tick = value.parse().unwrap_or(config.calls_per_tick);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            }
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let requested = *self;
        self.sessions = self.sessions.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.sessions = self.sessions.min(GUARD_MAX_SESSIONS);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.sessions != requested.sessions
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionState {
    Connected,
    Disconnected { since: u64 },
}

#[derive(Debug, Default)]
struct RunStats {
    operation_ms: Vec<f64>,
    calls: usize,
    expired_sessions: usize,
    remaining_sessions: usize,
    temporary_id_collections: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let source = create_sessions(config.sessions);
    let stats = run(&source, config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        ExpiryMode::Retain => stats.temporary_id_collections == 0,
        ExpiryMode::CollectRemove => stats.temporary_id_collections == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.ticks_completed == config.ticks
        && stats.expired_sessions > 0
        && stats.remaining_sessions > 0
        && !stats.time_budget_exhausted
        && path_ok;

    println!("SectorSync gateway expiry scan benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_sessions={GUARD_MAX_SESSIONS}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("sessions={}", config.sessions);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("retain_scan={}", config.mode == ExpiryMode::Retain);
    println!("calls={}", stats.calls);
    println!("expired_sessions={}", stats.expired_sessions);
    println!("remaining_sessions={}", stats.remaining_sessions);
    println!(
        "temporary_id_collections={}",
        stats.temporary_id_collections
    );
    println!(
        "operation_ms_p50={:.3}",
        percentile_ms(&stats.operation_ms, 0.50)
    );
    println!(
        "operation_ms_p95={:.3}",
        percentile_ms(&stats.operation_ms, 0.95)
    );
    println!(
        "operation_ms_p99={:.3}",
        percentile_ms(&stats.operation_ms, 0.99)
    );
    println!(
        "operation_ms_max={:.3}",
        percentile_ms(&stats.operation_ms, 1.00)
    );
    println!("threshold_path_ok={path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.calls == expected_calls
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_sessions(count: usize) -> BTreeMap<u64, SessionState> {
    (0..count)
        .map(|index| {
            let state = match index % 4 {
                0 => SessionState::Connected,
                1 => SessionState::Disconnected { since: NOW - GRACE },
                _ => SessionState::Disconnected { since: 0 },
            };
            (
                u64::try_from(index).expect("guarded session index fits u64"),
                state,
            )
        })
        .collect()
}

fn run(source: &BTreeMap<u64, SessionState>, config: Config) -> RunStats {
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats::default();
    'ticks: for _ in 0..config.ticks {
        for _ in 0..config.calls_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let mut sessions = source.clone();
            let operation_started = Instant::now();
            let expired = match config.mode {
                ExpiryMode::Retain => retain_expired(&mut sessions),
                ExpiryMode::CollectRemove => {
                    stats.temporary_id_collections =
                        stats.temporary_id_collections.saturating_add(1);
                    collect_remove_expired(&mut sessions)
                }
            };
            stats
                .operation_ms
                .push(operation_started.elapsed().as_secs_f64() * 1_000.0);
            stats.expired_sessions = stats.expired_sessions.saturating_add(expired);
            stats.remaining_sessions = stats.remaining_sessions.saturating_add(sessions.len());
            stats.calls = stats.calls.saturating_add(1);
            black_box(sessions);
        }
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
}

fn retain_expired(sessions: &mut BTreeMap<u64, SessionState>) -> usize {
    let before = sessions.len();
    sessions.retain(|_, state| match state {
        SessionState::Connected => true,
        SessionState::Disconnected { since } => NOW.saturating_sub(*since) <= GRACE,
    });
    before - sessions.len()
}

fn collect_remove_expired(sessions: &mut BTreeMap<u64, SessionState>) -> usize {
    let expired = sessions
        .iter()
        .filter_map(|(client_id, state)| match state {
            SessionState::Connected => None,
            SessionState::Disconnected { since } => {
                (NOW.saturating_sub(*since) > GRACE).then_some(*client_id)
            }
        })
        .collect::<Vec<_>>();
    for client_id in &expired {
        sessions.remove(client_id);
    }
    expired.len()
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile_ms(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_clamps_expiry_dimensions() {
        let config = Config::from_args(
            [
                "--sessions=999999",
                "--calls-per-tick=999999",
                "--ticks=999999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.sessions, GUARD_MAX_SESSIONS);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn expiry_algorithms_have_identical_results() {
        let source = create_sessions(37);
        let mut retained = source.clone();
        let mut removed = source;
        let retain_count = retain_expired(&mut retained);
        let remove_count = collect_remove_expired(&mut removed);

        assert_eq!(retain_count, remove_count);
        assert_eq!(retained, removed);
    }
}
