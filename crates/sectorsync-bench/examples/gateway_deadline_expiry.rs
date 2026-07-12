//! Guarded A/B benchmark for caller-driven gateway expiry maintenance.

use std::collections::BTreeMap;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{ClientId, GatewayConfig, GatewaySessionTable, StationId, Tick};

const DEFAULT_SESSIONS: usize = 20_000;
const DEFAULT_CALLS: usize = 20;
const DEFAULT_EXPIRED_EVERY: usize = 1_024;
const GUARD_MAX_SESSIONS: usize = 65_536;
const GUARD_MAX_CALLS: usize = 50;
const TIME_BUDGET: Duration = Duration::from_secs(10);
const NOW: u64 = 100;
const GRACE: u64 = 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Mode {
    #[default]
    Deadline,
    FullScan,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    sessions: usize,
    calls: usize,
    expired_every: usize,
    mode: Mode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            sessions: DEFAULT_SESSIONS,
            calls: DEFAULT_CALLS,
            expired_every: DEFAULT_EXPIRED_EVERY,
            mode: if args.iter().any(|arg| arg == "--full-scan") {
                Mode::FullScan
            } else {
                Mode::Deadline
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            guard_applied: false,
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--sessions=") {
                config.sessions = value.parse().unwrap_or(config.sessions);
            } else if let Some(value) = arg.strip_prefix("--calls=") {
                config.calls = value.parse().unwrap_or(config.calls);
            } else if let Some(value) = arg.strip_prefix("--expired-every=") {
                config.expired_every = value.parse().unwrap_or(config.expired_every);
            }
        }
        let requested = (config.sessions, config.calls);
        config.sessions = config.sessions.max(1);
        config.calls = config.calls.max(1);
        config.expired_every = config.expired_every.max(2);
        if !config.allow_heavy {
            config.sessions = config.sessions.min(GUARD_MAX_SESSIONS);
            config.calls = config.calls.min(GUARD_MAX_CALLS);
        }
        config.guard_applied = requested != (config.sessions, config.calls);
        config
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReferenceState {
    Connected,
    Disconnected { since: u64 },
}

#[derive(Clone, Debug)]
struct World {
    table: GatewaySessionTable,
    reference: BTreeMap<ClientId, ReferenceState>,
}

#[derive(Debug, Default)]
struct Stats {
    operation_ms: Vec<f64>,
    calls_completed: usize,
    expired_sessions: usize,
    remaining_sessions: usize,
    checksum: u64,
    entries_examined: usize,
    stale_deadlines: usize,
    deadline_capacity_max: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let world = create_world(config.sessions, config.expired_every);
    let expected = expected_result(&world.reference);
    let stats = run(&world, config);
    let calls = u64::try_from(stats.calls_completed).unwrap_or(0);
    let conservation_ok = stats.expired_sessions
        == expected.0.saturating_mul(stats.calls_completed)
        && stats.remaining_sessions == expected.1.saturating_mul(stats.calls_completed)
        && stats.checksum == expected.2.wrapping_mul(calls);
    let bounded_work_ok = match config.mode {
        Mode::Deadline => stats.entries_examined == stats.expired_sessions,
        Mode::FullScan => stats.entries_examined == config.sessions.saturating_mul(config.calls),
    };
    let benchmark_ok = stats.calls_completed == config.calls
        && !stats.time_budget_exhausted
        && conservation_ok
        && bounded_work_ok;

    println!("SectorSync gateway deadline expiry benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_sessions={GUARD_MAX_SESSIONS}");
    println!("guard_max_calls={GUARD_MAX_CALLS}");
    println!("sessions={}", config.sessions);
    println!("calls={}", config.calls);
    println!("expired_every={}", config.expired_every);
    println!("calls_completed={}", stats.calls_completed);
    println!("deadline_index={}", config.mode == Mode::Deadline);
    println!("expired_sessions={}", stats.expired_sessions);
    println!("remaining_sessions={}", stats.remaining_sessions);
    println!("entries_examined={}", stats.entries_examined);
    println!("stale_deadlines={}", stats.stale_deadlines);
    println!("deadline_capacity_max={}", stats.deadline_capacity_max);
    println!("result_checksum={}", stats.checksum);
    println!("reference_checksum_per_call={}", expected.2);
    println!("conservation_ok={conservation_ok}");
    println!(
        "operation_ms_p50={:.3}",
        percentile(&stats.operation_ms, 0.50)
    );
    println!(
        "operation_ms_p95={:.3}",
        percentile(&stats.operation_ms, 0.95)
    );
    println!(
        "operation_ms_p99={:.3}",
        percentile(&stats.operation_ms, 0.99)
    );
    println!(
        "operation_ms_max={:.3}",
        percentile(&stats.operation_ms, 1.00)
    );
    println!("threshold_bounded_work_ok={bounded_work_ok}");
    println!("time_budget_ms={}", TIME_BUDGET.as_millis());
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_world(sessions: usize, expired_every: usize) -> World {
    let mut table = GatewaySessionTable::new(GatewayConfig {
        max_sessions: sessions,
        reconnect_grace_ticks: GRACE,
        max_commands_per_tick: 64,
    });
    let mut reference = BTreeMap::new();
    for index in 0..sessions {
        let client_id = ClientId::new(u64::try_from(index).expect("client id fits u64"));
        table
            .connect(client_id, StationId::new(1), Tick::new(0))
            .expect("guarded table accepts session");
        let state = match index % expired_every {
            0 => {
                table
                    .disconnect(client_id, Tick::new(NOW - GRACE))
                    .expect("session exists");
                ReferenceState::Disconnected { since: NOW - GRACE }
            }
            1 => {
                table
                    .disconnect(client_id, Tick::new(0))
                    .expect("session exists");
                ReferenceState::Disconnected { since: 0 }
            }
            _ => ReferenceState::Connected,
        };
        reference.insert(client_id, state);
    }
    World { table, reference }
}

fn expected_result(reference: &BTreeMap<ClientId, ReferenceState>) -> (usize, usize, u64) {
    let mut sessions = reference.clone();
    let expired = expire_scan(&mut sessions);
    (expired, sessions.len(), checksum(sessions.keys().copied()))
}

fn run(world: &World, config: Config) -> Stats {
    let started = Instant::now();
    let mut stats = Stats::default();
    for _ in 0..config.calls {
        if started.elapsed() >= TIME_BUDGET {
            stats.time_budget_exhausted = true;
            break;
        }
        match config.mode {
            Mode::Deadline => {
                let mut table = world.table.clone();
                let operation_started = Instant::now();
                let expired = table.expire_disconnected(Tick::new(NOW));
                stats
                    .operation_ms
                    .push(operation_started.elapsed().as_secs_f64() * 1_000.0);
                let table_stats = table.stats();
                stats.entries_examined = stats
                    .entries_examined
                    .saturating_add(table_stats.expiry_deadlines_popped);
                stats.stale_deadlines = stats
                    .stale_deadlines
                    .saturating_add(table_stats.stale_expiry_deadlines);
                stats.deadline_capacity_max = stats
                    .deadline_capacity_max
                    .max(table.expiry_deadline_capacity());
                stats.expired_sessions = stats.expired_sessions.saturating_add(expired);
                stats.remaining_sessions = stats.remaining_sessions.saturating_add(table.len());
                stats.checksum = stats.checksum.wrapping_add(checksum(
                    world
                        .reference
                        .keys()
                        .copied()
                        .filter(|client_id| table.session(*client_id).is_some()),
                ));
                black_box(table);
            }
            Mode::FullScan => {
                let mut sessions = world.reference.clone();
                let operation_started = Instant::now();
                let expired = expire_scan(&mut sessions);
                stats
                    .operation_ms
                    .push(operation_started.elapsed().as_secs_f64() * 1_000.0);
                stats.entries_examined = stats.entries_examined.saturating_add(config.sessions);
                stats.expired_sessions = stats.expired_sessions.saturating_add(expired);
                stats.remaining_sessions = stats.remaining_sessions.saturating_add(sessions.len());
                stats.checksum = stats
                    .checksum
                    .wrapping_add(checksum(sessions.keys().copied()));
                black_box(sessions);
            }
        }
        stats.calls_completed = stats.calls_completed.saturating_add(1);
    }
    stats
}

fn expire_scan(sessions: &mut BTreeMap<ClientId, ReferenceState>) -> usize {
    let before = sessions.len();
    sessions.retain(|_, state| match state {
        ReferenceState::Connected => true,
        ReferenceState::Disconnected { since } => NOW.saturating_sub(*since) <= GRACE,
    });
    before - sessions.len()
}

fn checksum(clients: impl Iterator<Item = ClientId>) -> u64 {
    clients.fold(0_u64, |checksum, client_id| {
        checksum
            .wrapping_mul(1_099_511_628_211)
            .wrapping_add(client_id.get())
            .wrapping_add(1)
    })
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
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_and_scan_results_match() {
        let world = create_world(81, 40);
        let expected = expected_result(&world.reference);
        let mut table = world.table;
        let expired = table.expire_disconnected(Tick::new(NOW));
        let actual_checksum = checksum(
            world
                .reference
                .keys()
                .copied()
                .filter(|client_id| table.session(*client_id).is_some()),
        );

        assert_eq!(expired, expected.0);
        assert_eq!(table.len(), expected.1);
        assert_eq!(actual_checksum, expected.2);
    }
}
