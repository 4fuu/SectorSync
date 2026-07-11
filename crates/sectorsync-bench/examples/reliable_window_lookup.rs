//! Guarded A/B benchmark for reliable per-peer in-flight window lookup.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::ClientId;
use sectorsync_transport::{
    FakeTransport, OutboundPacket, ReliableClientConfig, ReliableClientSender,
};

const DEFAULT_PACKETS: usize = 4_096;
const DEFAULT_PEERS: usize = 256;
const DEFAULT_QUERIES_PER_TICK: usize = 1_000;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_PACKETS: usize = 8_000;
const GUARD_MAX_PEERS: usize = 2_000;
const GUARD_MAX_QUERIES_PER_TICK: usize = 2_000;
const GUARD_MAX_TICKS: usize = 10;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum LookupMode {
    #[default]
    Indexed,
    FullScan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    packets: usize,
    peers: usize,
    queries_per_tick: usize,
    ticks: usize,
    mode: LookupMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            packets: DEFAULT_PACKETS,
            peers: DEFAULT_PEERS,
            queries_per_tick: DEFAULT_QUERIES_PER_TICK,
            ticks: DEFAULT_TICKS,
            mode: LookupMode::Indexed,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--full-scan") {
                LookupMode::FullScan
            } else {
                LookupMode::Indexed
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--packets=") {
                config.packets = value.parse().unwrap_or(config.packets);
            } else if let Some(value) = arg.strip_prefix("--peers=") {
                config.peers = value.parse().unwrap_or(config.peers);
            } else if let Some(value) = arg.strip_prefix("--queries-per-tick=") {
                config.queries_per_tick = value.parse().unwrap_or(config.queries_per_tick);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            }
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let requested = *self;
        self.packets = self.packets.max(1);
        self.peers = self.peers.max(1).min(self.packets);
        self.queries_per_tick = self.queries_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.packets = self.packets.min(GUARD_MAX_PACKETS);
            self.peers = self.peers.min(GUARD_MAX_PEERS).min(self.packets);
            self.queries_per_tick = self.queries_per_tick.min(GUARD_MAX_QUERIES_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.packets != requested.packets
            || self.peers != requested.peers
            || self.queries_per_tick != requested.queries_per_tick
            || self.ticks != requested.ticks;
    }
}

struct Workload {
    sender: ReliableClientSender,
    assignments: Vec<ClientId>,
    peers: Vec<ClientId>,
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    queries: usize,
    count_checksum: usize,
    full_scan_queries: usize,
    in_flight_packets: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let workload = create_workload(config);
    let stats = run(&workload, config);
    let expected_queries = config.queries_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        LookupMode::Indexed => stats.full_scan_queries == 0,
        LookupMode::FullScan => stats.full_scan_queries == expected_queries,
    };
    let benchmark_ok = stats.queries == expected_queries
        && stats.count_checksum > 0
        && stats.in_flight_packets == config.packets
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && path_ok;

    println!("SectorSync reliable window lookup benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_packets={GUARD_MAX_PACKETS}");
    println!("guard_max_peers={GUARD_MAX_PEERS}");
    println!("guard_max_queries_per_tick={GUARD_MAX_QUERIES_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("packets={}", config.packets);
    println!("peers={}", config.peers);
    println!("queries_per_tick={}", config.queries_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("indexed_lookup={}", config.mode == LookupMode::Indexed);
    println!("queries={}", stats.queries);
    println!("count_checksum={}", stats.count_checksum);
    println!("full_scan_queries={}", stats.full_scan_queries);
    println!("in_flight_packets={}", stats.in_flight_packets);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_lookup_path_ok={path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.queries == expected_queries
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_workload(config: Config) -> Workload {
    let peers = (0..config.peers)
        .map(|index| {
            ClientId::new(u64::try_from(index.saturating_add(1)).expect("guarded peer fits u64"))
        })
        .collect::<Vec<_>>();
    let max_per_peer = config.packets.div_ceil(config.peers);
    let mut sender = ReliableClientSender::new(ReliableClientConfig {
        max_in_flight_per_peer: max_per_peer,
        retry_after_ticks: u64::MAX,
        max_attempts: 1,
        max_payload_bytes: 1,
        max_delivered_history: 0,
    });
    let mut transport = FakeTransport::default();
    let mut assignments = Vec::with_capacity(config.packets);
    for packet in 0..config.packets {
        let peer = peers[packet % peers.len()];
        sender
            .send(
                &mut transport,
                OutboundPacket {
                    client_id: peer,
                    bytes: vec![0],
                },
                0,
            )
            .expect("balanced setup packet should fit its peer window");
        assignments.push(peer);
    }
    Workload {
        sender,
        assignments,
        peers,
    }
}

fn run(workload: &Workload, config: Config) -> RunStats {
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats {
        in_flight_packets: workload.sender.in_flight_len(),
        ..RunStats::default()
    };
    'ticks: for tick in 0..config.ticks {
        let tick_started = Instant::now();
        for query in 0..config.queries_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let peer = workload.peers[(query.wrapping_mul(17).wrapping_add(tick)) % config.peers];
            let count = match config.mode {
                LookupMode::Indexed => workload.sender.in_flight_for(peer),
                LookupMode::FullScan => {
                    stats.full_scan_queries = stats.full_scan_queries.saturating_add(1);
                    workload
                        .assignments
                        .iter()
                        .filter(|assigned| **assigned == peer)
                        .count()
                }
            };
            stats.count_checksum = stats.count_checksum.saturating_add(count);
            stats.queries = stats.queries.saturating_add(1);
            black_box(count);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
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
    fn guard_clamps_window_dimensions() {
        let config = Config::from_args(
            [
                "--packets=99999",
                "--peers=99999",
                "--queries-per-tick=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.packets, GUARD_MAX_PACKETS);
        assert_eq!(config.peers, GUARD_MAX_PEERS);
        assert_eq!(config.queries_per_tick, GUARD_MAX_QUERIES_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn lookup_modes_produce_identical_counts() {
        let config = Config {
            packets: 31,
            peers: 7,
            queries_per_tick: 20,
            ticks: 2,
            ..Config::default()
        };
        let workload = create_workload(config);
        let indexed = run(&workload, config);
        let scanned = run(
            &workload,
            Config {
                mode: LookupMode::FullScan,
                ..config
            },
        );
        assert_eq!(indexed.queries, scanned.queries);
        assert_eq!(indexed.count_checksum, scanned.count_checksum);
        assert_eq!(indexed.in_flight_packets, scanned.in_flight_packets);
        assert_eq!(indexed.full_scan_queries, 0);
        assert_eq!(scanned.full_scan_queries, scanned.queries);
    }
}
