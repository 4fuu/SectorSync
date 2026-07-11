//! Guarded A/B benchmark for reusable packet-security open output.

use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_transport::{
    PacketAuthenticator, PacketSecurityBox, PacketSecurityConfig, PacketSecurityOpenScratch,
    PlaintextPacketCipher,
};

const TAG_BYTES: usize = 16;
const DEFAULT_PACKETS_PER_TICK: usize = 2_000;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_PACKETS_PER_TICK: usize = 4_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4 * 1_024;
const GUARD_MAX_TICKS: usize = 20;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum OutputMode {
    #[default]
    Reuse,
    Fresh,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    packets_per_tick: usize,
    payload_bytes: usize,
    ticks: usize,
    mode: OutputMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            packets_per_tick: DEFAULT_PACKETS_PER_TICK,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            ticks: DEFAULT_TICKS,
            mode: if args.iter().any(|arg| arg == "--fresh-output") {
                OutputMode::Fresh
            } else {
                OutputMode::Reuse
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            guard_applied: false,
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--packets-per-tick=") {
                config.packets_per_tick = value.parse().unwrap_or(config.packets_per_tick);
            } else if let Some(value) = arg.strip_prefix("--payload-bytes=") {
                config.payload_bytes = value.parse().unwrap_or(config.payload_bytes);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            }
        }
        let requested = config;
        config.packets_per_tick = config.packets_per_tick.max(1);
        config.payload_bytes = config.payload_bytes.max(1);
        config.ticks = config.ticks.max(1);
        if !config.allow_heavy {
            config.packets_per_tick = config.packets_per_tick.min(GUARD_MAX_PACKETS_PER_TICK);
            config.payload_bytes = config.payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
            config.ticks = config.ticks.min(GUARD_MAX_TICKS);
        }
        config.guard_applied = config.packets_per_tick != requested.packets_per_tick
            || config.payload_bytes != requested.payload_bytes
            || config.ticks != requested.ticks;
        config
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct BenchmarkAuthenticator;

impl PacketAuthenticator for BenchmarkAuthenticator {
    type Error = Infallible;

    fn sign(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.extend_from_slice(&benchmark_tag(key_id, nonce, payload));
        Ok(())
    }

    fn verify(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        tag: &[u8],
    ) -> Result<bool, Self::Error> {
        Ok(benchmark_tag(key_id, nonce, payload) == tag)
    }
}

fn benchmark_tag(key_id: u32, nonce: u64, payload: &[u8]) -> [u8; TAG_BYTES] {
    let edge = u64::from(payload.first().copied().unwrap_or(0))
        ^ u64::from(payload.last().copied().unwrap_or(0));
    let mut tag = [0_u8; TAG_BYTES];
    tag[..8].copy_from_slice(&(u64::from(key_id) ^ nonce).to_le_bytes());
    tag[8..].copy_from_slice(&(edge ^ payload.len() as u64).to_le_bytes());
    tag
}

#[derive(Debug, Default)]
struct Stats {
    tick_ms: Vec<f64>,
    packets: usize,
    payload_bytes: usize,
    checksum: u64,
    fresh_outputs: usize,
    retained_payload_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let stats = run(config);
    let expected_packets = config.packets_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        OutputMode::Reuse => {
            stats.fresh_outputs == 0 && stats.retained_payload_capacity >= config.payload_bytes
        }
        OutputMode::Fresh => stats.fresh_outputs == expected_packets,
    };
    let workload_ok = stats.packets == expected_packets
        && stats.payload_bytes == expected_packets.saturating_mul(config.payload_bytes)
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted;
    let benchmark_ok = path_ok && workload_ok && stats.checksum > 0;

    println!("SectorSync packet security open reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_packets_per_tick={GUARD_MAX_PACKETS_PER_TICK}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("packets_per_tick={}", config.packets_per_tick);
    println!("payload_bytes_per_packet={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_output={}", config.mode == OutputMode::Reuse);
    println!("packets={}", stats.packets);
    println!("opened_payload_bytes={}", stats.payload_bytes);
    println!("payload_checksum={}", stats.checksum);
    println!("fresh_outputs={}", stats.fresh_outputs);
    println!(
        "retained_payload_capacity={}",
        stats.retained_payload_capacity
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_output_path_ok={path_ok}");
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> Stats {
    let security_config = PacketSecurityConfig {
        max_payload_bytes: config.payload_bytes,
        max_tag_bytes: TAG_BYTES,
        max_replay_history: 0,
    };
    let payload = vec![0x5a; config.payload_bytes];
    let mut sender = PacketSecurityBox::new(
        security_config,
        BenchmarkAuthenticator,
        PlaintextPacketCipher,
    );
    let wire = sender
        .seal_with_nonce(7, 1, &payload)
        .expect("guarded packet should seal");
    let mut receiver = PacketSecurityBox::new(
        security_config,
        BenchmarkAuthenticator,
        PlaintextPacketCipher,
    );
    let mut scratch = match config.mode {
        OutputMode::Reuse => PacketSecurityOpenScratch::with_capacity(config.payload_bytes),
        OutputMode::Fresh => PacketSecurityOpenScratch::new(),
    };
    let started = Instant::now();
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = Stats::default();
    'ticks: for _ in 0..config.ticks {
        let tick_started = Instant::now();
        for _ in 0..config.packets_per_tick {
            if started.elapsed() >= budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let opened = match config.mode {
                OutputMode::Reuse => {
                    receiver
                        .open_with_scratch(&wire, &mut scratch)
                        .expect("guarded reused packet should open")
                        .payload
                }
                OutputMode::Fresh => {
                    stats.fresh_outputs = stats.fresh_outputs.saturating_add(1);
                    let owned = receiver.open(&wire).expect("guarded packet should open");
                    stats.packets = stats.packets.saturating_add(1);
                    stats.payload_bytes = stats.payload_bytes.saturating_add(owned.len());
                    stats.checksum = stats
                        .checksum
                        .wrapping_add(u64::from(owned.first().copied().unwrap_or(0)))
                        .wrapping_add(u64::from(owned.last().copied().unwrap_or(0)));
                    black_box(owned);
                    continue;
                }
            };
            stats.packets = stats.packets.saturating_add(1);
            stats.payload_bytes = stats.payload_bytes.saturating_add(opened.len());
            stats.checksum = stats
                .checksum
                .wrapping_add(u64::from(opened.first().copied().unwrap_or(0)))
                .wrapping_add(u64::from(opened.last().copied().unwrap_or(0)));
            black_box(opened);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.retained_payload_capacity = scratch.retained_payload_capacity();
    stats.time_budget_exhausted |= started.elapsed() >= budget;
    stats
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile_ms(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index]
}
