//! Guarded A/B benchmark for reusable packet-security sealing scratch.

use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_transport::{
    PacketAuthenticator, PacketSecurityBox, PacketSecurityConfig, PacketSecurityScratch,
    PlaintextPacketCipher,
};

const TAG_BYTES: usize = 16;
const HEADER_BYTES: usize = 22;
const DEFAULT_PACKETS_PER_TICK: usize = 2_000;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_PACKETS_PER_TICK: usize = 4_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4 * 1024;
const GUARD_MAX_TICKS: usize = 20;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ScratchMode {
    #[default]
    Reuse,
    Fresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    packets_per_tick: usize,
    payload_bytes: usize,
    ticks: usize,
    mode: ScratchMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            packets_per_tick: DEFAULT_PACKETS_PER_TICK,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            ticks: DEFAULT_TICKS,
            mode: ScratchMode::Reuse,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--fresh-scratch") {
                ScratchMode::Fresh
            } else {
                ScratchMode::Reuse
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
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
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let requested = *self;
        self.packets_per_tick = self.packets_per_tick.max(1);
        self.payload_bytes = self.payload_bytes.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.packets_per_tick = self.packets_per_tick.min(GUARD_MAX_PACKETS_PER_TICK);
            self.payload_bytes = self.payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.packets_per_tick != requested.packets_per_tick
            || self.payload_bytes != requested.payload_bytes
            || self.ticks != requested.ticks;
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
        let edge = u64::from(payload.first().copied().unwrap_or(0))
            ^ u64::from(payload.last().copied().unwrap_or(0));
        out.extend_from_slice(&(u64::from(key_id) ^ nonce).to_le_bytes());
        out.extend_from_slice(&(edge ^ payload.len() as u64).to_le_bytes());
        Ok(())
    }

    fn verify(
        &mut self,
        _key_id: u32,
        _nonce: u64,
        _payload: &[u8],
        _tag: &[u8],
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    packets: usize,
    wire_bytes: usize,
    wire_checksum: u64,
    fresh_scratch_sets: usize,
    retained_payload_capacity: usize,
    retained_tag_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let stats = run(config);
    let expected_packets = config.packets_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        ScratchMode::Reuse => {
            stats.fresh_scratch_sets == 0
                && stats.retained_payload_capacity >= config.payload_bytes
                && stats.retained_tag_capacity >= TAG_BYTES
        }
        ScratchMode::Fresh => stats.fresh_scratch_sets == expected_packets,
    };
    let benchmark_ok = stats.packets == expected_packets
        && stats.wire_bytes > 0
        && stats.wire_checksum > 0
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && path_ok;

    println!("SectorSync packet security seal reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_packets_per_tick={GUARD_MAX_PACKETS_PER_TICK}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("packets_per_tick={}", config.packets_per_tick);
    println!("payload_bytes={}", config.payload_bytes);
    println!("tag_bytes={TAG_BYTES}");
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_scratch={}", config.mode == ScratchMode::Reuse);
    println!("packets={}", stats.packets);
    println!("wire_bytes={}", stats.wire_bytes);
    println!("wire_checksum={}", stats.wire_checksum);
    println!("fresh_scratch_sets={}", stats.fresh_scratch_sets);
    println!(
        "retained_payload_capacity={}",
        stats.retained_payload_capacity
    );
    println!("retained_tag_capacity={}", stats.retained_tag_capacity);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_scratch_path_ok={path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.packets == expected_packets
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> RunStats {
    let security_config = PacketSecurityConfig {
        max_payload_bytes: config.payload_bytes,
        max_tag_bytes: TAG_BYTES,
        max_replay_history: 0,
    };
    let mut security = PacketSecurityBox::new(
        security_config,
        BenchmarkAuthenticator,
        PlaintextPacketCipher,
    );
    let payload = vec![0x5a; config.payload_bytes];
    let mut scratch = match config.mode {
        ScratchMode::Reuse => PacketSecurityScratch::with_capacity(config.payload_bytes, TAG_BYTES),
        ScratchMode::Fresh => PacketSecurityScratch::new(),
    };
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats::default();
    let mut nonce = 1_u64;
    'ticks: for _ in 0..config.ticks {
        let tick_started = Instant::now();
        for _ in 0..config.packets_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let wire = match config.mode {
                ScratchMode::Reuse => {
                    let mut out = Vec::with_capacity(
                        HEADER_BYTES
                            .saturating_add(config.payload_bytes)
                            .saturating_add(TAG_BYTES),
                    );
                    security
                        .seal_with_nonce_into(7, nonce, &payload, &mut out, &mut scratch)
                        .expect("guarded reused packet should seal");
                    out
                }
                ScratchMode::Fresh => {
                    stats.fresh_scratch_sets = stats.fresh_scratch_sets.saturating_add(1);
                    security
                        .seal_with_nonce(7, nonce, &payload)
                        .expect("guarded fresh packet should seal")
                }
            };
            stats.packets = stats.packets.saturating_add(1);
            stats.wire_bytes = stats.wire_bytes.saturating_add(wire.len());
            stats.wire_checksum = stats
                .wire_checksum
                .wrapping_add(u64::try_from(wire.len()).unwrap_or(u64::MAX))
                .wrapping_add(u64::from(wire.first().copied().unwrap_or(0)))
                .wrapping_add(u64::from(wire.last().copied().unwrap_or(0)));
            nonce = nonce.saturating_add(1);
            black_box(wire);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.retained_payload_capacity = scratch.retained_payload_capacity();
    stats.retained_tag_capacity = scratch.retained_tag_capacity();
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
    fn guard_clamps_security_dimensions() {
        let config = Config::from_args(
            [
                "--packets-per-tick=99999",
                "--payload-bytes=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.packets_per_tick, GUARD_MAX_PACKETS_PER_TICK);
        assert_eq!(config.payload_bytes, GUARD_MAX_PAYLOAD_BYTES);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn scratch_modes_produce_identical_wire_work() {
        let config = Config {
            packets_per_tick: 16,
            payload_bytes: 32,
            ticks: 2,
            ..Config::default()
        };
        let reused = run(config);
        let fresh = run(Config {
            mode: ScratchMode::Fresh,
            ..config
        });
        assert_eq!(reused.packets, fresh.packets);
        assert_eq!(reused.wire_bytes, fresh.wire_bytes);
        assert_eq!(reused.wire_checksum, fresh.wire_checksum);
        assert_eq!(reused.fresh_scratch_sets, 0);
        assert_eq!(fresh.fresh_scratch_sets, fresh.packets);
        assert!(reused.retained_payload_capacity >= config.payload_bytes);
        assert!(reused.retained_tag_capacity >= TAG_BYTES);
    }
}
