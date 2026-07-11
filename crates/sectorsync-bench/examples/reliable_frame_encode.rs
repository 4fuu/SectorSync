//! Guarded A/B benchmark for borrowed reliable Station data-frame encoding.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_transport::ReliableStationFrame;

const DEFAULT_FRAMES_PER_TICK: usize = 2_000;
const DEFAULT_PAYLOAD_BYTES: usize = 2 * 1024;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_FRAMES_PER_TICK: usize = 4_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4 * 1024;
const GUARD_MAX_TICKS: usize = 20;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum EncodeMode {
    #[default]
    Borrowed,
    Owned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    frames_per_tick: usize,
    payload_bytes: usize,
    ticks: usize,
    mode: EncodeMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frames_per_tick: DEFAULT_FRAMES_PER_TICK,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            ticks: DEFAULT_TICKS,
            mode: EncodeMode::Borrowed,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--owned-frame") {
                EncodeMode::Owned
            } else {
                EncodeMode::Borrowed
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--frames-per-tick=") {
                config.frames_per_tick = value.parse().unwrap_or(config.frames_per_tick);
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
        self.frames_per_tick = self.frames_per_tick.max(1);
        self.payload_bytes = self.payload_bytes.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.frames_per_tick = self.frames_per_tick.min(GUARD_MAX_FRAMES_PER_TICK);
            self.payload_bytes = self.payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.frames_per_tick != requested.frames_per_tick
            || self.payload_bytes != requested.payload_bytes
            || self.ticks != requested.ticks;
    }
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    frames: usize,
    wire_bytes: usize,
    wire_checksum: u64,
    temporary_payload_copies: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let stats = run(config);
    let expected_frames = config.frames_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        EncodeMode::Borrowed => stats.temporary_payload_copies == 0,
        EncodeMode::Owned => stats.temporary_payload_copies == expected_frames,
    };
    let benchmark_ok = stats.frames == expected_frames
        && stats.wire_bytes > 0
        && stats.wire_checksum > 0
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && path_ok;

    println!("SectorSync reliable frame encode benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_frames_per_tick={GUARD_MAX_FRAMES_PER_TICK}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("frames_per_tick={}", config.frames_per_tick);
    println!("payload_bytes={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("borrowed_payload={}", config.mode == EncodeMode::Borrowed);
    println!("frames={}", stats.frames);
    println!("wire_bytes={}", stats.wire_bytes);
    println!("wire_checksum={}", stats.wire_checksum);
    println!(
        "temporary_payload_copies={}",
        stats.temporary_payload_copies
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_encode_path_ok={path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.frames == expected_frames
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> RunStats {
    let payload = vec![0x5a; config.payload_bytes];
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats::default();
    let mut sequence = 1_u64;
    'ticks: for _ in 0..config.ticks {
        let tick_started = Instant::now();
        for _ in 0..config.frames_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let mut wire = Vec::with_capacity(config.payload_bytes.saturating_add(17));
            match config.mode {
                EncodeMode::Borrowed => {
                    ReliableStationFrame::encode_data(sequence, &payload, &mut wire)
                        .expect("guarded borrowed frame should encode");
                }
                EncodeMode::Owned => {
                    stats.temporary_payload_copies =
                        stats.temporary_payload_copies.saturating_add(1);
                    ReliableStationFrame::Data {
                        sequence,
                        payload: payload.clone(),
                    }
                    .encode(&mut wire)
                    .expect("guarded owned frame should encode");
                }
            }
            stats.frames = stats.frames.saturating_add(1);
            stats.wire_bytes = stats.wire_bytes.saturating_add(wire.len());
            stats.wire_checksum = stats
                .wire_checksum
                .wrapping_add(u64::try_from(wire.len()).unwrap_or(u64::MAX))
                .wrapping_add(u64::from(wire.first().copied().unwrap_or(0)))
                .wrapping_add(u64::from(wire.last().copied().unwrap_or(0)));
            sequence = sequence.saturating_add(1);
            black_box(wire);
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
    fn guard_clamps_encode_dimensions() {
        let config = Config::from_args(
            [
                "--frames-per-tick=99999",
                "--payload-bytes=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.frames_per_tick, GUARD_MAX_FRAMES_PER_TICK);
        assert_eq!(config.payload_bytes, GUARD_MAX_PAYLOAD_BYTES);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn encode_modes_produce_identical_wire_work() {
        let config = Config {
            frames_per_tick: 8,
            payload_bytes: 32,
            ticks: 2,
            ..Config::default()
        };
        let borrowed = run(config);
        let owned = run(Config {
            mode: EncodeMode::Owned,
            ..config
        });
        assert_eq!(borrowed.frames, owned.frames);
        assert_eq!(borrowed.wire_bytes, owned.wire_bytes);
        assert_eq!(borrowed.wire_checksum, owned.wire_checksum);
        assert_eq!(borrowed.temporary_payload_copies, 0);
        assert_eq!(owned.temporary_payload_copies, owned.frames);
    }
}
