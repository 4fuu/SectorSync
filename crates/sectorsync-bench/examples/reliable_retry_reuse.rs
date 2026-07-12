//! Guarded A/B benchmark for reusable reliable Station retry scans.

use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::StationId;
use sectorsync_transport::{
    ReliableStationConfig, ReliableStationRetryScratch, ReliableStationSender,
    StationOutboundPacket, StationTransportSink,
};

const DEFAULT_PACKETS: usize = 512;
const DEFAULT_PAYLOAD_BYTES: usize = 512;
const DEFAULT_CALLS_PER_TICK: usize = 20;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_PACKETS: usize = 2_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4 * 1024;
const GUARD_MAX_CALLS_PER_TICK: usize = 25;
const GUARD_MAX_TICKS: usize = 10;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ScanMode {
    #[default]
    Reuse,
    Fresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    packets: usize,
    payload_bytes: usize,
    calls_per_tick: usize,
    ticks: usize,
    mode: ScanMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            packets: DEFAULT_PACKETS,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            calls_per_tick: DEFAULT_CALLS_PER_TICK,
            ticks: DEFAULT_TICKS,
            mode: ScanMode::Reuse,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--fresh-scan") {
                ScanMode::Fresh
            } else {
                ScanMode::Reuse
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--packets=") {
                config.packets = value.parse().unwrap_or(config.packets);
            } else if let Some(value) = arg.strip_prefix("--payload-bytes=") {
                config.payload_bytes = value.parse().unwrap_or(config.payload_bytes);
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
        self.packets = self.packets.max(1);
        self.payload_bytes = self.payload_bytes.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.packets = self.packets.min(GUARD_MAX_PACKETS);
            self.payload_bytes = self.payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        let max_calls = usize::from(u8::MAX).saturating_sub(1);
        let calls = self.calls_per_tick.saturating_mul(self.ticks);
        if calls > max_calls {
            self.ticks = max_calls.div_ceil(self.calls_per_tick).max(1);
            if self.calls_per_tick.saturating_mul(self.ticks) > max_calls {
                self.calls_per_tick = max_calls / self.ticks;
            }
        }
        self.guard_applied = self.packets != requested.packets
            || self.payload_bytes != requested.payload_bytes
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

#[derive(Debug, Default)]
struct CountingStationSink {
    packets: usize,
    bytes: usize,
    checksum: u64,
}

impl CountingStationSink {
    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl StationTransportSink for CountingStationSink {
    type Error = Infallible;

    fn send_station(&mut self, packet: StationOutboundPacket) -> Result<(), Self::Error> {
        self.packets = self.packets.saturating_add(1);
        self.bytes = self.bytes.saturating_add(packet.bytes.len());
        self.checksum = self
            .checksum
            .wrapping_add(u64::from(packet.source_station.get()))
            .wrapping_add(u64::from(packet.target_station.get()))
            .wrapping_add(u64::try_from(packet.bytes.len()).unwrap_or(u64::MAX))
            .wrapping_add(u64::from(packet.bytes.first().copied().unwrap_or(0)))
            .wrapping_add(u64::from(packet.bytes.last().copied().unwrap_or(0)));
        black_box(packet.bytes);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    calls: usize,
    retried: usize,
    timed_out: usize,
    wire_packets: usize,
    wire_bytes: usize,
    wire_checksum: u64,
    fresh_scan_collections: usize,
    retained_key_capacity: usize,
    in_flight_remaining: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let stats = run(config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let expected_packets = expected_calls.saturating_mul(config.packets);
    let path_ok = match config.mode {
        ScanMode::Reuse => {
            stats.fresh_scan_collections == 0 && stats.retained_key_capacity >= config.packets
        }
        ScanMode::Fresh => stats.fresh_scan_collections == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.retried == expected_packets
        && stats.wire_packets == expected_packets
        && stats.wire_bytes > 0
        && stats.wire_checksum > 0
        && stats.timed_out == 0
        && stats.in_flight_remaining == config.packets
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && path_ok;

    println!("SectorSync reliable retry reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_packets={GUARD_MAX_PACKETS}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("packets={}", config.packets);
    println!("payload_bytes={}", config.payload_bytes);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_scan={}", config.mode == ScanMode::Reuse);
    println!("calls={}", stats.calls);
    println!("retried={}", stats.retried);
    println!("timed_out={}", stats.timed_out);
    println!("wire_packets={}", stats.wire_packets);
    println!("wire_bytes={}", stats.wire_bytes);
    println!("wire_checksum={}", stats.wire_checksum);
    println!("fresh_scan_collections={}", stats.fresh_scan_collections);
    println!("retained_key_capacity={}", stats.retained_key_capacity);
    println!("in_flight_remaining={}", stats.in_flight_remaining);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_retry_path_ok={path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.calls == expected_calls && stats.retried == expected_packets
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> RunStats {
    let source = StationId::new(1);
    let target = StationId::new(2);
    let mut sender = ReliableStationSender::new(ReliableStationConfig {
        max_in_flight_per_target: config.packets,
        retry_after_ticks: 1,
        max_attempts: u8::MAX,
        max_payload_bytes: config.payload_bytes,
        max_delivered_history: 0,
    });
    let payload = vec![0x5a; config.payload_bytes];
    let mut sink = CountingStationSink::default();
    for _ in 0..config.packets {
        sender
            .send(
                &mut sink,
                StationOutboundPacket {
                    source_station: source,
                    target_station: target,
                    bytes: payload.clone(),
                },
                0,
            )
            .expect("guarded setup packet should send");
    }
    sink.clear();

    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut scratch = ReliableStationRetryScratch::new();
    let mut stats = RunStats::default();
    let mut now_tick = 1_u64;
    'ticks: for _ in 0..config.ticks {
        let tick_started = Instant::now();
        for _ in 0..config.calls_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let report = match config.mode {
                ScanMode::Reuse => sender
                    .retry_due_into(&mut sink, now_tick, &mut scratch)
                    .expect("reusable retry should send"),
                ScanMode::Fresh => {
                    stats.fresh_scan_collections = stats.fresh_scan_collections.saturating_add(1);
                    let mut fresh_scratch = ReliableStationRetryScratch::new();
                    sender
                        .retry_due_into(&mut sink, now_tick, &mut fresh_scratch)
                        .expect("fresh retry should send")
                }
            };
            stats.retried = stats.retried.saturating_add(report.retried);
            stats.timed_out = stats.timed_out.saturating_add(report.timed_out);
            stats.calls = stats.calls.saturating_add(1);
            now_tick = now_tick.saturating_add(1);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.wire_packets = sink.packets;
    stats.wire_bytes = sink.bytes;
    stats.wire_checksum = sink.checksum;
    stats.retained_key_capacity = scratch.retained_key_capacity();
    stats.in_flight_remaining = sender.in_flight_len();
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
    fn guard_clamps_retry_dimensions() {
        let config = Config::from_args(
            [
                "--packets=99999",
                "--payload-bytes=99999",
                "--calls-per-tick=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.packets, GUARD_MAX_PACKETS);
        assert_eq!(config.payload_bytes, GUARD_MAX_PAYLOAD_BYTES);
        assert!(config.calls_per_tick.saturating_mul(config.ticks) < usize::from(u8::MAX));
        assert!(config.guard_applied);
    }

    #[test]
    fn scan_modes_produce_identical_wire_work() {
        let config = Config {
            packets: 16,
            payload_bytes: 32,
            calls_per_tick: 3,
            ticks: 2,
            ..Config::default()
        };
        let reused = run(config);
        let fresh = run(Config {
            mode: ScanMode::Fresh,
            ..config
        });
        assert_eq!(reused.calls, fresh.calls);
        assert_eq!(reused.retried, fresh.retried);
        assert_eq!(reused.wire_packets, fresh.wire_packets);
        assert_eq!(reused.wire_bytes, fresh.wire_bytes);
        assert_eq!(reused.wire_checksum, fresh.wire_checksum);
        assert_eq!(reused.in_flight_remaining, fresh.in_flight_remaining);
        assert_eq!(reused.fresh_scan_collections, 0);
        assert_eq!(fresh.fresh_scan_collections, fresh.calls);
        assert!(reused.retained_key_capacity >= config.packets);
    }
}
