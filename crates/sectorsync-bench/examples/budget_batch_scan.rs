//! Guarded A/B benchmark for packet batch budget validation scans.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::ClientId;
use sectorsync_transport::{OutboundPacket, PacketBatch};

const DEFAULT_PACKETS: usize = 100_000;
const DEFAULT_PAYLOAD_BYTES: usize = 8;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_PACKETS: usize = 200_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 1_024;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_INSPECTIONS: usize = 4_000_000;
const GUARD_MAX_PAYLOAD_WORK: usize = 64 * 1024 * 1024;
const TIME_BUDGET_MS: u64 = 10_000;

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug)]
struct Config {
    packets: usize,
    payload_bytes: usize,
    ticks: usize,
    double_scan: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

fn main() {
    let config = parse_config();
    let batch = build_batch(config);
    let packet_budget = config.payload_bytes;
    let expected_bytes = config.packets.saturating_mul(config.payload_bytes);
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut inspections = 0_usize;
    let mut checksum = 0_usize;
    let mut first_oversized = None;
    let mut time_budget_exhausted = false;

    for _ in 0..config.ticks {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        let result = if config.double_scan {
            double_scan(&batch, packet_budget)
        } else {
            single_scan(&batch, packet_budget)
        };
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        inspections = inspections.saturating_add(result.inspections);
        checksum = checksum.wrapping_add(result.bytes);
        first_oversized = result.first_oversized;
        ticks_completed = ticks_completed.saturating_add(1);
    }

    black_box(checksum);
    latencies.sort_by(f64::total_cmp);
    let expected_inspections = config
        .packets
        .saturating_mul(config.ticks)
        .saturating_mul(if config.double_scan { 2 } else { 1 });
    let workload_ok =
        ticks_completed == config.ticks && checksum == expected_bytes.saturating_mul(config.ticks);
    let inspection_count_ok = inspections == expected_inspections;
    let validation_ok = first_oversized.is_none();
    let benchmark_ok =
        workload_ok && inspection_count_ok && validation_ok && !time_budget_exhausted;

    println!("SectorSync budget batch scan benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_packets={GUARD_MAX_PACKETS}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_inspections={GUARD_MAX_INSPECTIONS}");
    println!("guard_max_payload_work={GUARD_MAX_PAYLOAD_WORK}");
    println!("packets={}", config.packets);
    println!("payload_bytes={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={ticks_completed}");
    println!("double_scan={}", config.double_scan);
    println!("inspections={inspections}");
    println!("validated_bytes={expected_bytes}");
    println!("validation_checksum={checksum}");
    println!("first_oversized={first_oversized:?}");
    println!("tick_ms_p50={:.3}", percentile(&latencies, 0.50));
    println!("tick_ms_p95={:.3}", percentile(&latencies, 0.95));
    println!("tick_ms_p99={:.3}", percentile(&latencies, 0.99));
    println!(
        "tick_ms_max={:.3}",
        latencies.last().copied().unwrap_or_default()
    );
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("threshold_inspection_count_ok={inspection_count_ok}");
    println!("threshold_validation_ok={validation_ok}");
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={time_budget_exhausted}");
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug)]
struct ScanResult {
    bytes: usize,
    first_oversized: Option<usize>,
    inspections: usize,
}

fn single_scan(batch: &PacketBatch, packet_budget: usize) -> ScanResult {
    let mut bytes = 0_usize;
    let mut first_oversized = None;
    for packet in &batch.packets {
        let packet_bytes = packet.bytes.len();
        bytes = bytes.saturating_add(packet_bytes);
        if first_oversized.is_none() && packet_bytes > packet_budget {
            first_oversized = Some(packet_bytes);
        }
    }
    ScanResult {
        bytes,
        first_oversized,
        inspections: batch.packets.len(),
    }
}

fn double_scan(batch: &PacketBatch, packet_budget: usize) -> ScanResult {
    let bytes = batch
        .packets
        .iter()
        .map(|packet| packet.bytes.len())
        .fold(0_usize, usize::saturating_add);
    let first_oversized = batch
        .packets
        .iter()
        .map(|packet| packet.bytes.len())
        .find(|bytes| *bytes > packet_budget);
    ScanResult {
        bytes,
        first_oversized,
        inspections: batch.packets.len().saturating_mul(2),
    }
}

fn build_batch(config: Config) -> PacketBatch {
    let mut batch = PacketBatch {
        packets: Vec::with_capacity(config.packets),
    };
    for index in 0..config.packets {
        batch.push(OutboundPacket {
            client_id: ClientId::new(u64::try_from(index % 1_024).expect("guarded id fits u64")),
            bytes: vec![
                u8::try_from(index % 251).expect("bounded byte fits u8");
                config.payload_bytes
            ],
        });
    }
    batch
}

fn parse_config() -> Config {
    let mut packets = DEFAULT_PACKETS;
    let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
    let mut ticks = DEFAULT_TICKS;
    let mut double_scan = false;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--packets=") {
            packets = value.parse().unwrap_or(packets);
        } else if let Some(value) = arg.strip_prefix("--payload-bytes=") {
            payload_bytes = value.parse().unwrap_or(payload_bytes);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--double-scan" {
            double_scan = true;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (packets, payload_bytes, ticks);
    packets = packets.max(1);
    payload_bytes = payload_bytes.max(1);
    ticks = ticks.max(1);
    if !allow_heavy {
        packets = packets.min(GUARD_MAX_PACKETS);
        payload_bytes = payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
        ticks = ticks.min(GUARD_MAX_TICKS);
        let scan_multiplier = if double_scan { 2 } else { 1 };
        if packets
            .saturating_mul(ticks)
            .saturating_mul(scan_multiplier)
            > GUARD_MAX_INSPECTIONS
        {
            packets = (GUARD_MAX_INSPECTIONS / ticks / scan_multiplier).max(1);
        }
        if packets.saturating_mul(payload_bytes) > GUARD_MAX_PAYLOAD_WORK {
            payload_bytes = (GUARD_MAX_PAYLOAD_WORK / packets).max(1);
        }
    }
    Config {
        packets,
        payload_bytes,
        ticks,
        double_scan,
        allow_heavy,
        guard_applied: requested != (packets, payload_bytes, ticks),
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
