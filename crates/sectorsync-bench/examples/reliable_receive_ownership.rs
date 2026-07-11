//! Guarded A/B benchmark for reliable receive payload ownership.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_transport::{
    ReliableClientFrame, ReliableClientFrameRef, ReliableStationFrame, ReliableStationFrameRef,
};

const DEFAULT_FRAMES_PER_TICK: usize = 2_000;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_FRAMES_PER_TICK: usize = 4_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4_096;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_TOTAL_PAYLOAD_BYTES: usize = 64 * 1_024 * 1_024;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameKind {
    Client,
    Station,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    frames_per_tick: usize,
    payload_bytes: usize,
    ticks: usize,
    frame_kind: FrameKind,
    reuse_wire: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    frames_received: usize,
    payload_bytes_received: usize,
    receive_checksum: u64,
    reused_wire_payloads: usize,
    fresh_owned_payloads: usize,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    ownership_ok: bool,
    workload_ok: bool,
    time_budget_exhausted: bool,
    benchmark_ok: bool,
}

fn main() {
    let config = parse_config();
    let result = run(config);
    print_result(config, &result);
    if !result.benchmark_ok {
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
fn run(config: Config) -> ResultSummary {
    let payload = (0..config.payload_bytes)
        .map(|index| u8::try_from(index % 251).expect("modulo fits u8"))
        .collect::<Vec<_>>();
    let mut encoded = Vec::with_capacity(payload.len().saturating_add(17));
    match config.frame_kind {
        FrameKind::Client => ReliableClientFrame::encode_data(1, &payload, &mut encoded)
            .expect("guarded client frame should encode"),
        FrameKind::Station => ReliableStationFrame::encode_data(1, &payload, &mut encoded)
            .expect("guarded Station frame should encode"),
    }
    let mut workloads = Vec::with_capacity(config.ticks);
    for _ in 0..config.ticks {
        let mut tick = Vec::with_capacity(config.frames_per_tick);
        for _ in 0..config.frames_per_tick {
            tick.push(encoded.clone());
        }
        workloads.push(tick);
    }

    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut frames_received = 0_usize;
    let mut payload_bytes_received = 0_usize;
    let mut receive_checksum = 0_u64;
    let mut reused_wire_payloads = 0_usize;
    let mut fresh_owned_payloads = 0_usize;
    let mut time_budget_exhausted = false;

    for workload in &mut workloads {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        for wire in core::mem::take(workload) {
            if config.reuse_wire {
                let wire_pointer = wire.as_ptr();
                let (sequence, payload_len) = decode_ref(config.frame_kind, &wire);
                let payload_offset = wire.len().saturating_sub(payload_len);
                let payload = reuse_payload(wire, payload_offset, payload_len);
                reused_wire_payloads = reused_wire_payloads
                    .saturating_add(usize::from(payload.as_ptr() == wire_pointer));
                consume_payload(
                    sequence,
                    &payload,
                    &mut frames_received,
                    &mut payload_bytes_received,
                    &mut receive_checksum,
                );
            } else {
                let (sequence, payload) = decode_owned(config.frame_kind, &wire);
                fresh_owned_payloads = fresh_owned_payloads.saturating_add(1);
                consume_payload(
                    sequence,
                    &payload,
                    &mut frames_received,
                    &mut payload_bytes_received,
                    &mut receive_checksum,
                );
            }
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        ticks_completed = ticks_completed.saturating_add(1);
    }

    black_box(receive_checksum);
    latencies.sort_by(f64::total_cmp);
    let expected_frames = config.frames_per_tick.saturating_mul(config.ticks);
    let expected_payload_bytes = expected_frames.saturating_mul(config.payload_bytes);
    let ownership_ok = if config.reuse_wire {
        reused_wire_payloads == expected_frames && fresh_owned_payloads == 0
    } else {
        reused_wire_payloads == 0 && fresh_owned_payloads == expected_frames
    };
    let workload_ok = ticks_completed == config.ticks
        && frames_received == expected_frames
        && payload_bytes_received == expected_payload_bytes;
    let benchmark_ok = ownership_ok && workload_ok && !time_budget_exhausted;

    ResultSummary {
        ticks_completed,
        frames_received,
        payload_bytes_received,
        receive_checksum,
        reused_wire_payloads,
        fresh_owned_payloads,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        ownership_ok,
        workload_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn decode_ref(kind: FrameKind, wire: &[u8]) -> (u64, usize) {
    match kind {
        FrameKind::Client => match ReliableClientFrame::decode_ref(wire)
            .expect("prebuilt client frame should decode")
        {
            ReliableClientFrameRef::Data { sequence, payload } => (sequence, payload.len()),
            ReliableClientFrameRef::Ack { .. } => unreachable!("prebuilt frame is data"),
        },
        FrameKind::Station => match ReliableStationFrame::decode_ref(wire)
            .expect("prebuilt Station frame should decode")
        {
            ReliableStationFrameRef::Data { sequence, payload } => (sequence, payload.len()),
            ReliableStationFrameRef::Ack { .. } => unreachable!("prebuilt frame is data"),
        },
    }
}

fn decode_owned(kind: FrameKind, wire: &[u8]) -> (u64, Vec<u8>) {
    match kind {
        FrameKind::Client => {
            match ReliableClientFrame::decode(wire).expect("prebuilt client frame should decode") {
                ReliableClientFrame::Data { sequence, payload } => (sequence, payload),
                ReliableClientFrame::Ack { .. } => unreachable!("prebuilt frame is data"),
            }
        }
        FrameKind::Station => match ReliableStationFrame::decode(wire)
            .expect("prebuilt Station frame should decode")
        {
            ReliableStationFrame::Data { sequence, payload } => (sequence, payload),
            ReliableStationFrame::Ack { .. } => unreachable!("prebuilt frame is data"),
        },
    }
}

fn reuse_payload(mut wire: Vec<u8>, offset: usize, len: usize) -> Vec<u8> {
    wire.copy_within(offset..offset.saturating_add(len), 0);
    wire.truncate(len);
    wire
}

fn consume_payload(
    sequence: u64,
    payload: &[u8],
    frames: &mut usize,
    payload_bytes: &mut usize,
    checksum: &mut u64,
) {
    *frames = frames.saturating_add(1);
    *payload_bytes = payload_bytes.saturating_add(payload.len());
    *checksum = checksum
        .saturating_add(sequence)
        .saturating_add(u64::try_from(payload.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::from(payload.first().copied().unwrap_or_default()))
        .saturating_add(u64::from(payload.last().copied().unwrap_or_default()));
}

fn parse_config() -> Config {
    let mut frames_per_tick = DEFAULT_FRAMES_PER_TICK;
    let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
    let mut ticks = DEFAULT_TICKS;
    let mut frame_kind = FrameKind::Client;
    let mut reuse_wire = true;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--frames-per-tick=") {
            frames_per_tick = value.parse().unwrap_or(frames_per_tick);
        } else if let Some(value) = arg.strip_prefix("--payload-bytes=") {
            payload_bytes = value.parse().unwrap_or(payload_bytes);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--station" {
            frame_kind = FrameKind::Station;
        } else if arg == "--owned-decode" {
            reuse_wire = false;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    frames_per_tick = frames_per_tick.max(1);
    payload_bytes = payload_bytes.max(1);
    ticks = ticks.max(1);
    let requested = (frames_per_tick, payload_bytes, ticks);
    if !allow_heavy {
        frames_per_tick = frames_per_tick.min(GUARD_MAX_FRAMES_PER_TICK);
        payload_bytes = payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
        ticks = ticks.min(GUARD_MAX_TICKS);
        let bytes_per_frame_series = payload_bytes.saturating_mul(ticks).max(1);
        if frames_per_tick.saturating_mul(bytes_per_frame_series) > GUARD_MAX_TOTAL_PAYLOAD_BYTES {
            frames_per_tick = (GUARD_MAX_TOTAL_PAYLOAD_BYTES / bytes_per_frame_series).max(1);
        }
    }
    Config {
        frames_per_tick,
        payload_bytes,
        ticks,
        frame_kind,
        reuse_wire,
        allow_heavy,
        guard_applied: requested != (frames_per_tick, payload_bytes, ticks),
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

#[allow(clippy::too_many_lines)]
fn print_result(config: Config, result: &ResultSummary) {
    println!("SectorSync reliable receive ownership benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_frames_per_tick={GUARD_MAX_FRAMES_PER_TICK}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_total_payload_bytes={GUARD_MAX_TOTAL_PAYLOAD_BYTES}");
    println!("frames_per_tick={}", config.frames_per_tick);
    println!("payload_bytes={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!(
        "frame_kind={}",
        match config.frame_kind {
            FrameKind::Client => "client",
            FrameKind::Station => "station",
        }
    );
    println!("reuse_wire_payload={}", config.reuse_wire);
    println!("frames_received={}", result.frames_received);
    println!("payload_bytes_received={}", result.payload_bytes_received);
    println!("receive_checksum={}", result.receive_checksum);
    println!("reused_wire_payloads={}", result.reused_wire_payloads);
    println!("fresh_owned_payloads={}", result.fresh_owned_payloads);
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_ownership_ok={}", result.ownership_ok);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
