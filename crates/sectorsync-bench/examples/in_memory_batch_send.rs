//! Guarded A/B benchmark for in-memory client batch locking.

use std::env;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::ClientId;
use sectorsync_transport::{
    ClientTransportLimits, InMemoryTransportHub, OutboundPacket, PacketBatch, TransportSink,
};

const DEFAULT_BATCHES: usize = 100;
const DEFAULT_PACKETS_PER_BATCH: usize = 1_000;
const DEFAULT_PAYLOAD_BYTES: usize = 8;
const GUARD_MAX_BATCHES: usize = 1_000;
const GUARD_MAX_PACKETS_PER_BATCH: usize = 1_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4_096;
const GUARD_MAX_PACKETS: usize = 100_000;
const GUARD_MAX_PAYLOAD_WORK: usize = 64 * 1024 * 1024;
const TIME_BUDGET_MS: u64 = 10_000;
const BATCH_LOCK_PACKETS: usize = 64;

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug)]
struct Config {
    batches: usize,
    packets_per_batch: usize,
    payload_bytes: usize,
    per_packet: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

fn main() {
    let config = parse_config();
    let packet_count = config.batches.saturating_mul(config.packets_per_batch);
    let payload_work = packet_count.saturating_mul(config.payload_bytes);
    let source_id = ClientId::new(1);
    let target_id = ClientId::new(2);
    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: packet_count,
        max_packet_bytes: config.payload_bytes,
    });
    let mut source = hub
        .endpoint(source_id, address(20_001))
        .expect("source should register");
    hub.endpoint(target_id, address(20_002))
        .expect("target should register");
    let batches = build_batches(config, target_id);
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.batches);
    let mut batches_completed = 0_usize;
    let mut send_calls = 0_usize;
    let mut time_budget_exhausted = false;

    for batch in batches {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        if config.per_packet {
            for packet in batch.packets {
                source.send(packet).expect("guarded packet should send");
                send_calls = send_calls.saturating_add(1);
            }
        } else {
            source.send_batch(batch).expect("guarded batch should send");
            send_calls = send_calls.saturating_add(1);
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        batches_completed = batches_completed.saturating_add(1);
    }

    latencies.sort_by(f64::total_cmp);
    let stats = hub.stats().expect("stats should read");
    let queued = hub
        .queued_len(target_id)
        .expect("queue should read")
        .expect("target should remain registered");
    let expected_send_calls = if config.per_packet {
        packet_count
    } else {
        config.batches
    };
    let workload_ok = batches_completed == config.batches
        && stats.packets_sent == packet_count
        && stats.bytes_sent == payload_work
        && queued == packet_count;
    let send_calls_ok = send_calls == expected_send_calls;
    let expected_lock_acquisitions = if config.per_packet {
        packet_count
    } else {
        config
            .packets_per_batch
            .div_ceil(BATCH_LOCK_PACKETS)
            .saturating_mul(config.batches)
    };
    let benchmark_ok = workload_ok && send_calls_ok && !time_budget_exhausted;

    println!("SectorSync in-memory batch send benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_batches={GUARD_MAX_BATCHES}");
    println!("guard_max_packets_per_batch={GUARD_MAX_PACKETS_PER_BATCH}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_packets={GUARD_MAX_PACKETS}");
    println!("guard_max_payload_work={GUARD_MAX_PAYLOAD_WORK}");
    println!("batches={}", config.batches);
    println!("batches_completed={batches_completed}");
    println!("packets_per_batch={}", config.packets_per_batch);
    println!("payload_bytes={}", config.payload_bytes);
    println!("per_packet={}", config.per_packet);
    println!("packet_count={packet_count}");
    println!("payload_work={payload_work}");
    println!("send_calls={send_calls}");
    println!("batch_lock_packets={BATCH_LOCK_PACKETS}");
    println!("expected_lock_acquisitions={expected_lock_acquisitions}");
    println!("queued_packets={queued}");
    println!("batch_ms_p50={:.3}", percentile(&latencies, 0.50));
    println!("batch_ms_p95={:.3}", percentile(&latencies, 0.95));
    println!("batch_ms_p99={:.3}", percentile(&latencies, 0.99));
    println!(
        "batch_ms_max={:.3}",
        latencies.last().copied().unwrap_or_default()
    );
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("threshold_send_calls_ok={send_calls_ok}");
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={time_budget_exhausted}");
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn build_batches(config: Config, target_id: ClientId) -> Vec<PacketBatch> {
    (0..config.batches)
        .map(|batch_index| {
            let mut batch = PacketBatch {
                packets: Vec::with_capacity(config.packets_per_batch),
            };
            for packet_index in 0..config.packets_per_batch {
                batch.push(OutboundPacket {
                    client_id: target_id,
                    bytes: vec![
                        u8::try_from((batch_index + packet_index) % 251)
                            .expect("bounded byte fits u8");
                        config.payload_bytes
                    ],
                });
            }
            batch
        })
        .collect()
}

fn parse_config() -> Config {
    let mut batches = DEFAULT_BATCHES;
    let mut packets_per_batch = DEFAULT_PACKETS_PER_BATCH;
    let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
    let mut per_packet = false;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--batches=") {
            batches = value.parse().unwrap_or(batches);
        } else if let Some(value) = arg.strip_prefix("--packets-per-batch=") {
            packets_per_batch = value.parse().unwrap_or(packets_per_batch);
        } else if let Some(value) = arg.strip_prefix("--payload-bytes=") {
            payload_bytes = value.parse().unwrap_or(payload_bytes);
        } else if arg == "--per-packet" {
            per_packet = true;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (batches, packets_per_batch, payload_bytes);
    batches = batches.max(1);
    packets_per_batch = packets_per_batch.max(1);
    payload_bytes = payload_bytes.max(1);
    if !allow_heavy {
        batches = batches.min(GUARD_MAX_BATCHES);
        packets_per_batch = packets_per_batch.min(GUARD_MAX_PACKETS_PER_BATCH);
        payload_bytes = payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
        if batches.saturating_mul(packets_per_batch) > GUARD_MAX_PACKETS {
            packets_per_batch = (GUARD_MAX_PACKETS / batches).max(1);
        }
        let packets = batches.saturating_mul(packets_per_batch);
        if packets.saturating_mul(payload_bytes) > GUARD_MAX_PAYLOAD_WORK {
            payload_bytes = (GUARD_MAX_PAYLOAD_WORK / packets).max(1);
        }
    }
    Config {
        batches,
        packets_per_batch,
        payload_bytes,
        per_packet,
        allow_heavy,
        guard_applied: requested != (batches, packets_per_batch, payload_bytes),
    }
}

fn address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
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
