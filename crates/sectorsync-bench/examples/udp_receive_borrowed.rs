//! Guarded localhost A/B benchmark for borrowed UDP packet receive.

use std::env;
use std::hint::black_box;
use std::net::UdpSocket;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::ClientId;
use sectorsync_transport::{TransportReceiver, UdpTransport};

const DEFAULT_PACKETS_PER_TICK: usize = 500;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_PACKETS_PER_TICK: usize = 2_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4 * 1_024;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_TOTAL_PAYLOAD_BYTES: usize = 64 * 1_024 * 1_024;
const MAX_POLLS_PER_PACKET: usize = 1_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ReceiveMode {
    #[default]
    Borrowed,
    Owned,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    packets_per_tick: usize,
    payload_bytes: usize,
    ticks: usize,
    mode: ReceiveMode,
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
            mode: if args.iter().any(|arg| arg == "--owned") {
                ReceiveMode::Owned
            } else {
                ReceiveMode::Borrowed
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            guard_applied: false,
        };
        for arg in args {
            parse_usize(&arg, "--packets-per-tick=", &mut config.packets_per_tick);
            parse_usize(&arg, "--payload-bytes=", &mut config.payload_bytes);
            parse_usize(&arg, "--ticks=", &mut config.ticks);
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
            let max_packets = GUARD_MAX_TOTAL_PAYLOAD_BYTES
                .checked_div(self.payload_bytes)
                .unwrap_or(0)
                .max(1);
            self.packets_per_tick = self
                .packets_per_tick
                .min(max_packets.checked_div(self.ticks).unwrap_or(0).max(1));
        }
        self.guard_applied = self.packets_per_tick != requested.packets_per_tick
            || self.payload_bytes != requested.payload_bytes
            || self.ticks != requested.ticks;
    }
}

fn parse_usize(arg: &str, prefix: &str, target: &mut usize) {
    if let Some(value) = arg.strip_prefix(prefix) {
        *target = value.parse().unwrap_or(*target);
    }
}

#[derive(Debug, Default)]
struct Stats {
    tick_ms: Vec<f64>,
    packets: usize,
    payload_bytes: usize,
    checksum: u64,
    poll_misses: usize,
    owned_packet_materializations: usize,
    borrowed_buffer_reuses: usize,
    first_borrowed_ptr: Option<usize>,
    ticks_completed: usize,
    poll_exhausted: bool,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let stats = run(config);
    let expected_packets = config.packets_per_tick.saturating_mul(config.ticks);
    let expected_payload_bytes = expected_packets.saturating_mul(config.payload_bytes);
    let path_ok = match config.mode {
        ReceiveMode::Borrowed => {
            stats.owned_packet_materializations == 0
                && stats.borrowed_buffer_reuses == expected_packets.saturating_sub(1)
        }
        ReceiveMode::Owned => {
            stats.owned_packet_materializations == expected_packets
                && stats.borrowed_buffer_reuses == 0
        }
    };
    let workload_ok = stats.packets == expected_packets
        && stats.payload_bytes == expected_payload_bytes
        && stats.ticks_completed == config.ticks
        && !stats.poll_exhausted
        && !stats.time_budget_exhausted;
    let benchmark_ok = path_ok && workload_ok && stats.checksum > 0;

    println!("SectorSync borrowed UDP receive benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_packets_per_tick={GUARD_MAX_PACKETS_PER_TICK}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_total_payload_bytes={GUARD_MAX_TOTAL_PAYLOAD_BYTES}");
    println!("max_polls_per_packet={MAX_POLLS_PER_PACKET}");
    println!("packets_per_tick={}", config.packets_per_tick);
    println!("payload_bytes_per_packet={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("borrowed_receive={}", config.mode == ReceiveMode::Borrowed);
    println!("packets={}", stats.packets);
    println!("received_payload_bytes={}", stats.payload_bytes);
    println!("payload_checksum={}", stats.checksum);
    println!("poll_misses={}", stats.poll_misses);
    println!(
        "owned_packet_materializations={}",
        stats.owned_packet_materializations
    );
    println!("borrowed_buffer_reuses={}", stats.borrowed_buffer_reuses);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_receive_path_ok={path_ok}");
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("poll_exhausted={}", stats.poll_exhausted);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> Stats {
    let sender = UdpSocket::bind("127.0.0.1:0").expect("localhost sender should bind");
    let sender_addr = sender.local_addr().expect("sender address should exist");
    let mut receiver = UdpTransport::bind("127.0.0.1:0").expect("localhost receiver should bind");
    let receiver_addr = receiver
        .local_addr()
        .expect("receiver address should exist");
    receiver.register_client(ClientId::new(1), sender_addr);
    receiver.set_recv_buffer_size(config.payload_bytes);
    let payload = vec![0x5a; config.payload_bytes];
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
            sender
                .send_to(&payload, receiver_addr)
                .expect("guarded localhost datagram should send");
            let packet_ready = match config.mode {
                ReceiveMode::Borrowed => receive_borrowed(&mut receiver, &mut stats),
                ReceiveMode::Owned => receive_owned(&mut receiver, &mut stats),
            };
            if !packet_ready {
                stats.poll_exhausted = true;
                break 'ticks;
            }
            stats.packets = stats.packets.saturating_add(1);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.time_budget_exhausted |= started.elapsed() >= budget;
    stats
}

fn receive_borrowed(receiver: &mut UdpTransport, stats: &mut Stats) -> bool {
    for _ in 0..MAX_POLLS_PER_PACKET {
        match receiver.try_recv_ref() {
            Ok(Some(packet)) => {
                let pointer = packet.bytes.as_ptr() as usize;
                if let Some(first) = stats.first_borrowed_ptr {
                    stats.borrowed_buffer_reuses = stats
                        .borrowed_buffer_reuses
                        .saturating_add(usize::from(pointer == first));
                } else {
                    stats.first_borrowed_ptr = Some(pointer);
                }
                consume(packet.bytes, stats);
                black_box(packet);
                return true;
            }
            Ok(None) => {
                stats.poll_misses = stats.poll_misses.saturating_add(1);
                std::thread::yield_now();
            }
            Err(error) => panic!("guarded borrowed receive failed: {error}"),
        }
    }
    false
}

fn receive_owned(receiver: &mut UdpTransport, stats: &mut Stats) -> bool {
    for _ in 0..MAX_POLLS_PER_PACKET {
        match receiver.try_recv() {
            Ok(Some(packet)) => {
                stats.owned_packet_materializations =
                    stats.owned_packet_materializations.saturating_add(1);
                consume(&packet.bytes, stats);
                black_box(packet);
                return true;
            }
            Ok(None) => {
                stats.poll_misses = stats.poll_misses.saturating_add(1);
                std::thread::yield_now();
            }
            Err(error) => panic!("guarded owned receive failed: {error}"),
        }
    }
    false
}

fn consume(bytes: &[u8], stats: &mut Stats) {
    stats.payload_bytes = stats.payload_bytes.saturating_add(bytes.len());
    stats.checksum = stats
        .checksum
        .wrapping_add(u64::from(bytes.first().copied().unwrap_or(0)))
        .wrapping_add(u64::from(bytes.last().copied().unwrap_or(0)));
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
