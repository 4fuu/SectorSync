//! Guarded A/B benchmark for gateway ACK ownership and report retention.

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    ClientId, CommandId, CommandIngress, CommandPriority, CommandQueueLimits, CommandQueues,
    EntityId, GatewayConfig, GatewaySessionTable, StationId, Tick,
};
use sectorsync_runtime::{GatewayClientTransportBridge, GatewayCommandPipeline};
use sectorsync_transport::{InboundPacket, OutboundPacket, TransportReceiver, TransportSink};
use sectorsync_wire::{BinaryFrameEncoder, CommandFrame, FrameEncoder};

const DEFAULT_PACKETS_PER_TICK: usize = 2_000;
const DEFAULT_PAYLOAD_BYTES: usize = 64;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_PACKETS_PER_TICK: usize = 4_000;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4 * 1_024;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_TOTAL_PAYLOAD_BYTES: usize = 64 * 1_024 * 1_024;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PumpMode {
    #[default]
    Compact,
    RetainReports,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    packets_per_tick: usize,
    payload_bytes: usize,
    ticks: usize,
    mode: PumpMode,
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
            mode: if args.iter().any(|arg| arg == "--retain-reports") {
                PumpMode::RetainReports
            } else {
                PumpMode::Compact
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

#[derive(Debug)]
struct QueueTransport {
    inbound: VecDeque<InboundPacket>,
    sent_packets: usize,
    sent_bytes: usize,
    sent_checksum: u64,
}

impl TransportReceiver for QueueTransport {
    type Error = Infallible;

    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error> {
        Ok(self.inbound.pop_front())
    }
}

impl TransportSink for QueueTransport {
    type Error = Infallible;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        self.sent_packets = self.sent_packets.saturating_add(1);
        self.sent_bytes = self.sent_bytes.saturating_add(packet.bytes.len());
        self.sent_checksum = self
            .sent_checksum
            .wrapping_add(u64::from(packet.bytes.first().copied().unwrap_or(0)))
            .wrapping_add(u64::from(packet.bytes.last().copied().unwrap_or(0)));
        black_box(packet);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Stats {
    tick_ms: Vec<f64>,
    packets: usize,
    accepted: usize,
    rejected: usize,
    acks_sent: usize,
    ack_bytes_sent: usize,
    retained_reports: usize,
    retained_ack_payloads: usize,
    report_checksum: u64,
    queued_commands: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let (stats, transport, bridge_stats, pipeline_stats) = run(config);
    let expected_packets = config.packets_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        PumpMode::Compact => stats.retained_reports == 0 && stats.retained_ack_payloads == 0,
        PumpMode::RetainReports => {
            stats.retained_reports == expected_packets
                && stats.retained_ack_payloads == expected_packets
        }
    };
    let workload_ok = stats.packets == expected_packets
        && stats.accepted == expected_packets
        && stats.rejected == 0
        && stats.acks_sent == expected_packets
        && stats.queued_commands == expected_packets
        && transport.sent_packets == expected_packets
        && bridge_stats.commands_accepted == expected_packets
        && pipeline_stats.commands_admitted == expected_packets
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted;
    let benchmark_ok = path_ok
        && workload_ok
        && stats.ack_bytes_sent > 0
        && transport.sent_checksum > 0
        && stats.report_checksum > 0;

    println!("SectorSync gateway ACK ownership benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_packets_per_tick={GUARD_MAX_PACKETS_PER_TICK}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_total_payload_bytes={GUARD_MAX_TOTAL_PAYLOAD_BYTES}");
    println!("packets_per_tick={}", config.packets_per_tick);
    println!("payload_bytes_per_command={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("compact_pump={}", config.mode == PumpMode::Compact);
    println!("packets={}", stats.packets);
    println!("commands_accepted={}", stats.accepted);
    println!("commands_rejected={}", stats.rejected);
    println!("acks_sent={}", stats.acks_sent);
    println!("ack_bytes_sent={}", stats.ack_bytes_sent);
    println!("retained_reports={}", stats.retained_reports);
    println!("retained_ack_payloads={}", stats.retained_ack_payloads);
    println!("report_checksum={}", stats.report_checksum);
    println!("transport_sent_packets={}", transport.sent_packets);
    println!("transport_sent_bytes={}", transport.sent_bytes);
    println!("transport_sent_checksum={}", transport.sent_checksum);
    println!("queued_commands={}", stats.queued_commands);
    println!("bridge_packets_received={}", bridge_stats.packets_received);
    println!(
        "bridge_commands_accepted={}",
        bridge_stats.commands_accepted
    );
    println!(
        "pipeline_commands_admitted={}",
        pipeline_stats.commands_admitted
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_ack_ownership_ok={path_ok}");
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
fn run(
    config: Config,
) -> (
    Stats,
    QueueTransport,
    sectorsync_runtime::GatewayClientTransportStats,
    sectorsync_runtime::GatewayCommandPipelineStats,
) {
    let client_id = ClientId::new(7);
    let station_id = StationId::new(1);
    let total = config.packets_per_tick.saturating_mul(config.ticks);
    let inbound = build_packets(config, client_id, total);
    let mut transport = QueueTransport {
        inbound,
        sent_packets: 0,
        sent_bytes: 0,
        sent_checksum: 0,
    };
    let mut gateway = GatewaySessionTable::new(GatewayConfig {
        max_sessions: 1,
        reconnect_grace_ticks: 10,
        max_commands_per_tick: total,
    });
    gateway
        .connect(client_id, station_id, Tick::new(1))
        .expect("guarded client should connect");
    let mut queues = BTreeMap::from([(
        station_id,
        CommandQueues::new(CommandQueueLimits {
            high: 0,
            normal: total,
            low: 0,
        }),
    )]);
    let mut pipeline = GatewayCommandPipeline::default();
    let mut bridge = GatewayClientTransportBridge::default();
    let started = Instant::now();
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = Stats::default();
    for tick in 0..config.ticks {
        if started.elapsed() >= budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let tick_started = Instant::now();
        match config.mode {
            PumpMode::Compact => {
                let summary = bridge
                    .pump_ingress_compact(
                        &mut transport,
                        &mut pipeline,
                        &mut gateway,
                        &mut queues,
                        Tick::new(1),
                        CommandIngress::RUNNING,
                        config.packets_per_tick,
                    )
                    .expect("guarded compact ingress should pump");
                stats.packets = stats.packets.saturating_add(summary.packets_received);
                stats.accepted = stats.accepted.saturating_add(summary.commands_accepted);
                stats.rejected = stats.rejected.saturating_add(summary.commands_rejected);
                stats.acks_sent = stats.acks_sent.saturating_add(summary.acks_sent);
                stats.ack_bytes_sent = stats.ack_bytes_sent.saturating_add(summary.ack_bytes_sent);
                stats.report_checksum = stats
                    .report_checksum
                    .wrapping_add(u64::try_from(summary.acks_sent).unwrap_or(u64::MAX));
            }
            PumpMode::RetainReports => {
                let pump = bridge
                    .pump_ingress(
                        &mut transport,
                        &mut pipeline,
                        &mut gateway,
                        &mut queues,
                        Tick::new(1),
                        CommandIngress::RUNNING,
                        config.packets_per_tick,
                    )
                    .expect("guarded retained ingress should pump");
                stats.packets = stats.packets.saturating_add(pump.packets_received);
                stats.accepted = stats.accepted.saturating_add(pump.commands_accepted());
                stats.rejected = stats.rejected.saturating_add(pump.commands_rejected());
                stats.acks_sent = stats.acks_sent.saturating_add(pump.acks_sent);
                stats.ack_bytes_sent = stats.ack_bytes_sent.saturating_add(pump.ack_bytes_sent);
                stats.retained_reports = stats.retained_reports.saturating_add(pump.reports.len());
                stats.retained_ack_payloads = stats.retained_ack_payloads.saturating_add(
                    pump.reports
                        .iter()
                        .filter(|report| report.ack_bytes.is_some())
                        .count(),
                );
                stats.report_checksum = stats.report_checksum.wrapping_add(
                    pump.reports
                        .iter()
                        .map(|report| u64::from(report.reason_code))
                        .sum::<u64>()
                        .wrapping_add(u64::try_from(pump.reports.len()).unwrap_or(u64::MAX)),
                );
                black_box(pump);
            }
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
        black_box(tick);
    }
    stats.queued_commands = queues
        .get(&station_id)
        .expect("guarded queue should exist")
        .total_len();
    stats.time_budget_exhausted |= started.elapsed() >= budget;
    (stats, transport, bridge.stats(), pipeline.stats())
}

fn build_packets(config: Config, client_id: ClientId, total: usize) -> VecDeque<InboundPacket> {
    let remote_addr = SocketAddr::from(([127, 0, 0, 1], 24007));
    let payload = vec![0x5a; config.payload_bytes];
    (0..total)
        .map(|index| {
            let sequence = u64::try_from(index).expect("guarded sequence") + 1;
            let command = CommandFrame {
                client_id,
                command_id: CommandId::new(sequence),
                entity_id: EntityId::new(100),
                sequence,
                kind: 1,
                priority: CommandPriority::Normal,
                payload: payload.clone(),
            };
            let mut bytes = Vec::new();
            BinaryFrameEncoder
                .encode_command(&command, &mut bytes)
                .expect("guarded command should encode");
            InboundPacket {
                client_id: Some(client_id),
                remote_addr,
                bytes,
            }
        })
        .collect()
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
