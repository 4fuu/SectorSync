//! Guarded A/B benchmark for mixed client-bound frame visitors.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    BarrierId, BarrierState, ClientId, CommandId, ComponentId, EntityId, OwnerEpoch, Tick,
};
use sectorsync_runtime::{
    ClientInboundFrameRef, ClientTransportBridge, ClientTransportConfig, ClientTransportStats,
};
use sectorsync_transport::{InboundPacket, TransportReceiver};
use sectorsync_wire::{
    BarrierFrame, BinaryFrameEncoder, CommandAckFrame, ComponentDelta, EntityDelta, FrameEncoder,
    ReplicationFrame,
};

const DEFAULT_REPLICATION_PER_TICK: usize = 100;
const DEFAULT_ACKS_PER_TICK: usize = 20;
const DEFAULT_BARRIERS_PER_TICK: usize = 1;
const DEFAULT_ENTITIES: usize = 64;
const DEFAULT_COMPONENTS: usize = 4;
const DEFAULT_PAYLOAD_BYTES: usize = 64;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_REPLICATION_PER_TICK: usize = 500;
const GUARD_MAX_ACKS_PER_TICK: usize = 500;
const GUARD_MAX_BARRIERS_PER_TICK: usize = 20;
const GUARD_MAX_ENTITIES: usize = 256;
const GUARD_MAX_COMPONENTS: usize = 8;
const GUARD_MAX_PAYLOAD_BYTES: usize = 1_024;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_TOTAL_PAYLOAD_BYTES: usize = 64 * 1_024 * 1_024;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Mode {
    #[default]
    Visit,
    Owned,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    replication_per_tick: usize,
    acks_per_tick: usize,
    barriers_per_tick: usize,
    entities: usize,
    components: usize,
    payload_bytes: usize,
    ticks: usize,
    mode: Mode,
    allow_heavy: bool,
    guard_applied: bool,
}

#[derive(Debug, Default)]
struct Stats {
    tick_ms: Vec<f64>,
    packets: usize,
    acks: usize,
    replication_frames: usize,
    barriers: usize,
    entities: usize,
    components: usize,
    payload_bytes: usize,
    checksum: u64,
    owned_replication_materializations: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

#[derive(Debug)]
struct QueueTransport {
    packets: VecDeque<InboundPacket>,
}

impl TransportReceiver for QueueTransport {
    type Error = Infallible;

    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error> {
        Ok(self.packets.pop_front())
    }
}

fn main() {
    let config = parse_config();
    let (stats, bridge_stats) = run(config);
    print_result(config, &stats, bridge_stats);
}

#[allow(clippy::too_many_lines)]
fn run(config: Config) -> (Stats, ClientTransportStats) {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let replication = encoded_replication(config, client_id);
    let ack = encoded_ack(client_id);
    let barrier = encoded_barrier(client_id);
    let packets_per_tick = config
        .replication_per_tick
        .saturating_add(config.acks_per_tick)
        .saturating_add(config.barriers_per_tick);
    let mut packets = VecDeque::with_capacity(packets_per_tick.saturating_mul(config.ticks));
    let remote_addr = SocketAddr::from(([127, 0, 0, 1], 23000));
    for _ in 0..config.ticks {
        append_packets(
            &mut packets,
            config.replication_per_tick,
            server_id,
            remote_addr,
            &replication,
        );
        append_packets(
            &mut packets,
            config.acks_per_tick,
            server_id,
            remote_addr,
            &ack,
        );
        append_packets(
            &mut packets,
            config.barriers_per_tick,
            server_id,
            remote_addr,
            &barrier,
        );
    }
    let mut transport = QueueTransport { packets };
    let mut bridge = ClientTransportBridge::new(
        ClientTransportConfig::new(client_id, server_id).with_expected_source(server_id),
    );
    let started = Instant::now();
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = Stats::default();
    for _ in 0..config.ticks {
        if started.elapsed() >= budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let tick_started = Instant::now();
        match config.mode {
            Mode::Visit => {
                let report = bridge
                    .pump_visit(&mut transport, packets_per_tick, |frame| {
                        match frame {
                            ClientInboundFrameRef::CommandAck(frame) => {
                                stats.acks = stats.acks.saturating_add(1);
                                stats.checksum = stats
                                    .checksum
                                    .saturating_add(frame.command_id.get())
                                    .saturating_add(u64::from(frame.reason_code));
                            }
                            ClientInboundFrameRef::Replication(frame) => {
                                stats.replication_frames =
                                    stats.replication_frames.saturating_add(1);
                                for entity in frame.entities() {
                                    stats.entities = stats.entities.saturating_add(1);
                                    for component in entity.components() {
                                        consume_component(component.bytes, &mut stats);
                                    }
                                }
                            }
                            ClientInboundFrameRef::Barrier(frame) => {
                                stats.barriers = stats.barriers.saturating_add(1);
                                stats.checksum = stats
                                    .checksum
                                    .saturating_add(frame.barrier_id.get())
                                    .saturating_add(frame.server_tick.get());
                            }
                        }
                        Ok::<_, Infallible>(())
                    })
                    .expect("guarded mixed frames should visit");
                stats.packets = stats.packets.saturating_add(report.packets_received);
            }
            Mode::Owned => {
                let pump = bridge
                    .pump(&mut transport, packets_per_tick)
                    .expect("guarded mixed frames should receive");
                stats.packets = stats.packets.saturating_add(pump.packets_received);
                stats.acks = stats.acks.saturating_add(pump.command_acks.len());
                stats.barriers = stats.barriers.saturating_add(pump.barriers.len());
                stats.replication_frames = stats
                    .replication_frames
                    .saturating_add(pump.replication_frames.len());
                stats.owned_replication_materializations = stats
                    .owned_replication_materializations
                    .saturating_add(pump.replication_frames.len());
                for frame in &pump.command_acks {
                    stats.checksum = stats
                        .checksum
                        .saturating_add(frame.command_id.get())
                        .saturating_add(u64::from(frame.reason_code));
                }
                for frame in &pump.replication_frames {
                    for entity in &frame.entities {
                        stats.entities = stats.entities.saturating_add(1);
                        for component in &entity.components {
                            consume_component(&component.bytes, &mut stats);
                        }
                    }
                }
                for frame in &pump.barriers {
                    stats.checksum = stats
                        .checksum
                        .saturating_add(frame.barrier_id.get())
                        .saturating_add(frame.server_tick.get());
                }
                black_box(pump);
            }
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.time_budget_exhausted |= started.elapsed() >= budget;
    (stats, bridge.stats())
}

fn append_packets(
    packets: &mut VecDeque<InboundPacket>,
    count: usize,
    source: ClientId,
    remote_addr: SocketAddr,
    bytes: &[u8],
) {
    for _ in 0..count {
        packets.push_back(InboundPacket {
            client_id: Some(source),
            remote_addr,
            bytes: bytes.to_vec(),
        });
    }
}

fn consume_component(bytes: &[u8], stats: &mut Stats) {
    stats.components = stats.components.saturating_add(1);
    stats.payload_bytes = stats.payload_bytes.saturating_add(bytes.len());
    stats.checksum = stats
        .checksum
        .saturating_add(u64::from(bytes.first().copied().unwrap_or_default()))
        .saturating_add(u64::from(bytes.last().copied().unwrap_or_default()));
}

fn encoded_replication(config: Config, client_id: ClientId) -> Vec<u8> {
    let payload = vec![0x5a; config.payload_bytes];
    let components = (0..config.components)
        .map(|component| ComponentDelta {
            component_id: ComponentId::new(
                u16::try_from(component).expect("guarded component id fits u16"),
            ),
            version: 1,
            flags: 0,
            bytes: payload.clone(),
        })
        .collect::<Vec<_>>();
    let entities = (0..config.entities)
        .map(|entity| EntityDelta {
            entity_id: EntityId::new(
                u64::try_from(entity).expect("guarded entity id fits u64") + 1,
            ),
            owner_epoch: OwnerEpoch::new(1),
            components: components.clone(),
        })
        .collect::<Vec<_>>();
    let frame = ReplicationFrame {
        client_id,
        server_tick: Tick::new(1),
        entity_count: u32::try_from(entities.len()).expect("guarded entity count fits u32"),
        estimated_payload_bytes: u32::try_from(
            config
                .entities
                .saturating_mul(config.components)
                .saturating_mul(config.payload_bytes),
        )
        .unwrap_or(u32::MAX),
        entities,
    };
    let mut bytes = Vec::new();
    BinaryFrameEncoder
        .encode_replication(&frame, &mut bytes)
        .expect("guarded replication should encode");
    bytes
}

fn encoded_ack(client_id: ClientId) -> Vec<u8> {
    let mut bytes = Vec::new();
    BinaryFrameEncoder
        .encode_command_ack(
            &CommandAckFrame {
                client_id,
                command_id: CommandId::new(1),
                server_tick: Tick::new(1),
                accepted: true,
                reason_code: 0,
            },
            &mut bytes,
        )
        .expect("guarded ACK should encode");
    bytes
}

fn encoded_barrier(client_id: ClientId) -> Vec<u8> {
    let mut bytes = Vec::new();
    BinaryFrameEncoder
        .encode_barrier(
            &BarrierFrame {
                client_id,
                barrier_id: BarrierId::new(1),
                server_tick: Tick::new(1),
                state: BarrierState::Frozen,
            },
            &mut bytes,
        )
        .expect("guarded barrier should encode");
    bytes
}

fn parse_config() -> Config {
    let mut config = Config {
        replication_per_tick: DEFAULT_REPLICATION_PER_TICK,
        acks_per_tick: DEFAULT_ACKS_PER_TICK,
        barriers_per_tick: DEFAULT_BARRIERS_PER_TICK,
        entities: DEFAULT_ENTITIES,
        components: DEFAULT_COMPONENTS,
        payload_bytes: DEFAULT_PAYLOAD_BYTES,
        ticks: DEFAULT_TICKS,
        mode: Mode::Visit,
        allow_heavy: false,
        guard_applied: false,
    };
    let args = env::args().skip(1).collect::<Vec<_>>();
    config.mode = if args.iter().any(|arg| arg == "--owned") {
        Mode::Owned
    } else {
        Mode::Visit
    };
    config.allow_heavy = args.iter().any(|arg| arg == "--allow-heavy");
    for arg in args {
        parse_usize(
            &arg,
            "--replication-per-tick=",
            &mut config.replication_per_tick,
        );
        parse_usize(&arg, "--acks-per-tick=", &mut config.acks_per_tick);
        parse_usize(&arg, "--barriers-per-tick=", &mut config.barriers_per_tick);
        parse_usize(&arg, "--entities=", &mut config.entities);
        parse_usize(&arg, "--components=", &mut config.components);
        parse_usize(&arg, "--payload-bytes=", &mut config.payload_bytes);
        parse_usize(&arg, "--ticks=", &mut config.ticks);
    }
    normalize(&mut config);
    config
}

fn parse_usize(arg: &str, prefix: &str, target: &mut usize) {
    if let Some(value) = arg.strip_prefix(prefix) {
        *target = value.parse().unwrap_or(*target);
    }
}

fn normalize(config: &mut Config) {
    let requested = *config;
    config.replication_per_tick = config.replication_per_tick.max(1);
    config.acks_per_tick = config.acks_per_tick.max(1);
    config.barriers_per_tick = config.barriers_per_tick.max(1);
    config.entities = config.entities.max(1);
    config.components = config.components.max(1);
    config.payload_bytes = config.payload_bytes.max(1);
    config.ticks = config.ticks.max(1);
    if !config.allow_heavy {
        config.replication_per_tick = config
            .replication_per_tick
            .min(GUARD_MAX_REPLICATION_PER_TICK);
        config.acks_per_tick = config.acks_per_tick.min(GUARD_MAX_ACKS_PER_TICK);
        config.barriers_per_tick = config.barriers_per_tick.min(GUARD_MAX_BARRIERS_PER_TICK);
        config.entities = config.entities.min(GUARD_MAX_ENTITIES);
        config.components = config.components.min(GUARD_MAX_COMPONENTS);
        config.payload_bytes = config.payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
        config.ticks = config.ticks.min(GUARD_MAX_TICKS);
        let payload_per_frame = config
            .entities
            .saturating_mul(config.components)
            .saturating_mul(config.payload_bytes);
        let max_frames = GUARD_MAX_TOTAL_PAYLOAD_BYTES
            .checked_div(payload_per_frame)
            .unwrap_or(0)
            .max(1);
        config.replication_per_tick = config
            .replication_per_tick
            .min(max_frames.checked_div(config.ticks).unwrap_or(0).max(1));
    }
    config.guard_applied = config.replication_per_tick != requested.replication_per_tick
        || config.acks_per_tick != requested.acks_per_tick
        || config.barriers_per_tick != requested.barriers_per_tick
        || config.entities != requested.entities
        || config.components != requested.components
        || config.payload_bytes != requested.payload_bytes
        || config.ticks != requested.ticks;
}

#[allow(clippy::too_many_lines)]
fn print_result(config: Config, stats: &Stats, bridge: ClientTransportStats) {
    let expected_replication = config.replication_per_tick.saturating_mul(config.ticks);
    let expected_acks = config.acks_per_tick.saturating_mul(config.ticks);
    let expected_barriers = config.barriers_per_tick.saturating_mul(config.ticks);
    let expected_packets = expected_replication
        .saturating_add(expected_acks)
        .saturating_add(expected_barriers);
    let expected_entities = expected_replication.saturating_mul(config.entities);
    let expected_components = expected_entities.saturating_mul(config.components);
    let expected_payload_bytes = expected_components.saturating_mul(config.payload_bytes);
    let path_ok = match config.mode {
        Mode::Visit => stats.owned_replication_materializations == 0,
        Mode::Owned => stats.owned_replication_materializations == expected_replication,
    };
    let workload_ok = stats.ticks_completed == config.ticks
        && stats.packets == expected_packets
        && stats.acks == expected_acks
        && stats.replication_frames == expected_replication
        && stats.barriers == expected_barriers
        && stats.entities == expected_entities
        && stats.components == expected_components
        && stats.payload_bytes == expected_payload_bytes
        && bridge.packets_received == expected_packets
        && bridge.command_acks_received == expected_acks
        && bridge.replication_frames_received == expected_replication
        && bridge.barrier_frames_received == expected_barriers
        && bridge.entities_received == expected_entities
        && bridge.components_received == expected_components;
    let benchmark_ok = path_ok && workload_ok && stats.checksum > 0 && !stats.time_budget_exhausted;

    println!("SectorSync mixed client receive visitor benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_replication_per_tick={GUARD_MAX_REPLICATION_PER_TICK}");
    println!("guard_max_acks_per_tick={GUARD_MAX_ACKS_PER_TICK}");
    println!("guard_max_barriers_per_tick={GUARD_MAX_BARRIERS_PER_TICK}");
    println!("guard_max_entities={GUARD_MAX_ENTITIES}");
    println!("guard_max_components={GUARD_MAX_COMPONENTS}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_total_payload_bytes={GUARD_MAX_TOTAL_PAYLOAD_BYTES}");
    println!("replication_per_tick={}", config.replication_per_tick);
    println!("acks_per_tick={}", config.acks_per_tick);
    println!("barriers_per_tick={}", config.barriers_per_tick);
    println!("entities_per_replication={}", config.entities);
    println!("components_per_entity={}", config.components);
    println!("payload_bytes_per_component={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("borrowed_visit={}", config.mode == Mode::Visit);
    println!("packets_received={}", stats.packets);
    println!("command_acks_received={}", stats.acks);
    println!("replication_frames_received={}", stats.replication_frames);
    println!("barrier_frames_received={}", stats.barriers);
    println!("entities_received={}", stats.entities);
    println!("components_received={}", stats.components);
    println!("payload_bytes_received={}", stats.payload_bytes);
    println!("receive_checksum={}", stats.checksum);
    println!(
        "owned_replication_materializations={}",
        stats.owned_replication_materializations
    );
    println!("bridge_packets_received={}", bridge.packets_received);
    println!("bridge_bytes_received={}", bridge.bytes_received);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_receive_path_ok={path_ok}");
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
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
