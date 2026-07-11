//! Guarded A/B benchmark for borrowed replication receive visitors.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{ClientId, ComponentId, EntityId, OwnerEpoch, Tick};
use sectorsync_runtime::{ReplicationReceiveBridge, ReplicationReceiveConfig};
use sectorsync_transport::{InboundPacket, TransportReceiver};
use sectorsync_wire::{
    BinaryFrameEncoder, ComponentDelta, EntityDelta, FrameEncoder, ReplicationFrame,
};

const DEFAULT_FRAMES_PER_TICK: usize = 100;
const DEFAULT_ENTITIES: usize = 64;
const DEFAULT_COMPONENTS: usize = 4;
const DEFAULT_PAYLOAD_BYTES: usize = 64;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_FRAMES_PER_TICK: usize = 500;
const GUARD_MAX_ENTITIES: usize = 256;
const GUARD_MAX_COMPONENTS: usize = 8;
const GUARD_MAX_PAYLOAD_BYTES: usize = 1_024;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_TOTAL_PAYLOAD_BYTES: usize = 64 * 1_024 * 1_024;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ReceiveMode {
    #[default]
    Visit,
    Owned,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    frames_per_tick: usize,
    entities: usize,
    components: usize,
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
            frames_per_tick: DEFAULT_FRAMES_PER_TICK,
            entities: DEFAULT_ENTITIES,
            components: DEFAULT_COMPONENTS,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            ticks: DEFAULT_TICKS,
            mode: if args.iter().any(|arg| arg == "--owned") {
                ReceiveMode::Owned
            } else {
                ReceiveMode::Visit
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            guard_applied: false,
        };
        for arg in args {
            parse_usize(&arg, "--frames-per-tick=", &mut config.frames_per_tick);
            parse_usize(&arg, "--entities=", &mut config.entities);
            parse_usize(&arg, "--components=", &mut config.components);
            parse_usize(&arg, "--payload-bytes=", &mut config.payload_bytes);
            parse_usize(&arg, "--ticks=", &mut config.ticks);
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let requested = *self;
        self.frames_per_tick = self.frames_per_tick.max(1);
        self.entities = self.entities.max(1);
        self.components = self.components.max(1);
        self.payload_bytes = self.payload_bytes.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.frames_per_tick = self.frames_per_tick.min(GUARD_MAX_FRAMES_PER_TICK);
            self.entities = self.entities.min(GUARD_MAX_ENTITIES);
            self.components = self.components.min(GUARD_MAX_COMPONENTS);
            self.payload_bytes = self.payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
            let payload_per_frame = self
                .entities
                .saturating_mul(self.components)
                .saturating_mul(self.payload_bytes);
            let max_frames = GUARD_MAX_TOTAL_PAYLOAD_BYTES
                .checked_div(payload_per_frame)
                .unwrap_or(0)
                .max(1);
            self.frames_per_tick = self
                .frames_per_tick
                .min(max_frames.checked_div(self.ticks).unwrap_or(0).max(1));
        }
        self.guard_applied = self.frames_per_tick != requested.frames_per_tick
            || self.entities != requested.entities
            || self.components != requested.components
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
    frames: usize,
    entities: usize,
    components: usize,
    payload_bytes: usize,
    checksum: u64,
    owned_frame_materializations: usize,
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
    let config = Config::from_args(env::args().skip(1));
    let (stats, bridge_stats) = run(config);
    let expected_frames = config.frames_per_tick.saturating_mul(config.ticks);
    let expected_entities = expected_frames.saturating_mul(config.entities);
    let expected_components = expected_entities.saturating_mul(config.components);
    let expected_payload_bytes = expected_components.saturating_mul(config.payload_bytes);
    let path_ok = match config.mode {
        ReceiveMode::Visit => stats.owned_frame_materializations == 0,
        ReceiveMode::Owned => stats.owned_frame_materializations == expected_frames,
    };
    let workload_ok = stats.frames == expected_frames
        && stats.entities == expected_entities
        && stats.components == expected_components
        && stats.payload_bytes == expected_payload_bytes
        && stats.ticks_completed == config.ticks
        && bridge_stats.frames_received == expected_frames
        && bridge_stats.entities_received == expected_entities
        && bridge_stats.components_received == expected_components
        && !stats.time_budget_exhausted;
    let benchmark_ok = path_ok && workload_ok && stats.checksum > 0;

    println!("SectorSync replication receive visitor benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_frames_per_tick={GUARD_MAX_FRAMES_PER_TICK}");
    println!("guard_max_entities={GUARD_MAX_ENTITIES}");
    println!("guard_max_components={GUARD_MAX_COMPONENTS}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_total_payload_bytes={GUARD_MAX_TOTAL_PAYLOAD_BYTES}");
    println!("frames_per_tick={}", config.frames_per_tick);
    println!("entities_per_frame={}", config.entities);
    println!("components_per_entity={}", config.components);
    println!("payload_bytes_per_component={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("borrowed_visit={}", config.mode == ReceiveMode::Visit);
    println!("frames={}", stats.frames);
    println!("received_entities={}", stats.entities);
    println!("received_components={}", stats.components);
    println!("received_payload_bytes={}", stats.payload_bytes);
    println!("payload_checksum={}", stats.checksum);
    println!(
        "owned_frame_materializations={}",
        stats.owned_frame_materializations
    );
    println!("bridge_packets_received={}", bridge_stats.packets_received);
    println!("bridge_bytes_received={}", bridge_stats.bytes_received);
    println!("bridge_frames_received={}", bridge_stats.frames_received);
    println!(
        "bridge_entities_received={}",
        bridge_stats.entities_received
    );
    println!(
        "bridge_components_received={}",
        bridge_stats.components_received
    );
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

fn run(config: Config) -> (Stats, sectorsync_runtime::ReplicationReceiveStats) {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let wire = encoded_frame(config, client_id);
    let packet_count = config.frames_per_tick.saturating_mul(config.ticks);
    let remote_addr = SocketAddr::from(([127, 0, 0, 1], 23000));
    let packets = (0..packet_count)
        .map(|_| InboundPacket {
            client_id: Some(server_id),
            remote_addr,
            bytes: wire.clone(),
        })
        .collect();
    let mut transport = QueueTransport { packets };
    let mut bridge = ReplicationReceiveBridge::new(
        ReplicationReceiveConfig::new(client_id).with_expected_source(server_id),
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
            ReceiveMode::Visit => {
                let report = bridge
                    .pump_visit(&mut transport, config.frames_per_tick, |frame| {
                        for entity in frame.entities() {
                            stats.entities = stats.entities.saturating_add(1);
                            for component in entity.components() {
                                consume(component.bytes, &mut stats);
                            }
                        }
                        Ok::<_, Infallible>(())
                    })
                    .expect("guarded frames should visit");
                stats.frames = stats.frames.saturating_add(report.frames_received);
            }
            ReceiveMode::Owned => {
                let pump = bridge
                    .pump(&mut transport, config.frames_per_tick)
                    .expect("guarded frames should receive");
                stats.owned_frame_materializations = stats
                    .owned_frame_materializations
                    .saturating_add(pump.frames.len());
                stats.frames = stats.frames.saturating_add(pump.frames.len());
                for frame in &pump.frames {
                    for entity in &frame.entities {
                        stats.entities = stats.entities.saturating_add(1);
                        for component in &entity.components {
                            consume(&component.bytes, &mut stats);
                        }
                    }
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

fn consume(bytes: &[u8], stats: &mut Stats) {
    stats.components = stats.components.saturating_add(1);
    stats.payload_bytes = stats.payload_bytes.saturating_add(bytes.len());
    stats.checksum = stats
        .checksum
        .wrapping_add(u64::from(bytes.first().copied().unwrap_or(0)))
        .wrapping_add(u64::from(bytes.last().copied().unwrap_or(0)));
}

fn encoded_frame(config: Config, client_id: ClientId) -> Vec<u8> {
    let payload = vec![0x5a; config.payload_bytes];
    let components = (0..config.components)
        .map(|component| ComponentDelta {
            component_id: ComponentId::new(u16::try_from(component).expect("guarded component id")),
            version: 1,
            flags: 0,
            bytes: payload.clone(),
        })
        .collect::<Vec<_>>();
    let entities = (0..config.entities)
        .map(|entity| EntityDelta {
            entity_id: EntityId::new(u64::try_from(entity).expect("guarded entity id") + 1),
            owner_epoch: OwnerEpoch::new(1),
            components: components.clone(),
        })
        .collect::<Vec<_>>();
    let frame = ReplicationFrame {
        client_id,
        server_tick: Tick::new(1),
        entity_count: u32::try_from(entities.len()).expect("guarded entity count"),
        estimated_payload_bytes: u32::try_from(
            config
                .entities
                .saturating_mul(config.components)
                .saturating_mul(config.payload_bytes),
        )
        .unwrap_or(u32::MAX),
        entities,
    };
    let mut wire = Vec::new();
    BinaryFrameEncoder
        .encode_replication(&frame, &mut wire)
        .expect("guarded frame should encode");
    wire
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
