//! Lightweight benchmark entry point.

use std::env;
use std::time::Instant;

use sectorsync_core::prelude::{
    Bounds, CellIndex, CellLoadSample, ClientId, CommandEnvelope, CommandId, CommandIngress,
    CommandPriority, CommandQueueLimits, CommandQueues, CompiledSyncPolicy, EntityId, GridSpec,
    HotspotPlanner, HotspotSeverity, HotspotThresholds, InstanceId, NodeId, PolicyId, PolicyTable,
    Position3, RangeOnlyVisibility, ReplicationBudget, ReplicationPlanner, Station, StationConfig,
    StationId, StationLoadSample, Tick, Vec3, ViewerQuery,
};
use sectorsync_transport::{FakeTransport, OutboundPacket, TransportSink};
use sectorsync_wire::{BinaryFrameEncoder, FrameEncoder, ReplicationFrame};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Baseline {
    FullBroadcast,
    RoomBroadcast,
    NaiveGrid,
    SectorSync,
}

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    entities: usize,
    clients: usize,
    stations: usize,
    ticks: usize,
    baseline: Baseline,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            entities: 2_000,
            clients: 100,
            stations: 4,
            ticks: 5,
            baseline: Baseline::SectorSync,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct BenchStats {
    updates: usize,
    estimated_payload_bytes: usize,
    encoded_packets: usize,
    encoded_bytes: usize,
    commands_enqueued: usize,
    commands_applied: usize,
    command_latency_ticks_total: u64,
    command_latency_ticks_max: u64,
    command_queue_max: usize,
    max_cell_entities: usize,
    warm_stations: usize,
    hotspot_stations: usize,
    split_candidate_cells: usize,
    tick_ms: Vec<f64>,
}

fn main() {
    let config = BenchConfig::from_args(env::args().skip(1));
    let start = Instant::now();
    let stats = run(config);
    let elapsed = start.elapsed();

    println!("SectorSync benchmark");
    println!("baseline={:?}", config.baseline);
    println!("entities={}", config.entities);
    println!("clients={}", config.clients);
    println!("stations={}", config.stations);
    println!("ticks={}", config.ticks);
    println!("updates={}", stats.updates);
    println!("estimated_payload_bytes={}", stats.estimated_payload_bytes);
    println!("encoded_packets={}", stats.encoded_packets);
    println!("encoded_bytes={}", stats.encoded_bytes);
    println!("commands_enqueued={}", stats.commands_enqueued);
    println!("commands_applied={}", stats.commands_applied);
    println!(
        "command_latency_ticks_avg={:.3}",
        stats.command_latency_ticks_avg()
    );
    println!(
        "command_latency_ticks_max={}",
        stats.command_latency_ticks_max
    );
    println!("command_queue_max={}", stats.command_queue_max);
    println!("max_cell_entities={}", stats.max_cell_entities);
    println!("warm_stations={}", stats.warm_stations);
    println!("hotspot_stations={}", stats.hotspot_stations);
    println!("split_candidate_cells={}", stats.split_candidate_cells);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
}

impl BenchStats {
    fn command_latency_ticks_avg(&self) -> f64 {
        if self.commands_applied == 0 {
            0.0
        } else {
            self.command_latency_ticks_total as f64 / self.commands_applied as f64
        }
    }
}

impl BenchConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let mut config = Self::default();
        for arg in args {
            if let Some(value) = arg.strip_prefix("--entities=") {
                config.entities = value.parse().unwrap_or(config.entities);
            } else if let Some(value) = arg.strip_prefix("--clients=") {
                config.clients = value.parse().unwrap_or(config.clients);
            } else if let Some(value) = arg.strip_prefix("--stations=") {
                config.stations = value.parse().unwrap_or(config.stations).max(1);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            } else if let Some(value) = arg.strip_prefix("--baseline=") {
                config.baseline = match value {
                    "full" => Baseline::FullBroadcast,
                    "room" => Baseline::RoomBroadcast,
                    "naive-grid" => Baseline::NaiveGrid,
                    "sectorsync" => Baseline::SectorSync,
                    _ => config.baseline,
                };
            } else if let Some(value) = arg.strip_prefix("--profile=") {
                match value {
                    "smoke" => config = Self::default(),
                    "medium" => {
                        config.entities = 50_000;
                        config.clients = 1_000;
                        config.stations = 16;
                        config.ticks = 10;
                    }
                    "large" => {
                        config.entities = 1_000_000;
                        config.clients = 10_000;
                        config.stations = 64;
                        config.ticks = 20;
                    }
                    _ => {}
                }
            }
        }
        config
    }
}

fn run(config: BenchConfig) -> BenchStats {
    let mut stations = create_stations(config.stations);
    let mut indexes = create_indexes(config.stations);
    let policies = create_policies();
    populate_entities(config.entities, &mut stations, &mut indexes);
    let clients = create_clients(config.clients);

    let mut stats = BenchStats::default();
    apply_hotspot_report(&mut stats, config, &stations, &indexes);
    let mut encoder = BinaryFrameEncoder;
    let mut transport = FakeTransport::default();
    let mut command_queues = create_command_queues(config.stations);
    let mut next_command_id = 1_u64;

    for tick_index in 0..config.ticks {
        let tick_start = Instant::now();
        for station in &mut stations {
            station.advance_tick();
        }

        enqueue_commands(
            config,
            tick_index,
            &clients,
            &mut command_queues,
            &mut next_command_id,
            &mut stats,
        );

        for (client_index, viewer_position) in clients.iter().copied().enumerate() {
            let station_index = client_index % stations.len();
            let station = &stations[station_index];
            let updates = match config.baseline {
                Baseline::FullBroadcast => config.entities,
                Baseline::RoomBroadcast => config.entities / stations.len(),
                Baseline::NaiveGrid => indexes[station_index]
                    .query_sphere(viewer_position, 256.0)
                    .len(),
                Baseline::SectorSync => {
                    let viewer = ViewerQuery {
                        client_id: ClientId::new(client_index as u64),
                        position: viewer_position,
                        radius: 256.0,
                        max_entities: 300,
                    };
                    let plan = ReplicationPlanner::plan_for_viewer(
                        station,
                        &indexes[station_index],
                        &policies,
                        &viewer,
                        &RangeOnlyVisibility,
                        ReplicationBudget::default(),
                    );
                    plan.stats.selected
                }
            };

            stats.updates += updates;
            stats.estimated_payload_bytes += updates * 32;

            let frame = ReplicationFrame {
                client_id: ClientId::new(client_index as u64),
                server_tick: station.tick(),
                entity_count: updates.min(u32::MAX as usize) as u32,
                estimated_payload_bytes: (updates * 32).min(u32::MAX as usize) as u32,
            };
            let mut bytes = Vec::with_capacity(32);
            encoder
                .encode_replication(&frame, &mut bytes)
                .expect("binary encoder is infallible");
            transport
                .send(OutboundPacket {
                    client_id: frame.client_id,
                    bytes,
                })
                .expect("fake transport is infallible");
        }

        apply_commands(&mut command_queues, &stations, &mut stats);
        stats
            .tick_ms
            .push(tick_start.elapsed().as_secs_f64() * 1000.0);
    }

    stats.encoded_packets = transport.packets_sent();
    stats.encoded_bytes = transport.bytes_sent();
    stats
}

fn percentile_ms(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn create_stations(count: usize) -> Vec<Station> {
    (0..count)
        .map(|index| {
            Station::new(StationConfig {
                station_id: StationId::new(index as u32),
                node_id: NodeId::new((index % 4) as u32),
                instance_id: InstanceId::new(1),
                tick_rate_hz: 20,
            })
        })
        .collect()
}

fn create_indexes(count: usize) -> Vec<CellIndex> {
    let grid = GridSpec::new(64.0).expect("cell size is valid");
    (0..count).map(|_| CellIndex::new(grid)).collect()
}

fn create_policies() -> PolicyTable {
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(0), 1, 20, 256.0));
    policies
}

fn create_command_queues(count: usize) -> Vec<CommandQueues> {
    (0..count)
        .map(|_| {
            CommandQueues::new(CommandQueueLimits {
                high: 1024,
                normal: 16_384,
                low: 4096,
            })
        })
        .collect()
}

fn enqueue_commands(
    config: BenchConfig,
    tick_index: usize,
    clients: &[Position3],
    command_queues: &mut [CommandQueues],
    next_command_id: &mut u64,
    stats: &mut BenchStats,
) {
    let stride = match config.baseline {
        Baseline::FullBroadcast => 4,
        Baseline::RoomBroadcast | Baseline::NaiveGrid | Baseline::SectorSync => 2,
    };
    let command_tick = Tick::new(tick_index as u64);

    for client_index in (0..clients.len()).step_by(stride) {
        let station_index = client_index % command_queues.len();
        let command_id = *next_command_id;
        *next_command_id = next_command_id.saturating_add(1);
        let command = CommandEnvelope {
            id: CommandId::new(command_id),
            client_id: ClientId::new(client_index as u64),
            entity_id: EntityId::new((client_index % config.entities.max(1)) as u64),
            sequence: command_id,
            received_at: command_tick,
            kind: 1,
            priority: if client_index % 16 == 0 {
                CommandPriority::High
            } else {
                CommandPriority::Normal
            },
            payload: Vec::new(),
        };
        if command_queues[station_index]
            .push(command, CommandIngress::RUNNING)
            .is_ok()
        {
            stats.commands_enqueued += 1;
        }
        stats.command_queue_max = stats
            .command_queue_max
            .max(command_queues[station_index].total_len());
    }
}

fn apply_commands(
    command_queues: &mut [CommandQueues],
    stations: &[Station],
    stats: &mut BenchStats,
) {
    for (queue, station) in command_queues.iter_mut().zip(stations) {
        while let Some(command) = queue.pop_next() {
            let latency = station
                .tick()
                .get()
                .saturating_sub(command.received_at.get());
            stats.command_latency_ticks_total =
                stats.command_latency_ticks_total.saturating_add(latency);
            stats.command_latency_ticks_max = stats.command_latency_ticks_max.max(latency);
            stats.commands_applied += 1;
        }
    }
}

fn populate_entities(count: usize, stations: &mut [Station], indexes: &mut [CellIndex]) {
    let mut rng = Lcg::new(0x5E_C7_0C);
    for entity_index in 0..count {
        let station_index = entity_index % stations.len();
        let position = Position3::new(
            rng.next_range(-5_000.0, 5_000.0),
            rng.next_range(-500.0, 500.0),
            rng.next_range(-5_000.0, 5_000.0),
        );
        let bounds = if entity_index % 97 == 0 {
            Bounds::Aabb {
                half_extents: Vec3::new(8.0, 4.0, 8.0),
            }
        } else {
            Bounds::Point
        };
        let handle = stations[station_index]
            .spawn_owned(
                EntityId::new(entity_index as u64),
                position,
                bounds,
                PolicyId::new(0),
            )
            .expect("entity ids are unique");
        indexes[station_index].upsert(handle, position, bounds);
    }
}

fn apply_hotspot_report(
    stats: &mut BenchStats,
    config: BenchConfig,
    stations: &[Station],
    indexes: &[CellIndex],
) {
    let thresholds = hotspot_thresholds(config);
    let subscribers_per_station = config.clients.div_ceil(config.stations);

    for (station, index) in stations.iter().zip(indexes) {
        let cells = index
            .cell_occupancy()
            .into_iter()
            .map(|occupancy| {
                stats.max_cell_entities = stats.max_cell_entities.max(occupancy.entities);
                CellLoadSample {
                    cell: occupancy.cell,
                    owned_entities: occupancy.entities,
                    estimated_updates: occupancy.entities,
                    estimated_bytes: occupancy.entities * 32,
                    subscribers: subscribers_per_station,
                    ..CellLoadSample::default()
                }
            })
            .collect::<Vec<_>>();

        let owned_entities = station.iter().filter(|entity| entity.is_owned()).count();
        let sample = StationLoadSample {
            station_id: station.config().station_id,
            owned_entities,
            subscribers: subscribers_per_station,
            estimated_bytes: owned_entities * subscribers_per_station * 32,
            tick_cost_units: owned_entities as u64,
            cells,
            ..StationLoadSample::default()
        };

        let decision = HotspotPlanner::evaluate(&sample, thresholds);
        match decision.severity {
            HotspotSeverity::Normal => {}
            HotspotSeverity::Warm => {
                stats.warm_stations += 1;
                stats.split_candidate_cells += HotspotPlanner::propose_cell_split(&sample, 2)
                    .cells_to_move
                    .len();
            }
            HotspotSeverity::Hot => {
                stats.hotspot_stations += 1;
                stats.split_candidate_cells += HotspotPlanner::propose_cell_split(&sample, 4)
                    .cells_to_move
                    .len();
            }
        }
    }
}

fn hotspot_thresholds(config: BenchConfig) -> HotspotThresholds {
    let average_entities = config.entities.div_ceil(config.stations).max(1);
    let average_subscribers = config.clients.div_ceil(config.stations).max(1);
    HotspotThresholds {
        max_station_entities: average_entities + average_entities / 2,
        max_station_subscribers: average_subscribers + average_subscribers / 2,
        max_estimated_bytes: average_entities * average_subscribers * 48,
        max_tick_cost_units: (average_entities as u64).saturating_mul(2),
        max_cell_pressure: 512,
        ..HotspotThresholds::default()
    }
}

fn create_clients(count: usize) -> Vec<Position3> {
    let mut rng = Lcg::new(0xC1_13_17);
    (0..count)
        .map(|_| {
            Position3::new(
                rng.next_range(-5_000.0, 5_000.0),
                rng.next_range(-500.0, 500.0),
                rng.next_range(-5_000.0, 5_000.0),
            )
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
struct Lcg {
    state: u64,
}

impl Lcg {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (self.state >> 32) as u32
    }

    fn next_range(&mut self, min: f32, max: f32) -> f32 {
        let unit = self.next_u32() as f32 / u32::MAX as f32;
        min + (max - min) * unit
    }
}
