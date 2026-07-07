//! Lightweight benchmark entry point.

use std::env;
use std::time::Instant;

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId,
    PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget, ReplicationPlanner,
    Station, StationConfig, StationId, Vec3, ViewerQuery,
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

#[derive(Clone, Copy, Debug, Default)]
struct BenchStats {
    updates: usize,
    estimated_payload_bytes: usize,
    encoded_packets: usize,
    encoded_bytes: usize,
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
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
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
    let mut encoder = BinaryFrameEncoder;
    let mut transport = FakeTransport::default();

    for _ in 0..config.ticks {
        for station in &mut stations {
            station.advance_tick();
        }

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
    }

    stats.encoded_packets = transport.packets_sent();
    stats.encoded_bytes = transport.bytes_sent();
    stats
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
