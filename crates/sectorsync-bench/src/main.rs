//! Lightweight benchmark entry point.

use std::collections::BTreeMap;
use std::env;
use std::time::Instant;

use sectorsync_core::prelude::{
    Bounds, CellIndex, CellLoadSample, CellQueryStrategy, ClientId, CommandEnvelope, CommandId,
    CommandIngress, CommandPriority, CommandQueueLimits, CommandQueues, CompiledSyncPolicy,
    ComponentId, EntityId, EventId, EventKind, EventPriority, EventQueueLimits, GatewayConfig,
    GatewaySessionTable, GridSpec, HotspotPlanner, HotspotSeverity, HotspotThresholds, InstanceId,
    NodeId, OwnerEpoch, PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget,
    ReplicationPlanner, ReplicationScratch, Station, StationConfig, StationEvent, StationId,
    StationLoadSample, Tick, Vec3, ViewerQuery,
};
use sectorsync_runtime::{
    ClientTransportBridge, ClientTransportConfig, CommandDispatchTransportBridge, DeploymentConfig,
    DeploymentRouteTable, EventRouter, GatewayClientTransportBridge, GatewayCommandPipeline,
    StationScheduleConfig, StationScheduler, StationSet,
};
use sectorsync_transport::{
    ClientTransportLimits, FakeTransport, InMemoryStationTransport, InMemoryTransportEndpoint,
    InMemoryTransportHub, OutboundPacket, StationTransportLimits, TransportSink,
};
use sectorsync_wire::{
    BinaryFrameEncoder, CommandFrame, ComponentDelta, EntityDelta, FrameEncoder, ReplicationFrame,
};

const DEFAULT_GUARD_MAX_ENTITIES: usize = 4_000;
const DEFAULT_GUARD_MAX_CLIENTS: usize = 150;
const DEFAULT_GUARD_MAX_STATIONS: usize = 8;
const DEFAULT_GUARD_MAX_TICKS: usize = 5;
const DISPATCH_BENCH_MAX_COMMANDS_PER_TICK: usize = 32;
const CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK: usize = 32;

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
    requested_profile: &'static str,
    profile_name: &'static str,
    allow_heavy: bool,
    heavy_profile_denied: bool,
    default_resource_guard_applied: bool,
    host_parallelism: usize,
    thresholds: BenchThresholds,
}

#[derive(Clone, Copy, Debug)]
struct BenchThresholds {
    tick_ms_p99: f64,
    command_latency_ticks_max: u64,
    command_queue_max: usize,
    command_queue_drops_max: usize,
    router_event_drops_max: usize,
    estimated_payload_bytes: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            entities: 2_000,
            clients: 100,
            stations: 4,
            ticks: 5,
            baseline: Baseline::SectorSync,
            requested_profile: "smoke",
            profile_name: "smoke",
            allow_heavy: false,
            heavy_profile_denied: false,
            default_resource_guard_applied: false,
            host_parallelism: 1,
            thresholds: BenchThresholds::default(),
        }
    }
}

impl Default for BenchThresholds {
    fn default() -> Self {
        Self {
            tick_ms_p99: 35.0,
            command_latency_ticks_max: 2,
            command_queue_max: 1024,
            command_queue_drops_max: 0,
            router_event_drops_max: 0,
            estimated_payload_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct BenchStats {
    updates: usize,
    estimated_payload_bytes: usize,
    encoded_packets: usize,
    encoded_bytes: usize,
    payload_entity_deltas: usize,
    payload_component_deltas: usize,
    replication_scratch_queries: usize,
    replication_scratch_candidates: usize,
    replication_scratch_grid_queries: usize,
    replication_scratch_occupied_queries: usize,
    replication_scratch_grid_cells_probed: usize,
    replication_scratch_occupied_cells_scanned: usize,
    replication_scratch_matched_cells: usize,
    replication_scratch_candidate_capacity_max: usize,
    replication_scratch_dedup_capacity_max: usize,
    replication_scratch_matching_cell_capacity_max: usize,
    replication_scratch_priority_capacity_max: usize,
    replication_candidates_selected: usize,
    commands_enqueued: usize,
    commands_applied: usize,
    command_queue_drops: usize,
    gateway_commands_dispatched: usize,
    command_dispatch_packets: usize,
    command_dispatch_bytes: usize,
    command_dispatch_enqueued: usize,
    command_dispatch_applied: usize,
    command_dispatch_latency_ticks_max: u64,
    client_bridge_commands_sent: usize,
    client_bridge_command_bytes: usize,
    client_bridge_gateway_packets_received: usize,
    client_bridge_gateway_commands_accepted: usize,
    client_bridge_gateway_acks_sent: usize,
    client_bridge_gateway_commands_applied: usize,
    client_bridge_packets_received: usize,
    client_bridge_bytes_received: usize,
    client_bridge_acks_received: usize,
    client_bridge_replication_frames_received: usize,
    client_bridge_entities_received: usize,
    client_bridge_components_received: usize,
    command_latency_ticks_total: u64,
    command_latency_ticks_max: u64,
    command_queue_max: usize,
    router_events_routed: usize,
    router_events_drained: usize,
    router_event_drops: usize,
    router_queue_max: usize,
    max_cell_entities: usize,
    warm_stations: usize,
    hotspot_stations: usize,
    split_candidate_cells: usize,
    scheduler_candidates_considered: usize,
    scheduler_stations_selected: usize,
    scheduler_total_advances: usize,
    tick_ms: Vec<f64>,
}

#[allow(clippy::too_many_lines)]
fn main() {
    let config = BenchConfig::from_args(env::args().skip(1));
    let start = Instant::now();
    let stats = run(config);
    let elapsed = start.elapsed();

    println!("SectorSync benchmark");
    println!("baseline={:?}", config.baseline);
    println!("requested_profile={}", config.requested_profile);
    println!("profile={}", config.profile_name);
    println!("allow_heavy={}", config.allow_heavy);
    println!("heavy_profile_denied={}", config.heavy_profile_denied);
    println!(
        "default_resource_guard_applied={}",
        config.default_resource_guard_applied
    );
    println!("host_parallelism={}", config.host_parallelism);
    println!("guard_max_entities={DEFAULT_GUARD_MAX_ENTITIES}");
    println!("guard_max_clients={DEFAULT_GUARD_MAX_CLIENTS}");
    println!("guard_max_stations={DEFAULT_GUARD_MAX_STATIONS}");
    println!("guard_max_ticks={DEFAULT_GUARD_MAX_TICKS}");
    println!("entities={}", config.entities);
    println!("clients={}", config.clients);
    println!("stations={}", config.stations);
    println!("ticks={}", config.ticks);
    println!("updates={}", stats.updates);
    println!("estimated_payload_bytes={}", stats.estimated_payload_bytes);
    println!("encoded_packets={}", stats.encoded_packets);
    println!("encoded_bytes={}", stats.encoded_bytes);
    println!("payload_entity_deltas={}", stats.payload_entity_deltas);
    println!(
        "payload_component_deltas={}",
        stats.payload_component_deltas
    );
    println!(
        "replication_scratch_queries={}",
        stats.replication_scratch_queries
    );
    println!(
        "replication_scratch_candidates={}",
        stats.replication_scratch_candidates
    );
    println!(
        "replication_scratch_grid_queries={}",
        stats.replication_scratch_grid_queries
    );
    println!(
        "replication_scratch_occupied_queries={}",
        stats.replication_scratch_occupied_queries
    );
    println!(
        "replication_scratch_grid_cells_probed={}",
        stats.replication_scratch_grid_cells_probed
    );
    println!(
        "replication_scratch_occupied_cells_scanned={}",
        stats.replication_scratch_occupied_cells_scanned
    );
    println!(
        "replication_scratch_matched_cells={}",
        stats.replication_scratch_matched_cells
    );
    println!(
        "replication_scratch_candidate_capacity_max={}",
        stats.replication_scratch_candidate_capacity_max
    );
    println!(
        "replication_scratch_dedup_capacity_max={}",
        stats.replication_scratch_dedup_capacity_max
    );
    println!(
        "replication_scratch_matching_cell_capacity_max={}",
        stats.replication_scratch_matching_cell_capacity_max
    );
    println!(
        "replication_scratch_priority_capacity_max={}",
        stats.replication_scratch_priority_capacity_max
    );
    println!(
        "replication_candidates_selected={}",
        stats.replication_candidates_selected
    );
    println!("commands_enqueued={}", stats.commands_enqueued);
    println!("commands_applied={}", stats.commands_applied);
    println!("command_queue_drops={}", stats.command_queue_drops);
    println!(
        "gateway_commands_dispatched={}",
        stats.gateway_commands_dispatched
    );
    println!(
        "command_dispatch_packets={}",
        stats.command_dispatch_packets
    );
    println!("command_dispatch_bytes={}", stats.command_dispatch_bytes);
    println!(
        "command_dispatch_enqueued={}",
        stats.command_dispatch_enqueued
    );
    println!(
        "command_dispatch_applied={}",
        stats.command_dispatch_applied
    );
    println!(
        "command_dispatch_latency_ticks_max={}",
        stats.command_dispatch_latency_ticks_max
    );
    println!(
        "client_bridge_commands_sent={}",
        stats.client_bridge_commands_sent
    );
    println!(
        "client_bridge_command_bytes={}",
        stats.client_bridge_command_bytes
    );
    println!(
        "client_bridge_gateway_packets_received={}",
        stats.client_bridge_gateway_packets_received
    );
    println!(
        "client_bridge_gateway_commands_accepted={}",
        stats.client_bridge_gateway_commands_accepted
    );
    println!(
        "client_bridge_gateway_acks_sent={}",
        stats.client_bridge_gateway_acks_sent
    );
    println!(
        "client_bridge_gateway_commands_applied={}",
        stats.client_bridge_gateway_commands_applied
    );
    println!(
        "client_bridge_packets_received={}",
        stats.client_bridge_packets_received
    );
    println!(
        "client_bridge_bytes_received={}",
        stats.client_bridge_bytes_received
    );
    println!(
        "client_bridge_acks_received={}",
        stats.client_bridge_acks_received
    );
    println!(
        "client_bridge_replication_frames_received={}",
        stats.client_bridge_replication_frames_received
    );
    println!(
        "client_bridge_entities_received={}",
        stats.client_bridge_entities_received
    );
    println!(
        "client_bridge_components_received={}",
        stats.client_bridge_components_received
    );
    println!(
        "command_latency_ticks_avg={:.3}",
        stats.command_latency_ticks_avg()
    );
    println!(
        "command_latency_ticks_max={}",
        stats.command_latency_ticks_max
    );
    println!("command_queue_max={}", stats.command_queue_max);
    println!("router_events_routed={}", stats.router_events_routed);
    println!("router_events_drained={}", stats.router_events_drained);
    println!("router_event_drops={}", stats.router_event_drops);
    println!("router_queue_max={}", stats.router_queue_max);
    println!("max_cell_entities={}", stats.max_cell_entities);
    println!("warm_stations={}", stats.warm_stations);
    println!("hotspot_stations={}", stats.hotspot_stations);
    println!("split_candidate_cells={}", stats.split_candidate_cells);
    println!(
        "scheduler_candidates_considered={}",
        stats.scheduler_candidates_considered
    );
    println!(
        "scheduler_stations_selected={}",
        stats.scheduler_stations_selected
    );
    println!(
        "scheduler_total_advances={}",
        stats.scheduler_total_advances
    );
    let tick_ms_p50 = percentile_ms(&stats.tick_ms, 0.50);
    let tick_ms_p95 = percentile_ms(&stats.tick_ms, 0.95);
    let tick_ms_p99 = percentile_ms(&stats.tick_ms, 0.99);
    let tick_ms_max = percentile_ms(&stats.tick_ms, 1.00);
    println!("tick_ms_p50={tick_ms_p50:.3}");
    println!("tick_ms_p95={tick_ms_p95:.3}");
    println!("tick_ms_p99={tick_ms_p99:.3}");
    println!("tick_ms_max={tick_ms_max:.3}");
    let verdict = BenchVerdict::evaluate(config.thresholds, &stats, tick_ms_p99);
    println!("threshold_tick_ms_p99={:.3}", config.thresholds.tick_ms_p99);
    println!(
        "threshold_command_latency_ticks_max={}",
        config.thresholds.command_latency_ticks_max
    );
    println!(
        "threshold_command_queue_max={}",
        config.thresholds.command_queue_max
    );
    println!(
        "threshold_command_queue_drops_max={}",
        config.thresholds.command_queue_drops_max
    );
    println!(
        "threshold_router_event_drops_max={}",
        config.thresholds.router_event_drops_max
    );
    println!(
        "threshold_estimated_payload_bytes={}",
        config.thresholds.estimated_payload_bytes
    );
    println!("threshold_tick_ok={}", verdict.tick_ok);
    println!(
        "threshold_command_latency_ok={}",
        verdict.command_latency_ok
    );
    println!("threshold_command_queue_ok={}", verdict.command_queue_ok);
    println!(
        "threshold_command_queue_drops_ok={}",
        verdict.command_queue_drops_ok
    );
    println!(
        "threshold_router_event_drops_ok={}",
        verdict.router_event_drops_ok
    );
    println!("threshold_payload_ok={}", verdict.payload_ok);
    println!(
        "threshold_command_delivery_ok={}",
        verdict.command_delivery_ok
    );
    println!(
        "threshold_router_delivery_ok={}",
        verdict.router_delivery_ok
    );
    println!("threshold_client_bridge_ok={}", verdict.client_bridge_ok);
    println!("benchmark_ok={}", verdict.is_ok());
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
}

impl BenchStats {
    #[allow(clippy::cast_precision_loss)]
    fn command_latency_ticks_avg(&self) -> f64 {
        if self.commands_applied == 0 {
            0.0
        } else {
            self.command_latency_ticks_total as f64 / self.commands_applied as f64
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct BenchVerdict {
    tick_ok: bool,
    command_latency_ok: bool,
    command_queue_ok: bool,
    command_queue_drops_ok: bool,
    router_event_drops_ok: bool,
    payload_ok: bool,
    command_delivery_ok: bool,
    router_delivery_ok: bool,
    client_bridge_ok: bool,
}

impl BenchVerdict {
    fn evaluate(thresholds: BenchThresholds, stats: &BenchStats, tick_ms_p99: f64) -> Self {
        let expected_client_bridge_frames = stats.client_bridge_commands_sent;
        Self {
            tick_ok: tick_ms_p99 <= thresholds.tick_ms_p99,
            command_latency_ok: stats.command_latency_ticks_max
                <= thresholds.command_latency_ticks_max,
            command_queue_ok: stats.command_queue_max <= thresholds.command_queue_max,
            command_queue_drops_ok: stats.command_queue_drops <= thresholds.command_queue_drops_max,
            router_event_drops_ok: stats.router_event_drops <= thresholds.router_event_drops_max,
            payload_ok: stats.estimated_payload_bytes <= thresholds.estimated_payload_bytes,
            command_delivery_ok: stats.commands_enqueued == stats.commands_applied
                && stats.gateway_commands_dispatched == stats.command_dispatch_packets
                && stats.command_dispatch_packets == stats.command_dispatch_enqueued
                && stats.command_dispatch_enqueued == stats.command_dispatch_applied,
            router_delivery_ok: stats.router_events_routed
                == stats
                    .router_events_drained
                    .saturating_add(stats.router_event_drops),
            client_bridge_ok: stats.client_bridge_gateway_commands_accepted
                == expected_client_bridge_frames
                && stats.client_bridge_gateway_acks_sent == expected_client_bridge_frames
                && stats.client_bridge_gateway_commands_applied == expected_client_bridge_frames
                && stats.client_bridge_acks_received == expected_client_bridge_frames
                && stats.client_bridge_replication_frames_received == expected_client_bridge_frames,
        }
    }

    const fn is_ok(self) -> bool {
        self.tick_ok
            && self.command_latency_ok
            && self.command_queue_ok
            && self.command_queue_drops_ok
            && self.router_event_drops_ok
            && self.payload_ok
            && self.command_delivery_ok
            && self.router_delivery_ok
            && self.client_bridge_ok
    }
}

impl BenchConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        Self::from_args_with_host(args, HostResources::detect())
    }

    fn from_args_with_host(args: impl Iterator<Item = String>, host: HostResources) -> Self {
        let args = args.collect::<Vec<_>>();
        let allow_heavy = args.iter().any(|arg| arg == "--allow-heavy");
        let mut config = Self::smoke(host, allow_heavy);
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
                    "smoke" => {
                        config = Self::smoke(host, allow_heavy);
                        config.requested_profile = "smoke";
                    }
                    "medium" => {
                        config.requested_profile = "medium";
                        if allow_heavy {
                            config.profile_name = "medium";
                            config.entities = 50_000;
                            config.clients = 1_000;
                            config.stations = 16;
                            config.ticks = 10;
                            config.thresholds.tick_ms_p99 = 50.0;
                            config.thresholds.estimated_payload_bytes = 512 * 1024 * 1024;
                        } else {
                            config.heavy_profile_denied = true;
                        }
                    }
                    "large" => {
                        config.requested_profile = "large";
                        if allow_heavy {
                            config.profile_name = "large";
                            config.entities = 1_000_000;
                            config.clients = 10_000;
                            config.stations = 64;
                            config.ticks = 20;
                            config.thresholds.tick_ms_p99 = 100.0;
                            config.thresholds.command_queue_max = 8192;
                            config.thresholds.estimated_payload_bytes = 8 * 1024 * 1024 * 1024;
                        } else {
                            config.heavy_profile_denied = true;
                        }
                    }
                    _ => {}
                }
            } else if let Some(value) = arg.strip_prefix("--tick-ms-p99-budget=") {
                config.thresholds.tick_ms_p99 =
                    value.parse().unwrap_or(config.thresholds.tick_ms_p99);
            } else if let Some(value) = arg.strip_prefix("--command-latency-ticks-budget=") {
                config.thresholds.command_latency_ticks_max = value
                    .parse()
                    .unwrap_or(config.thresholds.command_latency_ticks_max);
            } else if let Some(value) = arg.strip_prefix("--command-queue-budget=") {
                config.thresholds.command_queue_max =
                    value.parse().unwrap_or(config.thresholds.command_queue_max);
            } else if let Some(value) = arg.strip_prefix("--command-queue-drops-budget=") {
                config.thresholds.command_queue_drops_max = value
                    .parse()
                    .unwrap_or(config.thresholds.command_queue_drops_max);
            } else if let Some(value) = arg.strip_prefix("--router-event-drops-budget=") {
                config.thresholds.router_event_drops_max = value
                    .parse()
                    .unwrap_or(config.thresholds.router_event_drops_max);
            } else if let Some(value) = arg.strip_prefix("--payload-bytes-budget=") {
                config.thresholds.estimated_payload_bytes = value
                    .parse()
                    .unwrap_or(config.thresholds.estimated_payload_bytes);
            }
        }
        config.apply_default_resource_guard();
        config
    }

    fn smoke(host: HostResources, allow_heavy: bool) -> Self {
        Self {
            allow_heavy,
            host_parallelism: host.parallelism,
            ..Self::default()
        }
    }

    fn apply_default_resource_guard(&mut self) {
        if self.allow_heavy {
            return;
        }

        let before = (self.entities, self.clients, self.stations, self.ticks);
        self.entities = self.entities.min(DEFAULT_GUARD_MAX_ENTITIES);
        self.clients = self.clients.min(DEFAULT_GUARD_MAX_CLIENTS);
        self.stations = self.stations.clamp(1, DEFAULT_GUARD_MAX_STATIONS);
        self.ticks = self.ticks.min(DEFAULT_GUARD_MAX_TICKS);
        self.default_resource_guard_applied =
            before != (self.entities, self.clients, self.stations, self.ticks);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HostResources {
    parallelism: usize,
}

impl HostResources {
    fn detect() -> Self {
        Self {
            parallelism: std::thread::available_parallelism()
                .map_or(1, std::num::NonZeroUsize::get),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run(config: BenchConfig) -> BenchStats {
    let mut stations = create_stations(config.stations);
    let mut indexes = create_indexes(config.stations);
    let policies = create_policies();
    populate_entities(config.entities, &mut stations, &mut indexes);
    let clients = create_clients(config.clients);

    let mut stats = BenchStats::default();
    let load_samples = apply_hotspot_report(&mut stats, config, &stations, &indexes);
    apply_scheduler_report(&mut stats, config, &load_samples);
    let mut encoder = BinaryFrameEncoder;
    let mut transport = FakeTransport::default();
    let mut command_queues = create_command_queues(config.stations);
    let mut dispatch = DispatchBench::new(config);
    let mut client_bridge = ClientBridgeBench::new(config);
    let mut replication_scratch = ReplicationScratch::default();
    let mut next_command_id = 1_u64;
    let mut next_event_id = 1_u64;
    let mut event_router = EventRouter::new(EventQueueLimits {
        critical: 8,
        important: 32,
        best_effort: 64,
    });
    for station in &stations {
        event_router.register_station(station.config().station_id);
    }

    for tick_index in 0..config.ticks {
        let tick_start = Instant::now();
        for station in &mut stations {
            station.advance_tick();
        }
        exercise_event_router(&stations, &mut event_router, &mut next_event_id, &mut stats);

        enqueue_commands(
            config,
            tick_index,
            &clients,
            &mut command_queues,
            &mut next_command_id,
            &mut stats,
        );
        dispatch_gateway_commands(
            config,
            tick_index,
            &clients,
            &mut dispatch,
            &mut next_command_id,
            &mut stats,
        );
        exercise_client_bridge(
            config,
            tick_index,
            &mut client_bridge,
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
                    let plan = ReplicationPlanner::plan_for_viewer_with_scratch(
                        station,
                        &indexes[station_index],
                        &policies,
                        &viewer,
                        &RangeOnlyVisibility,
                        ReplicationBudget::default(),
                        &mut replication_scratch,
                    );
                    stats.replication_scratch_queries =
                        stats.replication_scratch_queries.saturating_add(1);
                    stats.replication_scratch_candidates = stats
                        .replication_scratch_candidates
                        .saturating_add(replication_scratch.candidate_count());
                    let query_stats = replication_scratch.query_stats();
                    match query_stats.strategy {
                        CellQueryStrategy::Grid => {
                            stats.replication_scratch_grid_queries =
                                stats.replication_scratch_grid_queries.saturating_add(1);
                        }
                        CellQueryStrategy::OccupiedCells => {
                            stats.replication_scratch_occupied_queries =
                                stats.replication_scratch_occupied_queries.saturating_add(1);
                        }
                    }
                    stats.replication_scratch_grid_cells_probed = stats
                        .replication_scratch_grid_cells_probed
                        .saturating_add(query_stats.grid_cells_probed);
                    stats.replication_scratch_occupied_cells_scanned = stats
                        .replication_scratch_occupied_cells_scanned
                        .saturating_add(query_stats.occupied_cells_scanned);
                    stats.replication_scratch_matched_cells = stats
                        .replication_scratch_matched_cells
                        .saturating_add(query_stats.matched_cells);
                    stats.replication_scratch_candidate_capacity_max = stats
                        .replication_scratch_candidate_capacity_max
                        .max(replication_scratch.candidate_capacity());
                    stats.replication_scratch_dedup_capacity_max = stats
                        .replication_scratch_dedup_capacity_max
                        .max(replication_scratch.candidate_dedup_capacity());
                    stats.replication_scratch_matching_cell_capacity_max = stats
                        .replication_scratch_matching_cell_capacity_max
                        .max(replication_scratch.matching_cell_capacity());
                    stats.replication_scratch_priority_capacity_max = stats
                        .replication_scratch_priority_capacity_max
                        .max(replication_scratch.prioritized_capacity());
                    plan.stats.selected
                }
            };

            stats.updates += updates;
            stats.replication_candidates_selected = stats
                .replication_candidates_selected
                .saturating_add(updates);
            stats.estimated_payload_bytes += updates * 32;
            let entity_deltas = build_sample_deltas(updates, client_index, station.tick());
            stats.payload_entity_deltas += entity_deltas.len();
            stats.payload_component_deltas += entity_deltas
                .iter()
                .map(|delta| delta.components.len())
                .sum::<usize>();

            let frame = ReplicationFrame {
                client_id: ClientId::new(client_index as u64),
                server_tick: station.tick(),
                entity_count: u32::try_from(updates).unwrap_or(u32::MAX),
                estimated_payload_bytes: u32::try_from(updates.saturating_mul(32))
                    .unwrap_or(u32::MAX),
                entities: entity_deltas,
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
    let router_stats = event_router.stats();
    stats.router_events_routed = router_stats.routed_events;
    stats.router_events_drained = router_stats.drained_events;
    stats.router_event_drops = router_stats.dropped_best_effort_events;
    stats
}

fn build_sample_deltas(update_count: usize, client_index: usize, tick: Tick) -> Vec<EntityDelta> {
    let sample_count = update_count.min(16);
    (0..sample_count)
        .map(|offset| {
            let entity_id = EntityId::new(((client_index * 31) + offset) as u64);
            let tick_value = u32::try_from(tick.get()).unwrap_or(u32::MAX);
            EntityDelta {
                entity_id,
                owner_epoch: OwnerEpoch::new(0),
                components: vec![ComponentDelta {
                    component_id: ComponentId::new(1),
                    version: tick.get(),
                    flags: 0,
                    bytes: tick_value.to_le_bytes().to_vec(),
                }],
            }
        })
        .collect()
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

fn create_stations(count: usize) -> Vec<Station> {
    (0..count)
        .map(|index| {
            Station::new(StationConfig {
                station_id: StationId::new(
                    u32::try_from(index).expect("station count must fit in u32"),
                ),
                node_id: NodeId::new(u32::try_from(index % 4).expect("node shard must fit in u32")),
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

struct DispatchBench {
    gateway: GatewaySessionTable,
    deployment: DeploymentRouteTable,
    station_transport: InMemoryStationTransport,
    station_queues: BTreeMap<StationId, CommandQueues>,
    pipeline: GatewayCommandPipeline,
    bridge: CommandDispatchTransportBridge,
}

impl DispatchBench {
    fn new(config: BenchConfig) -> Self {
        let mut gateway = GatewaySessionTable::new(GatewayConfig {
            max_sessions: config.clients.max(1),
            reconnect_grace_ticks: 20,
            max_commands_per_tick: DISPATCH_BENCH_MAX_COMMANDS_PER_TICK,
        });
        let mut deployment = DeploymentRouteTable::new(DeploymentConfig {
            max_nodes: config.stations.max(1),
            max_stations_per_node: config.stations.max(1),
            stale_after_ticks: 20,
        });
        let mut station_transport = InMemoryStationTransport::new(StationTransportLimits {
            max_queued_packets_per_station: DISPATCH_BENCH_MAX_COMMANDS_PER_TICK * 2,
            max_packet_bytes: 512,
        });
        let mut station_queues = BTreeMap::new();

        for station_index in 0..config.stations {
            let numeric_id = u32::try_from(station_index).expect("station count must fit in u32");
            let station_id = StationId::new(numeric_id);
            let node_id = NodeId::new(numeric_id);
            deployment
                .register_node(node_id, 1, Tick::new(0))
                .expect("benchmark deployment node should register");
            deployment
                .assign_station(station_id, node_id, Tick::new(0))
                .expect("benchmark station should assign");
            station_transport.register_station(station_id);
            station_queues.insert(
                station_id,
                CommandQueues::new(CommandQueueLimits {
                    high: 256,
                    normal: 1024,
                    low: 256,
                }),
            );
        }

        for client_index in 0..config.clients {
            let station_id = StationId::new(
                u32::try_from(client_index % config.stations.max(1))
                    .expect("station count must fit in u32"),
            );
            gateway
                .connect(ClientId::new(client_index as u64), station_id, Tick::new(0))
                .expect("benchmark client should connect");
        }

        Self {
            gateway,
            deployment,
            station_transport,
            station_queues,
            pipeline: GatewayCommandPipeline::default(),
            bridge: CommandDispatchTransportBridge::default(),
        }
    }
}

struct ClientBridgeBench {
    server_transport: InMemoryTransportEndpoint,
    client_endpoints: Vec<InMemoryTransportEndpoint>,
    client_bridges: Vec<ClientTransportBridge>,
    gateway: GatewaySessionTable,
    station_queues: BTreeMap<StationId, CommandQueues>,
    pipeline: GatewayCommandPipeline,
    gateway_bridge: GatewayClientTransportBridge,
}

impl ClientBridgeBench {
    fn new(config: BenchConfig) -> Self {
        let server_id = ClientId::new(u64::MAX);
        let sampled_clients = config.clients.min(CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK);
        let station_id = StationId::new(0);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK * 2,
            max_packet_bytes: 1024,
        });
        let server_transport = hub
            .endpoint(server_id, "127.0.0.1:25000".parse().expect("server addr"))
            .expect("benchmark server endpoint should register");
        let mut client_endpoints = Vec::with_capacity(sampled_clients);
        let mut client_bridges = Vec::with_capacity(sampled_clients);

        for client_index in 0..sampled_clients {
            let client_id = ClientId::new(client_index as u64);
            let endpoint = hub
                .endpoint(
                    client_id,
                    format!("127.0.0.1:{}", 25001 + client_index)
                        .parse()
                        .expect("client addr"),
                )
                .expect("benchmark client endpoint should register");
            client_endpoints.push(endpoint);
            client_bridges.push(ClientTransportBridge::new(
                ClientTransportConfig::new(client_id, server_id).with_expected_source(server_id),
            ));
        }
        let mut gateway = GatewaySessionTable::new(GatewayConfig {
            max_sessions: sampled_clients.max(1),
            reconnect_grace_ticks: 20,
            max_commands_per_tick: CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK,
        });
        for client_index in 0..sampled_clients {
            gateway
                .connect(ClientId::new(client_index as u64), station_id, Tick::new(0))
                .expect("benchmark client bridge gateway session should connect");
        }
        let station_queues = BTreeMap::from([(
            station_id,
            CommandQueues::new(CommandQueueLimits {
                high: CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK,
                normal: CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK * 4,
                low: CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK,
            }),
        )]);

        Self {
            server_transport,
            client_endpoints,
            client_bridges,
            gateway,
            station_queues,
            pipeline: GatewayCommandPipeline::default(),
            gateway_bridge: GatewayClientTransportBridge::default(),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn exercise_client_bridge(
    config: BenchConfig,
    tick_index: usize,
    bench: &mut ClientBridgeBench,
    next_command_id: &mut u64,
    stats: &mut BenchStats,
) {
    let command_count = bench
        .client_endpoints
        .len()
        .min(CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK);
    if command_count == 0 {
        return;
    }

    let tick = Tick::new(tick_index as u64);
    for client_index in 0..command_count {
        let command_id = *next_command_id;
        *next_command_id = next_command_id.saturating_add(1);
        let frame = CommandFrame {
            client_id: ClientId::new(client_index as u64),
            command_id: CommandId::new(command_id),
            entity_id: EntityId::new((client_index % config.entities.max(1)) as u64),
            sequence: command_id,
            kind: 1,
            priority: if client_index % 16 == 0 {
                CommandPriority::High
            } else {
                CommandPriority::Normal
            },
            payload: Vec::new(),
        };
        let report = bench.client_bridges[client_index]
            .send_command_frame(&mut bench.client_endpoints[client_index], &frame)
            .expect("benchmark client bridge command should send");
        stats.client_bridge_commands_sent = stats.client_bridge_commands_sent.saturating_add(1);
        stats.client_bridge_command_bytes = stats
            .client_bridge_command_bytes
            .saturating_add(report.bytes_sent);
    }

    let gateway_pump = bench
        .gateway_bridge
        .pump_ingress(
            &mut bench.server_transport,
            &mut bench.pipeline,
            &mut bench.gateway,
            &mut bench.station_queues,
            tick,
            CommandIngress::RUNNING,
            command_count,
        )
        .expect("benchmark gateway client bridge should pump");
    stats.client_bridge_gateway_packets_received = stats
        .client_bridge_gateway_packets_received
        .saturating_add(gateway_pump.packets_received);
    stats.client_bridge_gateway_commands_accepted = stats
        .client_bridge_gateway_commands_accepted
        .saturating_add(gateway_pump.commands_accepted());
    stats.client_bridge_gateway_acks_sent = stats
        .client_bridge_gateway_acks_sent
        .saturating_add(gateway_pump.acks_sent);

    for queue in bench.station_queues.values_mut() {
        while queue.pop_next().is_some() {
            stats.client_bridge_gateway_commands_applied = stats
                .client_bridge_gateway_commands_applied
                .saturating_add(1);
        }
    }

    let mut encoder = BinaryFrameEncoder;
    for client_index in 0..command_count {
        let client_id = ClientId::new(client_index as u64);
        let replication = ReplicationFrame {
            client_id,
            server_tick: tick,
            entity_count: 1,
            estimated_payload_bytes: 4,
            entities: build_sample_deltas(1, client_index, tick),
        };
        let mut replication_bytes = Vec::new();
        encoder
            .encode_replication(&replication, &mut replication_bytes)
            .expect("benchmark replication should encode");
        bench
            .server_transport
            .send(OutboundPacket {
                client_id,
                bytes: replication_bytes,
            })
            .expect("benchmark replication should send");
    }

    for client_index in 0..command_count {
        let pump = bench.client_bridges[client_index]
            .pump(&mut bench.client_endpoints[client_index], 4)
            .expect("benchmark client bridge should pump");
        stats.client_bridge_packets_received = stats
            .client_bridge_packets_received
            .saturating_add(pump.packets_received);
        stats.client_bridge_bytes_received = stats
            .client_bridge_bytes_received
            .saturating_add(pump.bytes_received);
        stats.client_bridge_acks_received = stats
            .client_bridge_acks_received
            .saturating_add(pump.command_acks_received());
        stats.client_bridge_replication_frames_received = stats
            .client_bridge_replication_frames_received
            .saturating_add(pump.replication_frames_received());
        stats.client_bridge_entities_received = stats
            .client_bridge_entities_received
            .saturating_add(pump.entities_received());
        stats.client_bridge_components_received = stats
            .client_bridge_components_received
            .saturating_add(pump.components_received());
    }
}

fn dispatch_gateway_commands(
    config: BenchConfig,
    tick_index: usize,
    clients: &[Position3],
    dispatch: &mut DispatchBench,
    next_command_id: &mut u64,
    stats: &mut BenchStats,
) {
    if clients.is_empty() {
        return;
    }

    let command_count = clients.len().min(DISPATCH_BENCH_MAX_COMMANDS_PER_TICK);
    let stride = clients.len().div_ceil(command_count).max(1);
    let tick = Tick::new(tick_index as u64);
    let mut encoder = BinaryFrameEncoder;

    for client_index in (0..clients.len()).step_by(stride).take(command_count) {
        let command_id = *next_command_id;
        *next_command_id = next_command_id.saturating_add(1);
        let frame = CommandFrame {
            client_id: ClientId::new(client_index as u64),
            command_id: CommandId::new(command_id),
            entity_id: EntityId::new((client_index % config.entities.max(1)) as u64),
            sequence: command_id,
            kind: 1,
            priority: if client_index % 16 == 0 {
                CommandPriority::High
            } else {
                CommandPriority::Normal
            },
            payload: Vec::new(),
        };
        let mut bytes = Vec::new();
        encoder
            .encode_command(&frame, &mut bytes)
            .expect("benchmark command should encode");
        let report =
            dispatch
                .pipeline
                .dispatch(&mut dispatch.gateway, &dispatch.deployment, &bytes, tick);
        if !report.accepted {
            continue;
        }
        let delivery = report
            .delivery
            .expect("accepted dispatch should include delivery route");
        let command = report
            .command
            .expect("accepted dispatch should include stamped command");
        dispatch
            .bridge
            .send_envelope(
                &mut dispatch.station_transport,
                StationId::new(0),
                delivery.station_id,
                &command,
            )
            .expect("benchmark command dispatch should send");
        stats.gateway_commands_dispatched = stats.gateway_commands_dispatched.saturating_add(1);
    }

    for station_index in 0..config.stations {
        let station_id =
            StationId::new(u32::try_from(station_index).expect("station count must fit in u32"));
        let pump = dispatch
            .bridge
            .pump_target(
                &mut dispatch.station_transport,
                &mut dispatch.station_queues,
                station_id,
                DISPATCH_BENCH_MAX_COMMANDS_PER_TICK,
                CommandIngress::RUNNING,
            )
            .expect("benchmark command dispatch should pump");
        stats.command_dispatch_packets = stats
            .command_dispatch_packets
            .saturating_add(pump.packets_received);
        stats.command_dispatch_bytes = stats
            .command_dispatch_bytes
            .saturating_add(pump.bytes_received);
        stats.command_dispatch_enqueued = stats
            .command_dispatch_enqueued
            .saturating_add(pump.commands_enqueued);
    }

    for queue in dispatch.station_queues.values_mut() {
        stats.command_queue_max = stats.command_queue_max.max(queue.total_len());
        while let Some(command) = queue.pop_next() {
            let latency = tick.get().saturating_sub(command.received_at.get());
            stats.command_dispatch_latency_ticks_max =
                stats.command_dispatch_latency_ticks_max.max(latency);
            stats.command_dispatch_applied = stats.command_dispatch_applied.saturating_add(1);
        }
    }
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
        match command_queues[station_index].push(command, CommandIngress::RUNNING) {
            Ok(_) => stats.commands_enqueued = stats.commands_enqueued.saturating_add(1),
            Err(_) => {
                stats.command_queue_drops = stats.command_queue_drops.saturating_add(1);
            }
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
) -> Vec<StationLoadSample> {
    let thresholds = hotspot_thresholds(config);
    let subscribers_per_station = config.clients.div_ceil(config.stations);
    let mut samples = Vec::with_capacity(stations.len());

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
        samples.push(sample);
    }

    samples
}

fn apply_scheduler_report(
    stats: &mut BenchStats,
    config: BenchConfig,
    samples: &[StationLoadSample],
) {
    let mut stations = StationSet::default();
    for station in create_stations(config.stations) {
        stations.push(station);
    }
    let plan = StationScheduler::default().plan_loaded(
        &stations,
        samples,
        StationScheduleConfig {
            max_station_advances_per_step: config.stations.min(2),
        },
    );
    stats.scheduler_candidates_considered = plan.candidates_considered;
    stats.scheduler_stations_selected = plan.stations_selected;
    stats.scheduler_total_advances = plan.total_advances;
}

fn exercise_event_router(
    stations: &[Station],
    router: &mut EventRouter,
    next_event_id: &mut u64,
    stats: &mut BenchStats,
) {
    for (station_index, station) in stations.iter().enumerate() {
        let source_index = if station_index == 0 {
            stations.len().saturating_sub(1)
        } else {
            station_index - 1
        };
        let event_id = *next_event_id;
        *next_event_id = next_event_id.saturating_add(1);
        router
            .route(StationEvent {
                id: EventId::new(event_id),
                source: stations[source_index].config().station_id,
                target: station.config().station_id,
                source_tick: station.tick(),
                target_tick: station.tick(),
                priority: EventPriority::BestEffort,
                kind: EventKind::Custom(
                    u32::try_from(event_id % u64::from(u32::MAX))
                        .expect("reduced event id must fit in u32"),
                ),
            })
            .expect("smoke event router should accept bounded event");
    }

    let queued = stations
        .iter()
        .filter_map(|station| router.queued_len(station.config().station_id))
        .sum::<usize>();
    stats.router_queue_max = stats.router_queue_max.max(queued);

    for station in stations {
        router
            .drain_ready(station.config().station_id, station.tick())
            .expect("smoke event router target should remain registered");
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

    #[allow(clippy::cast_precision_loss)]
    fn next_range(&mut self, min: f32, max: f32) -> f32 {
        let unit = self.next_u32() as f32 / u32::MAX as f32;
        min + (max - min) * unit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> std::vec::IntoIter<String> {
        values
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn large_profile_without_allow_heavy_is_denied() {
        let config = BenchConfig::from_args_with_host(
            args(&["--profile=large"]),
            HostResources { parallelism: 2 },
        );

        assert_eq!(config.requested_profile, "large");
        assert_eq!(config.profile_name, "smoke");
        assert!(!config.allow_heavy);
        assert!(config.heavy_profile_denied);
        assert!(!config.default_resource_guard_applied);
        assert_eq!(config.host_parallelism, 2);
    }

    #[test]
    fn custom_scale_is_clamped_without_allow_heavy() {
        let config = BenchConfig::from_args_with_host(
            args(&[
                "--entities=1000000",
                "--clients=100000",
                "--stations=100",
                "--ticks=100",
            ]),
            HostResources { parallelism: 4 },
        );

        assert_eq!(config.entities, DEFAULT_GUARD_MAX_ENTITIES);
        assert_eq!(config.clients, DEFAULT_GUARD_MAX_CLIENTS);
        assert_eq!(config.stations, DEFAULT_GUARD_MAX_STATIONS);
        assert_eq!(config.ticks, DEFAULT_GUARD_MAX_TICKS);
        assert!(config.default_resource_guard_applied);
    }

    #[test]
    fn allow_heavy_admits_large_profile() {
        let config = BenchConfig::from_args_with_host(
            args(&["--profile=large", "--allow-heavy"]),
            HostResources { parallelism: 16 },
        );

        assert_eq!(config.requested_profile, "large");
        assert_eq!(config.profile_name, "large");
        assert!(config.allow_heavy);
        assert!(!config.heavy_profile_denied);
        assert!(!config.default_resource_guard_applied);
        assert_eq!(config.entities, 1_000_000);
        assert_eq!(config.clients, 10_000);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn percentile_reports_requested_tick_latency_cutoffs() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];

        assert_eq!(percentile_ms(&values, 0.50), 3.0);
        assert_eq!(percentile_ms(&values, 0.95), 5.0);
        assert_eq!(percentile_ms(&values, 0.99), 5.0);
    }

    #[test]
    fn drop_thresholds_fail_the_benchmark_verdict() {
        let stats = BenchStats {
            command_queue_drops: 1,
            router_event_drops: 1,
            ..BenchStats::default()
        };

        let verdict = BenchVerdict::evaluate(BenchThresholds::default(), &stats, 0.0);

        assert!(!verdict.command_queue_drops_ok);
        assert!(!verdict.router_event_drops_ok);
        assert!(!verdict.is_ok());
    }

    #[test]
    fn smoke_run_records_acceptance_matrix_signals() {
        let stats = run(BenchConfig {
            entities: 64,
            clients: 8,
            stations: 2,
            ticks: 2,
            ..BenchConfig::default()
        });

        assert_eq!(stats.tick_ms.len(), 2);
        assert!(stats.replication_candidates_selected > 0);
        assert_eq!(
            stats.replication_scratch_grid_queries + stats.replication_scratch_occupied_queries,
            stats.replication_scratch_queries
        );
        assert!(
            stats.replication_scratch_grid_cells_probed
                + stats.replication_scratch_occupied_cells_scanned
                > 0
        );
        assert!(stats.replication_scratch_candidate_capacity_max > 0);
        assert!(stats.replication_scratch_dedup_capacity_max > 0);
        assert_eq!(stats.command_queue_drops, 0);
        assert_eq!(stats.router_events_routed, 4);
        assert_eq!(stats.router_events_drained, 4);
        assert_eq!(stats.router_event_drops, 0);
        assert_eq!(stats.router_queue_max, 2);
        assert_eq!(stats.scheduler_candidates_considered, 2);
        assert_eq!(stats.scheduler_stations_selected, 2);
        assert_eq!(stats.scheduler_total_advances, 2);
    }
}
