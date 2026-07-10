//! Lightweight benchmark entry point.

use std::collections::BTreeMap;
use std::env;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, CellLoadSample, ClientId, CommandEnvelope, CommandId, CommandIngress,
    CommandPriority, CommandQueueLimits, CommandQueues, CompiledSyncPolicy, ComponentId, EntityId,
    EventId, EventKind, EventPriority, EventQueueLimits, GatewayConfig, GatewaySessionTable,
    GridSpec, HotspotPlanner, HotspotSeverity, HotspotThresholds, InstanceId, NodeId, OwnerEpoch,
    PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBatchStats,
    ReplicationBudget, ReplicationPlanner, ReplicationScratch, Station, StationConfig,
    StationEvent, StationId, StationLoadSample, Tick, Vec3, ViewerQuery,
};
use sectorsync_runtime::{
    ClientTransportBridge, ClientTransportConfig, CommandDispatchTransportBridge, DeploymentConfig,
    DeploymentRouteTable, EventRouter, GatewayClientTransportBridge, GatewayCommandPipeline,
    StationScheduleConfig, StationScheduler, StationSet,
};
#[cfg(feature = "parallel")]
use sectorsync_runtime::{ReplicationThreadPool, ReplicationThreadPoolConfig};
use sectorsync_transport::{
    ClientTransportLimits, FakeTransport, InMemoryStationTransport, InMemoryTransportEndpoint,
    InMemoryTransportHub, OutboundPacket, PacketBatch, StationTransportLimits, TransportSink,
};
use sectorsync_wire::{
    BinaryFrameEncoder, CommandFrame, ComponentDelta, EntityDelta, FrameEncoder, ReplicationFrame,
};

const DEFAULT_GUARD_MAX_ENTITIES: usize = 4_000;
const DEFAULT_GUARD_MAX_CLIENTS: usize = 150;
const DEFAULT_GUARD_MAX_STATIONS: usize = 8;
const DEFAULT_GUARD_MAX_TICKS: usize = 5;
const LOCAL_MAX_ENTITIES: usize = 24_000;
const LOCAL_MAX_CLIENTS: usize = 480;
const LOCAL_MAX_STATIONS: usize = 8;
const LOCAL_TICKS: usize = 30;
const LOCAL_TIME_BUDGET_MS: u64 = 10_000;
const SAMPLED_ENTITY_DELTAS_PER_FRAME: usize = 16;
const FULL_ENTITY_DELTAS_PER_FRAME: usize = 300;
const TICK_128_HZ_BUDGET_MS: f64 = 1_000.0 / 128.0;
const DISPATCH_BENCH_MAX_COMMANDS_PER_TICK: usize = 32;
const CLIENT_BRIDGE_BENCH_MAX_CLIENTS_PER_TICK: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Baseline {
    FullBroadcast,
    RoomBroadcast,
    NaiveGrid,
    SectorSync,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PlannerMode {
    #[default]
    Scalar,
    Batch,
    Parallel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PayloadMaterialization {
    Sampled,
    Full,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkloadShape {
    Wide,
    Dense,
}

impl WorkloadShape {
    const fn horizontal_extent(self) -> f32 {
        match self {
            Self::Wide => 5_000.0,
            Self::Dense => 1_000.0,
        }
    }

    const fn vertical_extent(self) -> f32 {
        match self {
            Self::Wide => 500.0,
            Self::Dense => 100.0,
        }
    }

    const fn viewer_radius() -> f32 {
        256.0
    }
}

impl PayloadMaterialization {
    const fn entity_limit(self) -> usize {
        match self {
            Self::Sampled => SAMPLED_ENTITY_DELTAS_PER_FRAME,
            Self::Full => FULL_ENTITY_DELTAS_PER_FRAME,
        }
    }

    const fn requires_full(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    entities: usize,
    clients: usize,
    stations: usize,
    ticks: usize,
    baseline: Baseline,
    requested_planner: PlannerMode,
    planner: PlannerMode,
    requested_parallel_threads: usize,
    tick_rate_hz: u16,
    replication_hz: u16,
    requested_profile: &'static str,
    profile_name: &'static str,
    allow_heavy: bool,
    heavy_profile_denied: bool,
    default_resource_guard_applied: bool,
    host_parallelism: usize,
    payload_materialization: PayloadMaterialization,
    workload_shape: WorkloadShape,
    time_budget_ms: Option<u64>,
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
            requested_planner: PlannerMode::Scalar,
            planner: PlannerMode::Scalar,
            requested_parallel_threads: 0,
            tick_rate_hz: 20,
            replication_hz: 20,
            requested_profile: "smoke",
            profile_name: "smoke",
            allow_heavy: false,
            heavy_profile_denied: false,
            default_resource_guard_applied: false,
            host_parallelism: 1,
            payload_materialization: PayloadMaterialization::Sampled,
            workload_shape: WorkloadShape::Wide,
            time_budget_ms: None,
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
    parallel_threads: usize,
    time_budget_exhausted: bool,
    planning_ms: Vec<f64>,
    encoding_ms: Vec<f64>,
    tick_ms: Vec<f64>,
}

#[derive(Clone, Copy, Debug)]
struct UpdateWork {
    client_index: usize,
    server_tick: Tick,
    updates: usize,
    entity_limit: usize,
}

struct EncodedUpdate {
    packet: OutboundPacket,
    updates: usize,
    entity_deltas: usize,
    component_deltas: usize,
}

#[cfg(feature = "parallel")]
struct StationPipelineWork<'a> {
    station: &'a Station,
    index: &'a CellIndex,
    viewers: &'a [ViewerQuery],
    scratch: &'a mut ReplicationScratch,
}

#[cfg(feature = "parallel")]
struct StationPipelineOutput {
    stats: ReplicationBatchStats,
    encoded: Vec<EncodedUpdate>,
    planning_ms: f64,
    encoding_ms: f64,
}

#[allow(clippy::too_many_lines)]
fn main() {
    let config = BenchConfig::from_args(env::args().skip(1));
    let start = Instant::now();
    let stats = run(config);
    let elapsed = start.elapsed();

    println!("SectorSync benchmark");
    println!("baseline={:?}", config.baseline);
    println!("requested_planner={:?}", config.requested_planner);
    println!("planner={:?}", config.planner);
    println!(
        "planner_mode_denied={}",
        config.requested_planner != config.planner
    );
    println!("simd_enabled={}", cfg!(feature = "simd"));
    println!("parallel_enabled={}", cfg!(feature = "parallel"));
    println!(
        "requested_parallel_threads={}",
        config.requested_parallel_threads
    );
    println!("parallel_threads={}", stats.parallel_threads);
    let replication_phases = config.replication_phase_count();
    println!("tick_rate_hz={}", config.tick_rate_hz);
    println!("requested_replication_hz={}", config.replication_hz);
    println!("replication_phases={replication_phases}");
    println!(
        "effective_replication_hz={:.3}",
        f64::from(config.tick_rate_hz)
            / f64::from(config.tick_rate_hz.div_ceil(config.replication_hz))
    );
    println!("requested_profile={}", config.requested_profile);
    println!("profile={}", config.profile_name);
    println!("allow_heavy={}", config.allow_heavy);
    println!("heavy_profile_denied={}", config.heavy_profile_denied);
    println!(
        "default_resource_guard_applied={}",
        config.default_resource_guard_applied
    );
    println!("host_parallelism={}", config.host_parallelism);
    println!(
        "entity_deltas_per_frame_limit={}",
        config.payload_materialization.entity_limit()
    );
    println!(
        "require_full_payload_materialization={}",
        config.payload_materialization.requires_full()
    );
    println!("workload_shape={:?}", config.workload_shape);
    println!(
        "time_budget_ms={}",
        config.time_budget_ms.map_or(0, |value| value)
    );
    println!("guard_max_entities={DEFAULT_GUARD_MAX_ENTITIES}");
    println!("guard_max_clients={DEFAULT_GUARD_MAX_CLIENTS}");
    println!("guard_max_stations={DEFAULT_GUARD_MAX_STATIONS}");
    println!("guard_max_ticks={DEFAULT_GUARD_MAX_TICKS}");
    println!("entities={}", config.entities);
    println!("clients={}", config.clients);
    println!("stations={}", config.stations);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.tick_ms.len());
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
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
    print_phase_percentiles("planning_ms", &stats.planning_ms);
    print_phase_percentiles("encoding_ms", &stats.encoding_ms);
    println!("tick_128hz_budget_ms={TICK_128_HZ_BUDGET_MS:.4}");
    println!(
        "tick_128hz_headroom_ms={:.4}",
        TICK_128_HZ_BUDGET_MS - tick_ms_p99
    );
    println!("tick_128hz_ok={}", tick_ms_p99 <= TICK_128_HZ_BUDGET_MS);
    let verdict = BenchVerdict::evaluate(config, &stats, tick_ms_p99);
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
    println!(
        "threshold_payload_materialization_ok={}",
        verdict.payload_materialization_ok
    );
    println!(
        "threshold_workload_completed_ok={}",
        verdict.workload_completed_ok
    );
    println!("threshold_planner_mode_ok={}", verdict.planner_mode_ok);
    println!(
        "threshold_profile_admitted_ok={}",
        verdict.profile_admitted_ok
    );
    println!("benchmark_ok={}", verdict.is_ok());
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
    if !verdict.is_ok() {
        std::process::exit(1);
    }
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
    payload_materialization_ok: bool,
    workload_completed_ok: bool,
    planner_mode_ok: bool,
    profile_admitted_ok: bool,
}

impl BenchVerdict {
    fn evaluate(config: BenchConfig, stats: &BenchStats, tick_ms_p99: f64) -> Self {
        let expected_client_bridge_frames = stats.client_bridge_commands_sent;
        let thresholds = config.thresholds;
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
            payload_materialization_ok: !config.payload_materialization.requires_full()
                || stats.payload_entity_deltas == stats.updates,
            workload_completed_ok: !stats.time_budget_exhausted
                && !stats.tick_ms.is_empty()
                && stats.tick_ms.len() == config.ticks
                && stats.encoded_packets > 0,
            planner_mode_ok: config.requested_planner == config.planner,
            profile_admitted_ok: !config.heavy_profile_denied,
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
            && self.payload_materialization_ok
            && self.workload_completed_ok
            && self.planner_mode_ok
            && self.profile_admitted_ok
    }
}

impl BenchConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        Self::from_args_with_host(args, HostResources::detect())
    }

    #[allow(clippy::too_many_lines)]
    fn from_args_with_host(args: impl Iterator<Item = String>, host: HostResources) -> Self {
        let args = args.collect::<Vec<_>>();
        let allow_heavy = args.iter().any(|arg| arg == "--allow-heavy");
        let mut config = Self::smoke(host, allow_heavy);
        for arg in args {
            if let Some(value) = arg.strip_prefix("--entities=") {
                config.entities = value.parse().unwrap_or(config.entities).max(1);
            } else if let Some(value) = arg.strip_prefix("--clients=") {
                config.clients = value.parse().unwrap_or(config.clients).max(1);
            } else if let Some(value) = arg.strip_prefix("--stations=") {
                config.stations = value.parse().unwrap_or(config.stations).max(1);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks).max(1);
            } else if let Some(value) = arg.strip_prefix("--baseline=") {
                config.baseline = match value {
                    "full" => Baseline::FullBroadcast,
                    "room" => Baseline::RoomBroadcast,
                    "naive-grid" => Baseline::NaiveGrid,
                    "sectorsync" => Baseline::SectorSync,
                    _ => config.baseline,
                };
            } else if let Some(value) = arg.strip_prefix("--planner=") {
                config.requested_planner = match value {
                    "scalar" => PlannerMode::Scalar,
                    "batch" => PlannerMode::Batch,
                    "parallel" => PlannerMode::Parallel,
                    _ => config.requested_planner,
                };
            } else if let Some(value) = arg.strip_prefix("--threads=") {
                config.requested_parallel_threads =
                    value.parse().unwrap_or(config.requested_parallel_threads);
            } else if let Some(value) = arg.strip_prefix("--tick-rate-hz=") {
                config.tick_rate_hz = value.parse().unwrap_or(config.tick_rate_hz);
            } else if let Some(value) = arg.strip_prefix("--replication-hz=") {
                config.replication_hz = value.parse().unwrap_or(config.replication_hz);
            } else if let Some(value) = arg.strip_prefix("--profile=") {
                match value {
                    "smoke" => {
                        config = Self::smoke(host, allow_heavy);
                        config.requested_profile = "smoke";
                    }
                    "local" => {
                        config = Self::local(host, allow_heavy);
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
        config.resolve_planner();
        config.tick_rate_hz = config.tick_rate_hz.max(1);
        config.replication_hz = config.replication_hz.clamp(1, config.tick_rate_hz);
        config.apply_local_resource_guard();
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

    fn local(host: HostResources, allow_heavy: bool) -> Self {
        let mut config = Self::smoke(host, allow_heavy);
        config.requested_profile = "local";
        if allow_heavy {
            config.profile_name = "local";
            config.entities = host
                .parallelism
                .saturating_mul(2_000)
                .clamp(8_000, LOCAL_MAX_ENTITIES);
            config.clients = host
                .parallelism
                .saturating_mul(40)
                .clamp(160, LOCAL_MAX_CLIENTS);
            config.stations = host.parallelism.clamp(4, LOCAL_MAX_STATIONS);
            config.ticks = LOCAL_TICKS;
            config.payload_materialization = PayloadMaterialization::Full;
            config.workload_shape = WorkloadShape::Dense;
            config.time_budget_ms = Some(LOCAL_TIME_BUDGET_MS);
            config.tick_rate_hz = 128;
            config.replication_hz = 128;
            config.thresholds.estimated_payload_bytes = 512 * 1024 * 1024;
        } else {
            config.heavy_profile_denied = true;
        }
        config
    }

    fn apply_default_resource_guard(&mut self) {
        if self.allow_heavy {
            return;
        }

        let before = (self.entities, self.clients, self.stations, self.ticks);
        self.entities = self.entities.clamp(1, DEFAULT_GUARD_MAX_ENTITIES);
        self.clients = self.clients.clamp(1, DEFAULT_GUARD_MAX_CLIENTS);
        self.stations = self.stations.clamp(1, DEFAULT_GUARD_MAX_STATIONS);
        self.ticks = self.ticks.clamp(1, DEFAULT_GUARD_MAX_TICKS);
        self.default_resource_guard_applied =
            before != (self.entities, self.clients, self.stations, self.ticks);
    }

    fn apply_local_resource_guard(&mut self) {
        if self.profile_name != "local" {
            return;
        }

        let before = (self.entities, self.clients, self.stations, self.ticks);
        self.entities = self.entities.clamp(1, LOCAL_MAX_ENTITIES);
        self.clients = self.clients.clamp(1, LOCAL_MAX_CLIENTS);
        self.stations = self.stations.clamp(1, LOCAL_MAX_STATIONS);
        self.ticks = self.ticks.clamp(1, LOCAL_TICKS);
        self.default_resource_guard_applied |=
            before != (self.entities, self.clients, self.stations, self.ticks);
    }

    fn resolve_planner(&mut self) {
        self.planner = match self.requested_planner {
            PlannerMode::Parallel if !cfg!(feature = "parallel") => PlannerMode::Scalar,
            requested => requested,
        };
    }

    fn replication_phase_count(self) -> usize {
        usize::from(self.tick_rate_hz.div_ceil(self.replication_hz))
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
    let run_start = Instant::now();
    let mut stations = create_stations(config.stations, config.tick_rate_hz);
    let mut indexes = create_indexes(config.stations);
    let policies = create_policies();
    populate_entities(
        config.entities,
        config.workload_shape,
        &mut stations,
        &mut indexes,
    );
    let clients = create_clients(config.clients, config.workload_shape);
    let viewer_schedules =
        create_viewer_schedules(&clients, config.stations, config.replication_phase_count());

    let mut stats = BenchStats::default();
    let load_samples = apply_hotspot_report(&mut stats, config, &stations, &indexes);
    apply_scheduler_report(&mut stats, config, &load_samples);
    let mut transport = FakeTransport::default();
    let mut command_queues = create_command_queues(config.stations);
    let mut dispatch = DispatchBench::new(config);
    let mut client_bridge = ClientBridgeBench::new(config);
    let mut replication_scratch = ReplicationScratch::default();
    let mut batch_scratch = vec![ReplicationScratch::default(); config.stations];
    #[cfg(feature = "parallel")]
    let parallel_pool = if config.planner == PlannerMode::Parallel {
        let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(
            config.requested_parallel_threads,
            8,
        ))
        .expect("benchmark replication pool should build");
        stats.parallel_threads = pool.threads();
        Some(pool)
    } else {
        None
    };
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
        if config
            .time_budget_ms
            .is_some_and(|budget| run_start.elapsed() >= Duration::from_millis(budget))
        {
            stats.time_budget_exhausted = true;
            break;
        }
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
        let viewer_batches = &viewer_schedules[tick_index % viewer_schedules.len()];

        match config.baseline {
            Baseline::SectorSync => {
                if config.planner == PlannerMode::Parallel {
                    #[cfg(feature = "parallel")]
                    {
                        let outputs = run_parallel_station_pipeline(
                            parallel_pool
                                .as_ref()
                                .expect("parallel planner requires explicit pool"),
                            &stations,
                            &indexes,
                            viewer_batches,
                            &policies,
                            &mut batch_scratch,
                            config.payload_materialization.entity_limit(),
                        );
                        let planning_ms = outputs
                            .iter()
                            .map(|output| output.planning_ms)
                            .fold(0.0_f64, f64::max);
                        let encoding_ms = outputs
                            .iter()
                            .map(|output| output.encoding_ms)
                            .fold(0.0_f64, f64::max);
                        stats.planning_ms.push(planning_ms);
                        stats.encoding_ms.push(encoding_ms);
                        let mut encoded = Vec::with_capacity(config.clients);
                        for output in outputs {
                            record_replication_batch_stats(&mut stats, output.stats);
                            encoded.extend(output.encoded);
                        }
                        submit_encoded_updates(&mut stats, &mut transport, encoded);
                    }
                    #[cfg(not(feature = "parallel"))]
                    unreachable!("parallel planner is resolved away without its feature");
                } else {
                    let planning_start = Instant::now();
                    let planned_batches = match config.planner {
                        PlannerMode::Scalar => stations
                            .iter()
                            .zip(&indexes)
                            .zip(viewer_batches)
                            .map(|((station, index), viewers)| {
                                ReplicationPlanner::plan_for_viewers_with_scratch(
                                    station,
                                    index,
                                    &policies,
                                    viewers,
                                    &RangeOnlyVisibility,
                                    ReplicationBudget::default(),
                                    &mut replication_scratch,
                                )
                            })
                            .collect::<Vec<_>>(),
                        PlannerMode::Batch => stations
                            .iter()
                            .zip(&indexes)
                            .zip(viewer_batches)
                            .zip(&mut batch_scratch)
                            .map(|(((station, index), viewers), scratch)| {
                                ReplicationPlanner::plan_for_viewers_range_with_scratch(
                                    station,
                                    index,
                                    &policies,
                                    viewers,
                                    ReplicationBudget::default(),
                                    scratch,
                                )
                            })
                            .collect::<Vec<_>>(),
                        PlannerMode::Parallel => unreachable!(),
                    };
                    stats
                        .planning_ms
                        .push(planning_start.elapsed().as_secs_f64() * 1000.0);
                    let encoding_start = Instant::now();
                    let mut work = Vec::with_capacity(config.clients);
                    for (station_index, batch) in planned_batches.iter().enumerate() {
                        record_replication_batch_stats(&mut stats, batch.stats);
                        for (viewer, plan) in viewer_batches[station_index].iter().zip(&batch.plans)
                        {
                            let client_index = usize::try_from(viewer.client_id.get())
                                .expect("benchmark client id fits usize");
                            work.push(UpdateWork {
                                client_index,
                                server_tick: stations[station_index].tick(),
                                updates: plan.stats.selected,
                                entity_limit: config.payload_materialization.entity_limit(),
                            });
                        }
                    }
                    let encoded = encode_update_batch(config.planner, work);
                    submit_encoded_updates(&mut stats, &mut transport, encoded);
                    stats
                        .encoding_ms
                        .push(encoding_start.elapsed().as_secs_f64() * 1000.0);
                }
            }
            baseline => {
                let planning_start = Instant::now();
                let updates = clients
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(client_index, viewer_position)| {
                        let station_index = client_index % stations.len();
                        let updates = match baseline {
                            Baseline::FullBroadcast => config.entities,
                            Baseline::RoomBroadcast => config.entities / stations.len(),
                            Baseline::NaiveGrid => indexes[station_index]
                                .query_sphere(viewer_position, WorkloadShape::viewer_radius())
                                .len(),
                            Baseline::SectorSync => unreachable!(),
                        };
                        (client_index, station_index, updates)
                    })
                    .collect::<Vec<_>>();
                stats
                    .planning_ms
                    .push(planning_start.elapsed().as_secs_f64() * 1000.0);
                let encoding_start = Instant::now();
                let work = updates
                    .into_iter()
                    .map(|(client_index, station_index, updates)| UpdateWork {
                        client_index,
                        server_tick: stations[station_index].tick(),
                        updates,
                        entity_limit: config.payload_materialization.entity_limit(),
                    })
                    .collect();
                let encoded = encode_update_batch(config.planner, work);
                submit_encoded_updates(&mut stats, &mut transport, encoded);
                stats
                    .encoding_ms
                    .push(encoding_start.elapsed().as_secs_f64() * 1000.0);
            }
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

fn create_viewer_schedules(
    clients: &[Position3],
    stations: usize,
    phases: usize,
) -> Vec<Vec<Vec<ViewerQuery>>> {
    let mut schedules = vec![vec![Vec::new(); stations]; phases];
    for (client_index, position) in clients.iter().copied().enumerate() {
        let station_index = client_index % stations;
        let phase = (client_index / stations) % phases;
        schedules[phase][station_index].push(ViewerQuery {
            client_id: ClientId::new(client_index as u64),
            position,
            radius: WorkloadShape::viewer_radius(),
            max_entities: 300,
        });
    }
    schedules
}

fn record_replication_batch_stats(stats: &mut BenchStats, batch: ReplicationBatchStats) {
    stats.replication_scratch_queries = stats
        .replication_scratch_queries
        .saturating_add(batch.viewers);
    stats.replication_scratch_candidates = stats
        .replication_scratch_candidates
        .saturating_add(batch.candidates);
    stats.replication_scratch_grid_queries = stats
        .replication_scratch_grid_queries
        .saturating_add(batch.grid_queries);
    stats.replication_scratch_occupied_queries = stats
        .replication_scratch_occupied_queries
        .saturating_add(batch.occupied_queries);
    stats.replication_scratch_grid_cells_probed = stats
        .replication_scratch_grid_cells_probed
        .saturating_add(batch.grid_cells_probed);
    stats.replication_scratch_occupied_cells_scanned = stats
        .replication_scratch_occupied_cells_scanned
        .saturating_add(batch.occupied_cells_scanned);
    stats.replication_scratch_matched_cells = stats
        .replication_scratch_matched_cells
        .saturating_add(batch.matched_cells);
    stats.replication_scratch_candidate_capacity_max = stats
        .replication_scratch_candidate_capacity_max
        .max(batch.candidate_capacity_max);
    stats.replication_scratch_dedup_capacity_max = stats
        .replication_scratch_dedup_capacity_max
        .max(batch.dedup_capacity_max);
    stats.replication_scratch_matching_cell_capacity_max = stats
        .replication_scratch_matching_cell_capacity_max
        .max(batch.matching_cell_capacity_max);
    stats.replication_scratch_priority_capacity_max = stats
        .replication_scratch_priority_capacity_max
        .max(batch.priority_capacity_max);
}

#[cfg(feature = "parallel")]
fn run_parallel_station_pipeline(
    pool: &ReplicationThreadPool,
    stations: &[Station],
    indexes: &[CellIndex],
    viewer_batches: &[Vec<ViewerQuery>],
    policies: &PolicyTable,
    scratch: &mut [ReplicationScratch],
    entity_limit: usize,
) -> Vec<StationPipelineOutput> {
    let work = stations
        .iter()
        .zip(indexes)
        .zip(viewer_batches)
        .zip(scratch)
        .map(
            |(((station, index), viewers), scratch)| StationPipelineWork {
                station,
                index,
                viewers,
                scratch,
            },
        )
        .collect();
    pool.map_ordered(work, |work| {
        let planning_start = Instant::now();
        let batch = ReplicationPlanner::plan_for_viewers_range_with_scratch(
            work.station,
            work.index,
            policies,
            work.viewers,
            ReplicationBudget::default(),
            work.scratch,
        );
        let planning_ms = planning_start.elapsed().as_secs_f64() * 1000.0;
        let encoding_start = Instant::now();
        let encoded = work
            .viewers
            .iter()
            .zip(&batch.plans)
            .map(|(viewer, plan)| {
                encode_update(UpdateWork {
                    client_index: usize::try_from(viewer.client_id.get())
                        .expect("benchmark client id fits usize"),
                    server_tick: work.station.tick(),
                    updates: plan.stats.selected,
                    entity_limit,
                })
            })
            .collect();
        StationPipelineOutput {
            stats: batch.stats,
            encoded,
            planning_ms,
            encoding_ms: encoding_start.elapsed().as_secs_f64() * 1000.0,
        }
    })
}

fn encode_update_batch(_planner: PlannerMode, work: Vec<UpdateWork>) -> Vec<EncodedUpdate> {
    work.into_iter().map(encode_update).collect()
}

fn encode_update(work: UpdateWork) -> EncodedUpdate {
    let entity_deltas = build_sample_deltas(
        work.updates,
        work.client_index,
        work.server_tick,
        work.entity_limit,
    );
    let entity_delta_count = entity_deltas.len();
    let component_delta_count = entity_deltas
        .iter()
        .map(|delta| delta.components.len())
        .sum();
    let frame = ReplicationFrame {
        client_id: ClientId::new(work.client_index as u64),
        server_tick: work.server_tick,
        entity_count: u32::try_from(work.updates).unwrap_or(u32::MAX),
        estimated_payload_bytes: u32::try_from(work.updates.saturating_mul(32)).unwrap_or(u32::MAX),
        entities: entity_deltas,
    };
    let mut bytes = Vec::with_capacity(32);
    BinaryFrameEncoder
        .encode_replication(&frame, &mut bytes)
        .expect("binary encoder is infallible");
    EncodedUpdate {
        packet: OutboundPacket {
            client_id: frame.client_id,
            bytes,
        },
        updates: work.updates,
        entity_deltas: entity_delta_count,
        component_deltas: component_delta_count,
    }
}

fn submit_encoded_updates(
    stats: &mut BenchStats,
    transport: &mut FakeTransport,
    encoded: Vec<EncodedUpdate>,
) {
    let mut batch = PacketBatch::new();
    for update in encoded {
        stats.updates = stats.updates.saturating_add(update.updates);
        stats.replication_candidates_selected = stats
            .replication_candidates_selected
            .saturating_add(update.updates);
        stats.estimated_payload_bytes = stats
            .estimated_payload_bytes
            .saturating_add(update.updates.saturating_mul(32));
        stats.payload_entity_deltas = stats
            .payload_entity_deltas
            .saturating_add(update.entity_deltas);
        stats.payload_component_deltas = stats
            .payload_component_deltas
            .saturating_add(update.component_deltas);
        batch.push(update.packet);
    }
    transport
        .send_batch(batch)
        .expect("fake transport batch is infallible");
}

fn build_sample_deltas(
    update_count: usize,
    client_index: usize,
    tick: Tick,
    entity_limit: usize,
) -> Vec<EntityDelta> {
    let sample_count = update_count.min(entity_limit);
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

fn print_phase_percentiles(name: &str, values: &[f64]) {
    println!("{name}_p50={:.3}", percentile_ms(values, 0.50));
    println!("{name}_p95={:.3}", percentile_ms(values, 0.95));
    println!("{name}_p99={:.3}", percentile_ms(values, 0.99));
    println!("{name}_max={:.3}", percentile_ms(values, 1.00));
}

fn create_stations(count: usize, tick_rate_hz: u16) -> Vec<Station> {
    (0..count)
        .map(|index| {
            Station::new(StationConfig {
                station_id: StationId::new(
                    u32::try_from(index).expect("station count must fit in u32"),
                ),
                node_id: NodeId::new(u32::try_from(index % 4).expect("node shard must fit in u32")),
                instance_id: InstanceId::new(1),
                tick_rate_hz,
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
            entities: build_sample_deltas(1, client_index, tick, 1),
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

fn populate_entities(
    count: usize,
    shape: WorkloadShape,
    stations: &mut [Station],
    indexes: &mut [CellIndex],
) {
    let mut rng = Lcg::new(0x5E_C7_0C);
    let horizontal_extent = shape.horizontal_extent();
    let vertical_extent = shape.vertical_extent();
    for entity_index in 0..count {
        let station_index = entity_index % stations.len();
        let position = Position3::new(
            rng.next_range(-horizontal_extent, horizontal_extent),
            rng.next_range(-vertical_extent, vertical_extent),
            rng.next_range(-horizontal_extent, horizontal_extent),
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
    for station in create_stations(config.stations, config.tick_rate_hz) {
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

fn create_clients(count: usize, shape: WorkloadShape) -> Vec<Position3> {
    let mut rng = Lcg::new(0xC1_13_17);
    let horizontal_extent = shape.horizontal_extent();
    let vertical_extent = shape.vertical_extent();
    (0..count)
        .map(|_| {
            Position3::new(
                rng.next_range(-horizontal_extent, horizontal_extent),
                rng.next_range(-vertical_extent, vertical_extent),
                rng.next_range(-horizontal_extent, horizontal_extent),
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
    fn custom_scale_cannot_disable_benchmark_work() {
        let config = BenchConfig::from_args_with_host(
            args(&["--entities=0", "--clients=0", "--ticks=0"]),
            HostResources { parallelism: 4 },
        );

        assert_eq!(config.entities, 1);
        assert_eq!(config.clients, 1);
        assert_eq!(config.ticks, 1);
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
    fn local_profile_is_gated_and_scales_to_the_current_host_cap() {
        let denied = BenchConfig::from_args_with_host(
            args(&["--profile=local"]),
            HostResources { parallelism: 12 },
        );
        assert_eq!(denied.requested_profile, "local");
        assert_eq!(denied.profile_name, "smoke");
        assert!(denied.heavy_profile_denied);

        let config = BenchConfig::from_args_with_host(
            args(&["--profile=local", "--allow-heavy"]),
            HostResources { parallelism: 12 },
        );
        assert_eq!(config.profile_name, "local");
        assert_eq!(config.entities, LOCAL_MAX_ENTITIES);
        assert_eq!(config.clients, LOCAL_MAX_CLIENTS);
        assert_eq!(config.stations, LOCAL_MAX_STATIONS);
        assert_eq!(config.ticks, LOCAL_TICKS);
        assert_eq!(config.payload_materialization, PayloadMaterialization::Full);
        assert_eq!(config.workload_shape, WorkloadShape::Dense);
        assert_eq!(config.time_budget_ms, Some(LOCAL_TIME_BUDGET_MS));
        assert_eq!(config.tick_rate_hz, 128);
        assert_eq!(config.replication_hz, 128);
    }

    #[test]
    fn local_profile_clamps_manual_overrides_even_with_allow_heavy() {
        let config = BenchConfig::from_args_with_host(
            args(&[
                "--profile=local",
                "--allow-heavy",
                "--entities=1000000",
                "--clients=10000",
                "--stations=64",
                "--ticks=100",
            ]),
            HostResources { parallelism: 12 },
        );

        assert_eq!(config.entities, LOCAL_MAX_ENTITIES);
        assert_eq!(config.clients, LOCAL_MAX_CLIENTS);
        assert_eq!(config.stations, LOCAL_MAX_STATIONS);
        assert_eq!(config.ticks, LOCAL_TICKS);
        assert!(config.default_resource_guard_applied);
    }

    #[test]
    fn replication_rate_builds_balanced_station_phases() {
        let config = BenchConfig::from_args_with_host(
            args(&["--profile=local", "--allow-heavy", "--replication-hz=32"]),
            HostResources { parallelism: 12 },
        );
        let clients = create_clients(32, WorkloadShape::Dense);
        let schedules = create_viewer_schedules(&clients, 4, config.replication_phase_count());

        assert_eq!(config.replication_phase_count(), 4);
        assert_eq!(schedules.len(), 4);
        assert!(schedules.iter().flatten().all(|viewers| viewers.len() == 2));
    }

    #[test]
    fn parallel_planner_request_matches_compile_time_availability() {
        let config = BenchConfig::from_args_with_host(
            args(&["--planner=parallel"]),
            HostResources { parallelism: 4 },
        );

        assert_eq!(config.requested_planner, PlannerMode::Parallel);
        if cfg!(feature = "parallel") {
            assert_eq!(config.planner, PlannerMode::Parallel);
        } else {
            assert_eq!(config.planner, PlannerMode::Scalar);
        }
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

        let verdict = BenchVerdict::evaluate(BenchConfig::default(), &stats, 0.0);

        assert!(!verdict.command_queue_drops_ok);
        assert!(!verdict.router_event_drops_ok);
        assert!(!verdict.is_ok());
    }

    #[test]
    fn denied_profile_fails_the_benchmark_verdict() {
        let config = BenchConfig {
            heavy_profile_denied: true,
            ..BenchConfig::default()
        };
        let stats = BenchStats::default();

        let verdict = BenchVerdict::evaluate(config, &stats, 0.0);

        assert!(!verdict.profile_admitted_ok);
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

    #[test]
    fn full_payload_run_materializes_every_selected_update() {
        let config = BenchConfig {
            entities: 64,
            clients: 8,
            stations: 2,
            ticks: 2,
            payload_materialization: PayloadMaterialization::Full,
            ..BenchConfig::default()
        };
        let stats = run(config);
        let verdict = BenchVerdict::evaluate(config, &stats, percentile_ms(&stats.tick_ms, 0.99));

        assert_eq!(stats.payload_entity_deltas, stats.updates);
        assert!(verdict.payload_materialization_ok);
        assert!(verdict.workload_completed_ok);
    }
}
