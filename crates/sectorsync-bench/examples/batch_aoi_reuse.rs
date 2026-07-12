//! Guarded A/B benchmark for within-call batch AOI candidate reuse.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId,
    PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBatchScratch,
    ReplicationBudget, ReplicationPlanner, ReplicationScratch, Station, StationConfig, StationId,
    ViewerQuery,
};
use sectorsync_core::replication::ReplicationSelectionMode;

const DEFAULT_ENTITIES: usize = 2_000;
const DEFAULT_VIEWERS: usize = 256;
const DEFAULT_CALLS: usize = 20;
const GUARD_MAX_ENTITIES: usize = 4_000;
const GUARD_MAX_VIEWERS: usize = 500;
const GUARD_MAX_CALLS: usize = 30;
const TIME_BUDGET: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Shape {
    Identical,
    Partial,
    Unique,
    #[default]
    Mixed,
}

impl Shape {
    fn parse(value: &str) -> Self {
        match value {
            "identical" => Self::Identical,
            "partial" => Self::Partial,
            "unique" => Self::Unique,
            _ => Self::Mixed,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Identical => "identical",
            Self::Partial => "partial",
            Self::Unique => "unique",
            Self::Mixed => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Layout {
    #[default]
    Dense,
    Sparse,
}

impl Layout {
    const fn name(self) -> &'static str {
        match self {
            Self::Dense => "dense",
            Self::Sparse => "sparse",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    entities: usize,
    viewers: usize,
    calls: usize,
    shape: Shape,
    layout: Layout,
    reuse: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            entities: DEFAULT_ENTITIES,
            viewers: DEFAULT_VIEWERS,
            calls: DEFAULT_CALLS,
            shape: Shape::Mixed,
            layout: Layout::Dense,
            reuse: !args.iter().any(|arg| arg == "--no-query-reuse"),
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            guard_applied: false,
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--entities=") {
                config.entities = value.parse().unwrap_or(config.entities);
            } else if let Some(value) = arg.strip_prefix("--viewers=") {
                config.viewers = value.parse().unwrap_or(config.viewers);
            } else if let Some(value) = arg.strip_prefix("--calls=") {
                config.calls = value.parse().unwrap_or(config.calls);
            } else if let Some(value) = arg.strip_prefix("--shape=") {
                config.shape = Shape::parse(value);
            } else if arg == "--layout=sparse" {
                config.layout = Layout::Sparse;
            }
        }
        let requested = (config.entities, config.viewers, config.calls);
        config.entities = config.entities.max(1);
        config.viewers = config.viewers.max(1);
        config.calls = config.calls.max(1);
        if !config.allow_heavy {
            config.entities = config.entities.min(GUARD_MAX_ENTITIES);
            config.viewers = config.viewers.min(GUARD_MAX_VIEWERS);
            config.calls = config.calls.min(GUARD_MAX_CALLS);
        }
        config.guard_applied = requested != (config.entities, config.viewers, config.calls);
        config
    }
}

#[derive(Debug, Default)]
struct RunStats {
    call_ms: Vec<f64>,
    calls_completed: usize,
    selected: usize,
    checksum: u64,
    unique_ranges: usize,
    reused_ranges: usize,
    grid_queries: usize,
    occupied_queries: usize,
    grid_cells_probed: usize,
    occupied_cells_scanned: usize,
    cache_slots_max: usize,
    cache_candidate_capacity_max: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let (station, index, policies, viewers) = create_world(config);
    let expected_checksum = reference_checksum(&station, &index, &policies, &viewers);
    let stats = run(&station, &index, &policies, &viewers, config);
    let output_equal = stats.calls_completed > 0
        && stats.checksum
            == expected_checksum.wrapping_mul(u64::try_from(stats.calls_completed).unwrap_or(0));
    let work_reduction_ok =
        !config.reuse || config.shape == Shape::Unique || stats.reused_ranges > 0;
    let benchmark_ok = stats.calls_completed == config.calls
        && !stats.time_budget_exhausted
        && output_equal
        && stats.selected > 0
        && work_reduction_ok;

    println!("SectorSync batch AOI reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_entities={GUARD_MAX_ENTITIES}");
    println!("guard_max_viewers={GUARD_MAX_VIEWERS}");
    println!("guard_max_calls={GUARD_MAX_CALLS}");
    println!("shape={}", config.shape.name());
    println!("layout={}", config.layout.name());
    println!("query_reuse={}", config.reuse);
    println!("entities={}", config.entities);
    println!("viewers={}", config.viewers);
    println!("calls={}", config.calls);
    println!("calls_completed={}", stats.calls_completed);
    println!("selected_entities={}", stats.selected);
    println!("unique_query_ranges={}", stats.unique_ranges);
    println!("reused_query_ranges={}", stats.reused_ranges);
    println!("grid_queries={}", stats.grid_queries);
    println!("occupied_queries={}", stats.occupied_queries);
    println!("grid_cells_probed={}", stats.grid_cells_probed);
    println!("occupied_cells_scanned={}", stats.occupied_cells_scanned);
    println!("query_cache_slots_max={}", stats.cache_slots_max);
    println!(
        "query_cache_candidate_capacity_max={}",
        stats.cache_candidate_capacity_max
    );
    println!("output_checksum={}", stats.checksum);
    println!("reference_checksum_per_call={expected_checksum}");
    println!("output_equal={output_equal}");
    println!("call_ms_p50={:.3}", percentile(&stats.call_ms, 0.50));
    println!("call_ms_p95={:.3}", percentile(&stats.call_ms, 0.95));
    println!("call_ms_p99={:.3}", percentile(&stats.call_ms, 0.99));
    println!("call_ms_max={:.3}", percentile(&stats.call_ms, 1.00));
    println!("time_budget_ms={}", TIME_BUDGET.as_millis());
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("threshold_work_reduction_ok={work_reduction_ok}");
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_world(config: Config) -> (Station, CellIndex, PolicyTable, Vec<ViewerQuery>) {
    let mut station = Station::with_capacity(
        StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 30,
        },
        config.entities,
    );
    let mut index = CellIndex::with_capacity(
        GridSpec::new(16.0).expect("fixed grid is valid"),
        config.entities,
        config.entities,
    );
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 30, 96.0));
    let spacing = match config.layout {
        Layout::Dense => 4.0,
        Layout::Sparse => 48.0,
    };
    for entity in 0..config.entities {
        let x = u16::try_from(entity % 64).expect("x lane fits u16");
        let z = u16::try_from((entity / 64) % 64).expect("z lane fits u16");
        let position = Position3::new(f32::from(x) * spacing, 0.0, f32::from(z) * spacing);
        let handle = station
            .spawn_owned(
                EntityId::new(u64::try_from(entity).expect("entity id fits u64")),
                position,
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("entity ids are unique");
        index.upsert(handle, position, Bounds::Point);
    }
    let viewers = (0..config.viewers)
        .map(|viewer| {
            let range = viewer_range(config.shape, viewer);
            let lane = u16::try_from(range).expect("guarded viewer lane fits u16");
            let viewer_spacing = spacing.max(16.0);
            ViewerQuery {
                client_id: ClientId::new(u64::try_from(viewer).expect("client id fits u64")),
                position: Position3::new(f32::from(lane) * viewer_spacing, 0.0, 0.0),
                radius: 96.0,
                max_entities: 256,
            }
        })
        .collect();
    (station, index, policies, viewers)
}

const fn viewer_range(shape: Shape, viewer: usize) -> usize {
    match shape {
        Shape::Identical => 0,
        Shape::Partial => viewer / 2,
        Shape::Mixed if viewer % 4 != 3 => viewer / 4,
        Shape::Unique | Shape::Mixed => viewer,
    }
}

fn reference_checksum(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewers: &[ViewerQuery],
) -> u64 {
    let mut scratch = ReplicationScratch::default();
    let mut batch = ReplicationBatchScratch::default();
    let view = plan(
        station,
        index,
        policies,
        viewers,
        0,
        &mut scratch,
        &mut batch,
    );
    checksum(view.plans)
}

fn run(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewers: &[ViewerQuery],
    config: Config,
) -> RunStats {
    let started = Instant::now();
    let mut scratch = ReplicationScratch::default();
    let mut batch = ReplicationBatchScratch::default();
    let mut stats = RunStats::default();
    for _ in 0..config.calls {
        if started.elapsed() >= TIME_BUDGET {
            stats.time_budget_exhausted = true;
            break;
        }
        let call_started = Instant::now();
        let max_cached_ranges = if config.reuse {
            viewers.len().min(64)
        } else {
            0
        };
        let view = plan(
            station,
            index,
            policies,
            viewers,
            max_cached_ranges,
            &mut scratch,
            &mut batch,
        );
        stats
            .call_ms
            .push(call_started.elapsed().as_secs_f64() * 1_000.0);
        stats.calls_completed = stats.calls_completed.saturating_add(1);
        stats.selected = stats.selected.saturating_add(view.stats.selected);
        stats.checksum = stats.checksum.wrapping_add(checksum(view.plans));
        stats.unique_ranges = stats
            .unique_ranges
            .saturating_add(view.stats.unique_query_ranges);
        stats.reused_ranges = stats
            .reused_ranges
            .saturating_add(view.stats.reused_query_ranges);
        stats.grid_queries = stats.grid_queries.saturating_add(view.stats.grid_queries);
        stats.occupied_queries = stats
            .occupied_queries
            .saturating_add(view.stats.occupied_queries);
        stats.grid_cells_probed = stats
            .grid_cells_probed
            .saturating_add(view.stats.grid_cells_probed);
        stats.occupied_cells_scanned = stats
            .occupied_cells_scanned
            .saturating_add(view.stats.occupied_cells_scanned);
        stats.cache_slots_max = stats
            .cache_slots_max
            .max(view.stats.query_cache_capacity_max);
        stats.cache_candidate_capacity_max = stats
            .cache_candidate_capacity_max
            .max(view.stats.query_cache_candidate_capacity_max);
        black_box(view.plans);
    }
    stats
}

#[allow(clippy::too_many_arguments)]
fn plan<'a>(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewers: &[ViewerQuery],
    max_cached_ranges: usize,
    scratch: &mut ReplicationScratch,
    batch: &'a mut ReplicationBatchScratch,
) -> sectorsync_core::replication::ReplicationBatchView<'a> {
    ReplicationPlanner::plan_for_viewers_configured_into(
        station,
        index,
        policies,
        viewers,
        &RangeOnlyVisibility,
        ReplicationBudget::default(),
        ReplicationSelectionMode::Throughput,
        max_cached_ranges,
        |_, _, _| true,
        |_, _| None,
        scratch,
        batch,
    )
}

fn checksum(plans: &[sectorsync_core::replication::ReplicationPlan]) -> u64 {
    plans.iter().fold(0_u64, |checksum, plan| {
        plan.entities.iter().fold(checksum, |checksum, handle| {
            checksum
                .wrapping_mul(1_099_511_628_211)
                .wrapping_add(u64::from(handle.index()))
                .wrapping_add(u64::from(handle.generation()) << 32)
                .wrapping_add(1)
        })
    })
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
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[index]
}
