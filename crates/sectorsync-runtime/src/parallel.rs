//! Explicit, bounded parallel replication planning.

use core::fmt;

use rayon::prelude::*;
use sectorsync_core::prelude::{
    CellIndex, PolicyTable, ReplicationBatchResult, ReplicationBatchStats, ReplicationBudget,
    ReplicationPlanner, ReplicationScratch, Station, ViewerQuery, VisibilityFilter,
};

/// Configuration for an explicitly created replication planning thread pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationThreadPoolConfig {
    /// Requested workers. Zero selects half of host logical parallelism.
    pub requested_threads: usize,
    /// Upper bound applied after host parallelism is detected.
    pub max_threads: usize,
}

impl Default for ReplicationThreadPoolConfig {
    fn default() -> Self {
        Self {
            requested_threads: 0,
            max_threads: 8,
        }
    }
}

impl ReplicationThreadPoolConfig {
    /// Creates an explicit bounded configuration.
    pub const fn new(requested_threads: usize, max_threads: usize) -> Self {
        Self {
            requested_threads,
            max_threads,
        }
    }

    fn resolve(self, available: usize) -> usize {
        let available = available.max(1);
        let requested = if self.requested_threads == 0 {
            available.div_ceil(2)
        } else {
            self.requested_threads
        };
        requested.clamp(1, self.max_threads.max(1).min(available))
    }
}

/// Error returned when the explicit worker pool cannot be created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationThreadPoolBuildError {
    message: String,
}

impl fmt::Display for ReplicationThreadPoolBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ReplicationThreadPoolBuildError {}

/// One immutable station-local viewer batch.
#[derive(Clone, Copy, Debug)]
pub struct StationReplicationBatch<'a> {
    /// Station containing the authoritative/ghost records used for planning.
    pub station: &'a Station,
    /// Spatial index paired with `station`.
    pub index: &'a CellIndex,
    /// Viewer queries retained in deterministic result order.
    pub viewers: &'a [ViewerQuery],
}

impl<'a> StationReplicationBatch<'a> {
    /// Creates a station-local batch without copying its inputs.
    pub const fn new(
        station: &'a Station,
        index: &'a CellIndex,
        viewers: &'a [ViewerQuery],
    ) -> Self {
        Self {
            station,
            index,
            viewers,
        }
    }
}

/// Caller-owned scratch lanes retained across parallel planning calls.
#[derive(Clone, Debug, Default)]
pub struct ParallelReplicationScratch {
    lanes: Vec<ReplicationScratch>,
}

impl ParallelReplicationScratch {
    /// Creates empty scratch storage. Lanes are allocated on first use.
    pub const fn new() -> Self {
        Self { lanes: Vec::new() }
    }

    /// Number of scratch lanes currently retained.
    pub fn lanes(&self) -> usize {
        self.lanes.len()
    }

    fn prepare(&mut self, lanes: usize) {
        self.lanes.resize_with(lanes, ReplicationScratch::default);
    }
}

/// Ordered station results and merged aggregate statistics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParallelReplicationResult {
    /// One result per input station batch, retaining input order.
    pub batches: Vec<ReplicationBatchResult>,
    /// Aggregate statistics merged in station input order.
    pub stats: ReplicationBatchStats,
}

impl ParallelReplicationResult {
    fn from_batches(batches: Vec<ReplicationBatchResult>) -> Self {
        let mut stats = ReplicationBatchStats::default();
        for batch in &batches {
            stats.merge(batch.stats);
        }
        Self { batches, stats }
    }
}

/// Explicit Rayon pool used only when the embedding application constructs it.
pub struct ReplicationThreadPool {
    pool: rayon::ThreadPool,
    threads: usize,
}

impl fmt::Debug for ReplicationThreadPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationThreadPool")
            .field("threads", &self.threads)
            .finish_non_exhaustive()
    }
}

impl ReplicationThreadPool {
    /// Creates a bounded worker pool. No threads exist before this call.
    pub fn new(
        config: ReplicationThreadPoolConfig,
    ) -> Result<Self, ReplicationThreadPoolBuildError> {
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let threads = config.resolve(available);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("sectorsync-replication-{index}"))
            .build()
            .map_err(|error| ReplicationThreadPoolBuildError {
                message: error.to_string(),
            })?;
        Ok(Self { pool, threads })
    }

    /// Creates a conservative host-sized pool using [`ReplicationThreadPoolConfig::default`].
    pub fn for_host() -> Result<Self, ReplicationThreadPoolBuildError> {
        Self::new(ReplicationThreadPoolConfig::default())
    }

    /// Number of worker threads created in this pool.
    pub const fn threads(&self) -> usize {
        self.threads
    }

    /// Maps an owned batch synchronously and retains input order.
    ///
    /// Work runs only for the duration of this call. The pool does not retain
    /// inputs, outputs, jobs, or cross-client state after it returns.
    pub fn map_ordered<T, R, F>(&self, inputs: Vec<T>, operation: F) -> Vec<R>
    where
        T: Send,
        R: Send,
        F: Fn(T) -> R + Send + Sync,
    {
        self.pool
            .install(|| inputs.into_par_iter().map(operation).collect())
    }

    /// Plans generic visibility batches in parallel by station.
    pub fn plan_station_batches<F>(
        &self,
        batches: &[StationReplicationBatch<'_>],
        policies: &PolicyTable,
        filter: &F,
        budget: ReplicationBudget,
        scratch: &mut ParallelReplicationScratch,
    ) -> ParallelReplicationResult
    where
        F: VisibilityFilter + Sync,
    {
        scratch.prepare(batches.len());
        let results = self.pool.install(|| {
            batches
                .par_iter()
                .zip(scratch.lanes.par_iter_mut())
                .map(|(batch, lane)| {
                    ReplicationPlanner::plan_for_viewers_with_scratch(
                        batch.station,
                        batch.index,
                        policies,
                        batch.viewers,
                        filter,
                        budget,
                        lane,
                    )
                })
                .collect()
        });
        ParallelReplicationResult::from_batches(results)
    }

    /// Plans range-only batches using SIMD when the core `simd` feature is enabled.
    pub fn plan_station_range_batches(
        &self,
        batches: &[StationReplicationBatch<'_>],
        policies: &PolicyTable,
        budget: ReplicationBudget,
        scratch: &mut ParallelReplicationScratch,
    ) -> ParallelReplicationResult {
        scratch.prepare(batches.len());
        let results = self.pool.install(|| {
            batches
                .par_iter()
                .zip(scratch.lanes.par_iter_mut())
                .map(|(batch, lane)| {
                    ReplicationPlanner::plan_for_viewers_range_with_scratch(
                        batch.station,
                        batch.index,
                        policies,
                        batch.viewers,
                        budget,
                        lane,
                    )
                })
                .collect()
        });
        ParallelReplicationResult::from_batches(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sectorsync_core::prelude::{
        Bounds, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId, PolicyId,
        Position3, RangeOnlyVisibility, StationConfig, StationId,
    };

    #[test]
    fn auto_thread_resolution_is_conservative_and_bounded() {
        assert_eq!(ReplicationThreadPoolConfig::default().resolve(12), 6);
        assert_eq!(ReplicationThreadPoolConfig::default().resolve(1), 1);
        assert_eq!(ReplicationThreadPoolConfig::new(64, 4).resolve(12), 4);
    }

    #[test]
    fn parallel_station_batches_match_serial_order() {
        let mut stations = Vec::new();
        let mut indexes = Vec::new();
        let mut viewers = Vec::new();
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 128, 128.0));

        for station_index in 0_u32..2 {
            let mut station = Station::new(StationConfig {
                station_id: StationId::new(station_index),
                node_id: NodeId::new(1),
                instance_id: InstanceId::new(1),
                tick_rate_hz: 128,
            });
            let mut index = CellIndex::new(GridSpec::new(16.0).expect("valid grid"));
            for entity_index in 0_u16..16 {
                let position = Position3::new(f32::from(entity_index) * 4.0, 0.0, 0.0);
                let handle = station
                    .spawn_owned(
                        EntityId::new(u64::from(station_index) * 100 + u64::from(entity_index)),
                        position,
                        Bounds::Point,
                        PolicyId::new(1),
                    )
                    .expect("unique entity");
                index.upsert(handle, position, Bounds::Point);
            }
            stations.push(station);
            indexes.push(index);
            viewers.push(vec![ViewerQuery {
                client_id: ClientId::new(u64::from(station_index)),
                position: Position3::new(16.0, 0.0, 0.0),
                radius: 64.0,
                max_entities: 32,
            }]);
        }

        let batches = stations
            .iter()
            .zip(&indexes)
            .zip(&viewers)
            .map(|((station, index), viewers)| {
                StationReplicationBatch::new(station, index, viewers)
            })
            .collect::<Vec<_>>();
        let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
            .expect("pool builds");
        let mut parallel_scratch = ParallelReplicationScratch::new();
        let parallel = pool.plan_station_range_batches(
            &batches,
            &policies,
            ReplicationBudget::default(),
            &mut parallel_scratch,
        );

        for (batch_index, batch) in batches.iter().enumerate() {
            let mut serial_scratch = ReplicationScratch::default();
            let serial = ReplicationPlanner::plan_for_viewers_with_scratch(
                batch.station,
                batch.index,
                &policies,
                batch.viewers,
                &RangeOnlyVisibility,
                ReplicationBudget::default(),
                &mut serial_scratch,
            );
            assert_eq!(parallel.batches[batch_index].plans, serial.plans);
        }
        assert_eq!(parallel.stats.viewers, 2);
        assert_eq!(parallel_scratch.lanes(), 2);
    }

    #[test]
    fn ordered_map_retains_input_order() {
        let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
            .expect("pool builds");

        let output = pool.map_ordered(vec![3_u32, 1, 2], |value| value * value);

        assert_eq!(output, vec![9, 1, 4]);
    }
}
