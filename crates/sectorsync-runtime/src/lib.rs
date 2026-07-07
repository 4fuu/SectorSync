//! Multi-station orchestration helpers for SectorSync.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use sectorsync_core::prelude::{
    BarrierId, BarrierScope, BarrierState, CellCoord3, CellIndex, CommandQueueMode, EntityHandle,
    EntityId, EventQueueError, EventQueueLimits, EventQueues, HandoffTransfer, HotspotDecision,
    HotspotPlanner, HotspotSeverity, HotspotThresholds, OwnerEpoch, PushOutcome, RuntimeBarrier,
    SnapshotVersion, SplitProposal, Station, StationError, StationEvent, StationId,
    StationLoadSample, StationSnapshot, Tick,
};

/// Small in-process station collection for simulations and embedders.
#[derive(Clone, Debug, Default)]
pub struct StationSet {
    stations: Vec<Station>,
}

impl StationSet {
    /// Adds a station to the collection.
    pub fn push(&mut self, station: Station) {
        self.stations.push(station);
    }

    /// Gets a station by id.
    pub fn get(&self, station_id: StationId) -> Option<&Station> {
        self.stations
            .iter()
            .find(|station| station.config().station_id == station_id)
    }

    /// Gets a mutable station by id.
    pub fn get_mut(&mut self, station_id: StationId) -> Option<&mut Station> {
        self.stations
            .iter_mut()
            .find(|station| station.config().station_id == station_id)
    }

    /// Gets two distinct mutable stations by id.
    pub fn get_pair_mut(
        &mut self,
        left_id: StationId,
        right_id: StationId,
    ) -> Option<(&mut Station, &mut Station)> {
        if left_id == right_id {
            return None;
        }

        let left_index = self
            .stations
            .iter()
            .position(|station| station.config().station_id == left_id)?;
        let right_index = self
            .stations
            .iter()
            .position(|station| station.config().station_id == right_id)?;

        if left_index < right_index {
            let (left, right) = self.stations.split_at_mut(right_index);
            Some((&mut left[left_index], &mut right[0]))
        } else {
            let (left, right) = self.stations.split_at_mut(left_index);
            Some((&mut right[0], &mut left[right_index]))
        }
    }

    /// Iterates over stations.
    pub fn iter(&self) -> impl Iterator<Item = &Station> {
        self.stations.iter()
    }

    /// Iterates mutably over stations.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Station> {
        self.stations.iter_mut()
    }

    /// Returns station ids matching a barrier scope.
    pub fn station_ids_in_scope(&self, scope: BarrierScope) -> Vec<StationId> {
        self.stations
            .iter()
            .filter(|station| match scope {
                BarrierScope::Instance(instance_id) => station.config().instance_id == instance_id,
                BarrierScope::Station(station_id) => station.config().station_id == station_id,
            })
            .map(|station| station.config().station_id)
            .collect()
    }

    /// Number of stations.
    pub fn len(&self) -> usize {
        self.stations.len()
    }

    /// Returns whether no stations are registered.
    pub fn is_empty(&self) -> bool {
        self.stations.is_empty()
    }
}

/// Station-local spatial indexes keyed by station id.
#[derive(Clone, Debug, Default)]
pub struct StationIndexSet {
    indexes: Vec<(StationId, CellIndex)>,
}

impl StationIndexSet {
    /// Adds or replaces one station index.
    pub fn insert(&mut self, station_id: StationId, index: CellIndex) {
        if let Some((_, existing)) = self.indexes.iter_mut().find(|(id, _)| *id == station_id) {
            *existing = index;
        } else {
            self.indexes.push((station_id, index));
        }
    }

    /// Gets one station index.
    pub fn get(&self, station_id: StationId) -> Option<&CellIndex> {
        self.indexes
            .iter()
            .find(|(id, _)| *id == station_id)
            .map(|(_, index)| index)
    }

    /// Gets one mutable station index.
    pub fn get_mut(&mut self, station_id: StationId) -> Option<&mut CellIndex> {
        self.indexes
            .iter_mut()
            .find(|(id, _)| *id == station_id)
            .map(|(_, index)| index)
    }

    /// Gets two distinct mutable station indexes.
    pub fn get_pair_mut(
        &mut self,
        left_id: StationId,
        right_id: StationId,
    ) -> Option<(&mut CellIndex, &mut CellIndex)> {
        if left_id == right_id {
            return None;
        }

        let left_index = self.indexes.iter().position(|(id, _)| *id == left_id)?;
        let right_index = self.indexes.iter().position(|(id, _)| *id == right_id)?;

        if left_index < right_index {
            let (left, right) = self.indexes.split_at_mut(right_index);
            Some((&mut left[left_index].1, &mut right[0].1))
        } else {
            let (left, right) = self.indexes.split_at_mut(left_index);
            Some((&mut right[0].1, &mut left[right_index].1))
        }
    }

    /// Number of indexes.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Returns whether no indexes are registered.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }
}

/// Result of an in-process entity owner migration.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityMigrationReport {
    /// Transfer payload used for the migration.
    pub transfer: HandoffTransfer,
    /// Source-side ghost handle after commit.
    pub source_ghost: EntityHandle,
    /// Target-side authoritative handle after commit.
    pub target_owner: EntityHandle,
}

/// Entity migration error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntityMigrationError {
    /// Source and target station ids must differ.
    SameSourceAndTarget(StationId),
    /// Source station was not found.
    MissingSource(StationId),
    /// Target station was not found.
    MissingTarget(StationId),
    /// Station-level operation failed.
    Station(StationError),
}

impl core::fmt::Display for EntityMigrationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::SameSourceAndTarget(id) => {
                write!(f, "source and target station are both {}", id.get())
            }
            Self::MissingSource(id) => write!(f, "source station {} is missing", id.get()),
            Self::MissingTarget(id) => write!(f, "target station {} is missing", id.get()),
            Self::Station(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for EntityMigrationError {}

impl From<StationError> for EntityMigrationError {
    fn from(value: StationError) -> Self {
        Self::Station(value)
    }
}

/// Runtime helper for in-process station-to-station owner migration.
#[derive(Clone, Copy, Debug, Default)]
pub struct EntityMigrationExecutor;

impl EntityMigrationExecutor {
    /// Migrates one authoritative entity from source station to target station.
    pub fn migrate_entity(
        stations: &mut StationSet,
        entity_id: EntityId,
        source_station: StationId,
        target_station: StationId,
        ghost_ttl_ticks: u64,
    ) -> Result<EntityMigrationReport, EntityMigrationError> {
        if source_station == target_station {
            return Err(EntityMigrationError::SameSourceAndTarget(source_station));
        }

        if stations.get(source_station).is_none() {
            return Err(EntityMigrationError::MissingSource(source_station));
        }
        if stations.get(target_station).is_none() {
            return Err(EntityMigrationError::MissingTarget(target_station));
        }

        let (source, target) = stations
            .get_pair_mut(source_station, target_station)
            .expect("stations were checked above");
        let target_epoch = next_target_epoch(target);
        let source_ghost_expires_at =
            Tick::new(source.tick().get().saturating_add(ghost_ttl_ticks));
        let transfer = source.prepare_outgoing_handoff(
            entity_id,
            target_station,
            target_epoch,
            source_ghost_expires_at,
        )?;
        target.prewarm_handoff_ghost(&transfer)?;
        let target_owner = target.commit_incoming_handoff(transfer.clone())?;
        let source_ghost = source.commit_outgoing_handoff(&transfer)?;

        Ok(EntityMigrationReport {
            transfer,
            source_ghost,
            target_owner,
        })
    }
}

fn next_target_epoch(station: &mut Station) -> OwnerEpoch {
    station.next_owner_epoch()
}

/// Dynamic ownership table for fixed 3D cells.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CellOwnershipTable {
    owners: BTreeMap<CellCoord3, StationId>,
}

impl CellOwnershipTable {
    /// Assigns one cell to a station and returns the previous owner.
    pub fn assign(&mut self, cell: CellCoord3, station_id: StationId) -> Option<StationId> {
        self.owners.insert(cell, station_id)
    }

    /// Returns the current owner for one cell.
    pub fn owner_of(&self, cell: CellCoord3) -> Option<StationId> {
        self.owners.get(&cell).copied()
    }

    /// Applies a split proposal by assigning all proposed cells to `target_station`.
    pub fn apply_split(
        &mut self,
        proposal: &SplitProposal,
        target_station: StationId,
    ) -> CellOwnershipUpdate {
        let mut moved_cells = Vec::new();
        for cell in &proposal.cells_to_move {
            let previous = self.assign(*cell, target_station);
            if previous != Some(target_station) {
                moved_cells.push(*cell);
            }
        }
        CellOwnershipUpdate {
            source_station: proposal.source_station,
            target_station,
            moved_cells,
        }
    }

    /// Number of explicitly assigned cells.
    pub fn len(&self) -> usize {
        self.owners.len()
    }

    /// Returns whether no cells are explicitly assigned.
    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
}

/// Result of applying cell ownership changes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CellOwnershipUpdate {
    /// Previous/source station.
    pub source_station: StationId,
    /// New/target station.
    pub target_station: StationId,
    /// Cells whose owner changed.
    pub moved_cells: Vec<CellCoord3>,
}

/// Result of migrating entities indexed by moved cells.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CellMigrationReport {
    /// Source station.
    pub source_station: StationId,
    /// Target station.
    pub target_station: StationId,
    /// Cells scanned for owner entities.
    pub scanned_cells: Vec<CellCoord3>,
    /// Entity migrations that were committed.
    pub entity_migrations: Vec<EntityMigrationReport>,
    /// Candidate handles that no longer resolved to an entity.
    pub skipped_missing_handles: usize,
    /// Candidate entities skipped because they were ghosts or non-authoritative.
    pub skipped_non_owned: usize,
    /// Duplicate candidate entities skipped after first occurrence.
    pub skipped_duplicate_entities: usize,
}

/// Cell-level migration error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellMigrationError {
    /// Entity migration failed.
    Entity(EntityMigrationError),
    /// Target owner record was not found after a successful migration.
    MissingTargetRecord(EntityId),
    /// Source ghost record was not found after a successful migration.
    MissingSourceRecord(EntityId),
}

impl core::fmt::Display for CellMigrationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Entity(error) => write!(f, "{error}"),
            Self::MissingTargetRecord(id) => {
                write!(f, "target owner record for entity {} is missing", id.get())
            }
            Self::MissingSourceRecord(id) => {
                write!(f, "source ghost record for entity {} is missing", id.get())
            }
        }
    }
}

impl std::error::Error for CellMigrationError {}

impl From<EntityMigrationError> for CellMigrationError {
    fn from(value: EntityMigrationError) -> Self {
        Self::Entity(value)
    }
}

/// Executes cell-level ownership migration using station-local indexes.
#[derive(Clone, Copy, Debug, Default)]
pub struct CellMigrationExecutor;

impl CellMigrationExecutor {
    /// Migrates owned entities found in `cells` from source station to target station.
    pub fn migrate_cells(
        stations: &mut StationSet,
        source_index: &mut CellIndex,
        target_index: &mut CellIndex,
        source_station: StationId,
        target_station: StationId,
        cells: &[CellCoord3],
        ghost_ttl_ticks: u64,
    ) -> Result<CellMigrationReport, CellMigrationError> {
        let mut report = CellMigrationReport {
            source_station,
            target_station,
            scanned_cells: cells.to_vec(),
            ..CellMigrationReport::default()
        };
        let mut seen_handles = BTreeSet::new();
        let mut entity_ids = Vec::new();

        {
            let source = stations
                .get(source_station)
                .ok_or(EntityMigrationError::MissingSource(source_station))?;
            for cell in cells {
                for handle in source_index.handles_in_cell(*cell) {
                    if !seen_handles.insert(handle) {
                        report.skipped_duplicate_entities += 1;
                        continue;
                    }
                    let Some(record) = source.get(handle) else {
                        report.skipped_missing_handles += 1;
                        continue;
                    };
                    if record.is_owned() {
                        entity_ids.push(record.id);
                    } else {
                        report.skipped_non_owned += 1;
                    }
                }
            }
        }

        let mut seen_entities = BTreeSet::new();
        for entity_id in entity_ids {
            if !seen_entities.insert(entity_id) {
                report.skipped_duplicate_entities += 1;
                continue;
            }
            let migration = EntityMigrationExecutor::migrate_entity(
                stations,
                entity_id,
                source_station,
                target_station,
                ghost_ttl_ticks,
            )?;

            {
                let target = stations
                    .get(target_station)
                    .ok_or(EntityMigrationError::MissingTarget(target_station))?;
                let target_record = target
                    .get(migration.target_owner)
                    .ok_or(CellMigrationError::MissingTargetRecord(entity_id))?;
                target_index.upsert(
                    migration.target_owner,
                    target_record.position,
                    target_record.bounds,
                );
            }

            {
                let source = stations
                    .get(source_station)
                    .ok_or(EntityMigrationError::MissingSource(source_station))?;
                let source_record = source
                    .get(migration.source_ghost)
                    .ok_or(CellMigrationError::MissingSourceRecord(entity_id))?;
                source_index.upsert(
                    migration.source_ghost,
                    source_record.position,
                    source_record.bounds,
                );
            }

            report.entity_migrations.push(migration);
        }

        Ok(report)
    }
}

/// Automatic split scheduler configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SplitSchedulerConfig {
    /// Hotspot thresholds.
    pub thresholds: HotspotThresholds,
    /// Maximum split actions to create per scheduling pass.
    pub max_actions_per_pass: usize,
    /// Maximum cells to move in each split action.
    pub max_cells_per_action: usize,
    /// Source ghost TTL used during migration execution.
    pub ghost_ttl_ticks: u64,
}

impl Default for SplitSchedulerConfig {
    fn default() -> Self {
        Self {
            thresholds: HotspotThresholds::default(),
            max_actions_per_pass: 4,
            max_cells_per_action: 4,
            ghost_ttl_ticks: 4,
        }
    }
}

/// One scheduled split action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitAction {
    /// Source station selected for split.
    pub source_station: StationId,
    /// Target station selected to receive cells.
    pub target_station: StationId,
    /// Cell split proposal.
    pub proposal: SplitProposal,
}

/// Split schedule produced from a load snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SplitSchedule {
    /// Hotspot decisions produced for every input station.
    pub decisions: Vec<HotspotDecision>,
    /// Actions selected for execution.
    pub actions: Vec<SplitAction>,
    /// Hot stations skipped because no distinct target existed.
    pub skipped_no_target: usize,
    /// Hot stations skipped because no cells were proposed.
    pub skipped_no_cells: usize,
}

/// Result of executing a split schedule.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SplitScheduleExecutionReport {
    /// Ownership changes applied.
    pub ownership_updates: Vec<CellOwnershipUpdate>,
    /// Cell migration reports.
    pub cell_migrations: Vec<CellMigrationReport>,
}

/// Split schedule execution error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitScheduleExecutionError {
    /// Source index is missing.
    MissingSourceIndex(StationId),
    /// Target index is missing.
    MissingTargetIndex(StationId),
    /// Cell migration failed.
    CellMigration(CellMigrationError),
}

impl core::fmt::Display for SplitScheduleExecutionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingSourceIndex(id) => write!(f, "source index {} is missing", id.get()),
            Self::MissingTargetIndex(id) => write!(f, "target index {} is missing", id.get()),
            Self::CellMigration(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for SplitScheduleExecutionError {}

impl From<CellMigrationError> for SplitScheduleExecutionError {
    fn from(value: CellMigrationError) -> Self {
        Self::CellMigration(value)
    }
}

/// Conservative automatic split scheduler.
#[derive(Clone, Copy, Debug)]
pub struct SplitScheduler {
    /// Scheduler configuration.
    pub config: SplitSchedulerConfig,
}

impl SplitScheduler {
    /// Creates a split scheduler.
    pub const fn new(config: SplitSchedulerConfig) -> Self {
        Self { config }
    }

    /// Plans split actions from station load samples.
    pub fn plan(&self, samples: &[StationLoadSample]) -> SplitSchedule {
        let decisions = samples
            .iter()
            .map(|sample| HotspotPlanner::evaluate(sample, self.config.thresholds))
            .collect::<Vec<_>>();
        let mut schedule = SplitSchedule {
            decisions,
            ..SplitSchedule::default()
        };

        for source in samples {
            if schedule.actions.len() >= self.config.max_actions_per_pass {
                break;
            }
            let Some(source_decision) = schedule
                .decisions
                .iter()
                .find(|decision| decision.station_id == source.station_id)
            else {
                continue;
            };
            if source_decision.severity != HotspotSeverity::Hot {
                continue;
            }

            let Some(target) = select_split_target(source.station_id, samples, &schedule.decisions)
            else {
                schedule.skipped_no_target += 1;
                continue;
            };
            let proposal =
                HotspotPlanner::propose_cell_split(source, self.config.max_cells_per_action);
            if proposal.cells_to_move.is_empty() {
                schedule.skipped_no_cells += 1;
                continue;
            }
            schedule.actions.push(SplitAction {
                source_station: source.station_id,
                target_station: target.station_id,
                proposal,
            });
        }

        schedule
    }

    /// Executes a split schedule by applying ownership updates and migrating entities.
    pub fn execute(
        &self,
        schedule: &SplitSchedule,
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
    ) -> Result<SplitScheduleExecutionReport, SplitScheduleExecutionError> {
        let mut report = SplitScheduleExecutionReport::default();

        for action in &schedule.actions {
            if indexes.get(action.source_station).is_none() {
                return Err(SplitScheduleExecutionError::MissingSourceIndex(
                    action.source_station,
                ));
            }
            if indexes.get(action.target_station).is_none() {
                return Err(SplitScheduleExecutionError::MissingTargetIndex(
                    action.target_station,
                ));
            }

            let update = ownership.apply_split(&action.proposal, action.target_station);
            let (source_index, target_index) = indexes
                .get_pair_mut(action.source_station, action.target_station)
                .expect("indexes were checked above");
            let migration = CellMigrationExecutor::migrate_cells(
                stations,
                source_index,
                target_index,
                action.source_station,
                action.target_station,
                &update.moved_cells,
                self.config.ghost_ttl_ticks,
            )?;
            report.ownership_updates.push(update);
            report.cell_migrations.push(migration);
        }

        Ok(report)
    }
}

impl Default for SplitScheduler {
    fn default() -> Self {
        Self::new(SplitSchedulerConfig::default())
    }
}

fn select_split_target<'a>(
    source_station: StationId,
    samples: &'a [StationLoadSample],
    decisions: &[HotspotDecision],
) -> Option<&'a StationLoadSample> {
    let normal_target = samples
        .iter()
        .filter(|sample| sample.station_id != source_station)
        .filter(|sample| {
            decisions
                .iter()
                .find(|decision| decision.station_id == sample.station_id)
                .is_some_and(|decision| decision.severity == HotspotSeverity::Normal)
        })
        .min_by_key(|sample| station_load_score(sample));

    normal_target.or_else(|| {
        samples
            .iter()
            .filter(|sample| sample.station_id != source_station)
            .min_by_key(|sample| station_load_score(sample))
    })
}

fn station_load_score(sample: &StationLoadSample) -> u64 {
    (sample.total_entities() as u64)
        .saturating_mul(8)
        .saturating_add((sample.subscribers as u64).saturating_mul(4))
        .saturating_add(sample.queued_events as u64)
        .saturating_add((sample.estimated_bytes / 256) as u64)
        .saturating_add(sample.tick_cost_units)
}

/// Event router statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventRouterStats {
    /// Events accepted by target queues.
    pub routed_events: usize,
    /// Ready events drained for station application.
    pub drained_events: usize,
    /// Best-effort events dropped by bounded target queues.
    pub dropped_best_effort_events: usize,
}

/// Event router error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventRouterError {
    /// Target station was not registered with the router.
    MissingTarget(StationId),
    /// Underlying target queue rejected the event.
    Queue(EventQueueError),
}

impl core::fmt::Display for EventRouterError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingTarget(id) => write!(f, "event target station {} is missing", id.get()),
            Self::Queue(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for EventRouterError {}

impl From<EventQueueError> for EventRouterError {
    fn from(value: EventQueueError) -> Self {
        Self::Queue(value)
    }
}

/// In-process station event router.
#[derive(Clone, Debug)]
pub struct EventRouter {
    limits: EventQueueLimits,
    queues: BTreeMap<StationId, EventQueues>,
    stats: EventRouterStats,
}

impl EventRouter {
    /// Creates an empty event router.
    pub fn new(limits: EventQueueLimits) -> Self {
        Self {
            limits,
            queues: BTreeMap::new(),
            stats: EventRouterStats::default(),
        }
    }

    /// Registers a station target queue.
    pub fn register_station(&mut self, station_id: StationId) {
        self.queues
            .entry(station_id)
            .or_insert_with(|| EventQueues::new(self.limits));
    }

    /// Registers all stations in a set.
    pub fn register_stations(&mut self, stations: &StationSet) {
        for station in stations.iter() {
            self.register_station(station.config().station_id);
        }
    }

    /// Routes an event to its target station queue.
    pub fn route(&mut self, event: StationEvent) -> Result<PushOutcome, EventRouterError> {
        let queue = self
            .queues
            .get_mut(&event.target)
            .ok_or(EventRouterError::MissingTarget(event.target))?;
        let outcome = queue.push(event)?;
        self.stats.routed_events += 1;
        if outcome == PushOutcome::DroppedOldestBestEffort {
            self.stats.dropped_best_effort_events += 1;
        }
        Ok(outcome)
    }

    /// Drains events whose `target_tick` is ready for application.
    pub fn drain_ready(
        &mut self,
        station_id: StationId,
        current_tick: Tick,
    ) -> Result<Vec<StationEvent>, EventRouterError> {
        let queue = self
            .queues
            .get_mut(&station_id)
            .ok_or(EventRouterError::MissingTarget(station_id))?;
        let mut ready = Vec::new();
        let mut delayed = Vec::new();

        while let Some(event) = queue.pop_next() {
            if event.target_tick <= current_tick {
                ready.push(event);
            } else {
                delayed.push(event);
            }
        }

        for event in delayed {
            queue.push(event)?;
        }
        self.stats.drained_events += ready.len();
        Ok(ready)
    }

    /// Returns queued event count for one station.
    pub fn queued_len(&self, station_id: StationId) -> Option<usize> {
        self.queues.get(&station_id).map(EventQueues::len)
    }

    /// Returns router statistics.
    pub const fn stats(&self) -> EventRouterStats {
        self.stats
    }
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new(EventQueueLimits::default())
    }
}

/// Basic in-process station scheduler.
#[derive(Clone, Debug, Default)]
pub struct StationScheduler {
    /// Total station ticks advanced by this scheduler.
    pub advanced_ticks: u64,
}

impl StationScheduler {
    /// Advances every station by one tick.
    pub fn advance_all(&mut self, stations: &mut StationSet) {
        for station in stations.iter_mut() {
            station.advance_tick();
            self.advanced_ticks = self.advanced_ticks.saturating_add(1);
        }
    }

    /// Drains router events ready for each station's current tick.
    pub fn drain_ready_events(
        &mut self,
        stations: &StationSet,
        router: &mut EventRouter,
    ) -> Result<Vec<StationEvent>, EventRouterError> {
        let mut events = Vec::new();
        for station in stations.iter() {
            events.extend(router.drain_ready(station.config().station_id, station.tick())?);
        }
        Ok(events)
    }
}

/// Per-station progress inside a full runtime barrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StationBarrierPhase {
    /// Station is part of the barrier but has not reached the target tick.
    WaitingTick,
    /// Station reached the target tick and is frozen.
    Frozen,
    /// Station has resumed.
    Resumed,
}

/// Barrier progress summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BarrierProgress {
    /// Barrier state.
    pub state: BarrierState,
    /// Number of stations covered by the barrier.
    pub station_count: usize,
    /// Number of stations frozen.
    pub frozen_count: usize,
    /// Target tick selected for the barrier.
    pub target_tick: Tick,
}

/// Runtime barrier metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BarrierMetrics {
    /// Number of stations covered by this barrier.
    pub station_count: usize,
    /// Number of snapshots exported while frozen.
    pub snapshots_exported: usize,
    /// Number of times polling observed at least one station still waiting.
    pub waiting_polls: u64,
    /// Number of times polling observed a fully frozen barrier.
    pub frozen_polls: u64,
}

/// Runtime barrier execution error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierRuntimeError {
    /// A barrier is already active.
    AlreadyActive(BarrierId),
    /// No barrier is active.
    NoActiveBarrier,
    /// Barrier scope matched no stations.
    EmptyScope(BarrierScope),
    /// Requested operation requires frozen state.
    NotFrozen(BarrierState),
    /// A station covered by the barrier is missing.
    MissingStation(StationId),
}

impl core::fmt::Display for BarrierRuntimeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AlreadyActive(id) => write!(f, "barrier {} is already active", id.get()),
            Self::NoActiveBarrier => f.write_str("no active barrier"),
            Self::EmptyScope(scope) => write!(f, "barrier scope {scope:?} contains no stations"),
            Self::NotFrozen(state) => {
                write!(f, "barrier operation requires Frozen state, got {state:?}")
            }
            Self::MissingStation(id) => write!(f, "barrier station {} is missing", id.get()),
        }
    }
}

impl std::error::Error for BarrierRuntimeError {}

/// Full runtime barrier executor for in-process station sets.
#[derive(Clone, Debug, Default)]
pub struct BarrierController {
    active: Option<RuntimeBarrier>,
    phases: BTreeMap<StationId, StationBarrierPhase>,
    metrics: BarrierMetrics,
}

impl BarrierController {
    /// Returns the active barrier, if any.
    pub const fn active(&self) -> Option<RuntimeBarrier> {
        self.active
    }

    /// Requests a barrier over stations matching `scope`.
    pub fn request(
        &mut self,
        stations: &StationSet,
        id: BarrierId,
        scope: BarrierScope,
        target_tick: Tick,
        command_mode: CommandQueueMode,
    ) -> Result<BarrierProgress, BarrierRuntimeError> {
        if let Some(active) = self.active {
            return Err(BarrierRuntimeError::AlreadyActive(active.id));
        }

        let station_ids = stations.station_ids_in_scope(scope);
        if station_ids.is_empty() {
            return Err(BarrierRuntimeError::EmptyScope(scope));
        }

        let requested_at = station_ids
            .iter()
            .filter_map(|station_id| stations.get(*station_id).map(Station::tick))
            .map(Tick::get)
            .max()
            .map(Tick::new)
            .unwrap_or(Tick::new(0));

        let mut barrier =
            RuntimeBarrier::requested(id, scope, requested_at, target_tick, command_mode);
        barrier.wait_for_tick_boundary();

        self.metrics = BarrierMetrics {
            station_count: station_ids.len(),
            ..BarrierMetrics::default()
        };
        self.phases.clear();
        for station_id in station_ids {
            self.phases
                .insert(station_id, StationBarrierPhase::WaitingTick);
        }
        self.active = Some(barrier);

        Ok(self.progress())
    }

    /// Polls station ticks and freezes the barrier once all covered stations are aligned.
    pub fn poll(&mut self, stations: &StationSet) -> Result<BarrierProgress, BarrierRuntimeError> {
        let Some(mut barrier) = self.active else {
            return Err(BarrierRuntimeError::NoActiveBarrier);
        };

        if matches!(barrier.state, BarrierState::Frozen) {
            self.metrics.frozen_polls = self.metrics.frozen_polls.saturating_add(1);
            return Ok(self.progress());
        }

        let mut all_ready = true;
        for (station_id, phase) in &mut self.phases {
            let station = stations
                .get(*station_id)
                .ok_or(BarrierRuntimeError::MissingStation(*station_id))?;
            if station.tick() >= barrier.target_tick {
                *phase = StationBarrierPhase::Frozen;
            } else {
                all_ready = false;
            }
        }

        if all_ready {
            barrier.freeze();
            self.active = Some(barrier);
            self.metrics.frozen_polls = self.metrics.frozen_polls.saturating_add(1);
        } else {
            self.metrics.waiting_polls = self.metrics.waiting_polls.saturating_add(1);
        }

        Ok(self.progress())
    }

    /// Exports station snapshots while the barrier is frozen.
    pub fn export_snapshots(
        &mut self,
        stations: &StationSet,
        version: SnapshotVersion,
    ) -> Result<Vec<StationSnapshot>, BarrierRuntimeError> {
        let barrier = self.active.ok_or(BarrierRuntimeError::NoActiveBarrier)?;
        if barrier.state != BarrierState::Frozen {
            return Err(BarrierRuntimeError::NotFrozen(barrier.state));
        }

        let mut snapshots = Vec::with_capacity(self.phases.len());
        for station_id in self.phases.keys().copied() {
            let station = stations
                .get(station_id)
                .ok_or(BarrierRuntimeError::MissingStation(station_id))?;
            snapshots.push(station.snapshot(version.clone()));
        }
        self.metrics.snapshots_exported = self
            .metrics
            .snapshots_exported
            .saturating_add(snapshots.len());
        Ok(snapshots)
    }

    /// Resumes all stations covered by the barrier and returns final metrics.
    pub fn resume(&mut self) -> Result<BarrierMetrics, BarrierRuntimeError> {
        let Some(mut barrier) = self.active else {
            return Err(BarrierRuntimeError::NoActiveBarrier);
        };
        if barrier.state != BarrierState::Frozen {
            return Err(BarrierRuntimeError::NotFrozen(barrier.state));
        }

        barrier.resume();
        for phase in self.phases.values_mut() {
            *phase = StationBarrierPhase::Resumed;
        }
        barrier.finish();
        let metrics = self.metrics;
        self.active = None;
        self.phases.clear();
        self.metrics = BarrierMetrics::default();
        Ok(metrics)
    }

    /// Returns current barrier progress.
    pub fn progress(&self) -> BarrierProgress {
        let state = self
            .active
            .map_or(BarrierState::Running, |barrier| barrier.state);
        let target_tick = self
            .active
            .map_or(Tick::new(0), |barrier| barrier.target_tick);
        let frozen_count = self
            .phases
            .values()
            .filter(|phase| matches!(phase, StationBarrierPhase::Frozen))
            .count();

        BarrierProgress {
            state,
            station_count: self.phases.len(),
            frozen_count,
            target_tick,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sectorsync_core::prelude::{
        Bounds, CellCoord3, CellLoadSample, EventId, EventKind, EventPriority, GridSpec,
        HotspotThresholds, InstanceId, NodeId, PolicyId, Position3, StationConfig,
        StationLoadSample,
    };

    fn station(station_id: u32, instance_id: u64) -> Station {
        Station::new(StationConfig {
            station_id: StationId::new(station_id),
            node_id: NodeId::new(0),
            instance_id: InstanceId::new(instance_id),
            tick_rate_hz: 20,
        })
    }

    #[test]
    fn barrier_freezes_snapshots_and_resumes_instance_scope() {
        let mut stations = StationSet::default();
        stations.push(station(1, 10));
        stations.push(station(2, 10));

        for station in stations.iter_mut() {
            station.advance_tick();
            station.advance_tick();
        }

        let mut controller = BarrierController::default();
        let requested = controller
            .request(
                &stations,
                BarrierId::new(7),
                BarrierScope::Instance(InstanceId::new(10)),
                Tick::new(2),
                CommandQueueMode::Buffer,
            )
            .expect("request should work");
        assert_eq!(requested.state, BarrierState::WaitingTickBoundary);

        let frozen = controller.poll(&stations).expect("poll should work");
        assert_eq!(frozen.state, BarrierState::Frozen);
        assert_eq!(frozen.frozen_count, 2);

        let snapshots = controller
            .export_snapshots(&stations, SnapshotVersion::default())
            .expect("snapshot should work while frozen");
        assert_eq!(snapshots.len(), 2);

        let metrics = controller.resume().expect("resume should work");
        assert_eq!(metrics.station_count, 2);
        assert_eq!(metrics.snapshots_exported, 2);
        assert_eq!(controller.progress().state, BarrierState::Running);
    }

    #[test]
    fn migration_executor_moves_owner_and_leaves_source_ghost() {
        let mut stations = StationSet::default();
        let mut source = station(1, 10);
        source
            .spawn_owned(
                EntityId::new(99),
                Position3::new(1.0, 2.0, 3.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        stations.push(source);
        stations.push(station(2, 10));

        let report = EntityMigrationExecutor::migrate_entity(
            &mut stations,
            EntityId::new(99),
            StationId::new(1),
            StationId::new(2),
            4,
        )
        .expect("migration should work");

        assert_eq!(report.transfer.target_station, StationId::new(2));
        assert!(
            !stations
                .get(StationId::new(1))
                .expect("source")
                .get_by_id(EntityId::new(99))
                .expect("source ghost")
                .is_owned()
        );
        assert!(
            stations
                .get(StationId::new(2))
                .expect("target")
                .get_by_id(EntityId::new(99))
                .expect("target owner")
                .is_owned()
        );
    }

    #[test]
    fn event_router_delays_until_target_tick_and_scheduler_drains() {
        let mut stations = StationSet::default();
        stations.push(station(1, 10));
        stations.push(station(2, 10));

        let mut router = EventRouter::default();
        router.register_stations(&stations);
        router
            .route(StationEvent {
                id: EventId::new(1),
                source: StationId::new(1),
                target: StationId::new(2),
                source_tick: Tick::new(0),
                target_tick: Tick::new(2),
                priority: EventPriority::Critical,
                kind: EventKind::Custom(7),
            })
            .expect("route should work");

        let mut scheduler = StationScheduler::default();
        scheduler.advance_all(&mut stations);
        let drained = scheduler
            .drain_ready_events(&stations, &mut router)
            .expect("drain should work");
        assert!(drained.is_empty());

        scheduler.advance_all(&mut stations);
        let drained = scheduler
            .drain_ready_events(&stations, &mut router)
            .expect("drain should work");
        assert_eq!(drained.len(), 1);
        assert_eq!(router.stats().routed_events, 1);
        assert_eq!(router.stats().drained_events, 1);
    }

    #[test]
    fn cell_migration_moves_owned_entities_and_updates_indexes() {
        let grid = GridSpec::new(16.0).expect("valid grid");
        let cell = CellCoord3::new(0, 0, 0);
        let mut stations = StationSet::default();
        let mut source = station(1, 10);
        let first = source
            .spawn_owned(
                EntityId::new(1),
                Position3::new(1.0, 1.0, 1.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("first spawn should work");
        let second = source
            .spawn_owned(
                EntityId::new(2),
                Position3::new(2.0, 1.0, 1.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("second spawn should work");
        stations.push(source);
        stations.push(station(2, 10));

        let mut source_index = CellIndex::new(grid);
        source_index.upsert(first, Position3::new(1.0, 1.0, 1.0), Bounds::Point);
        source_index.upsert(second, Position3::new(2.0, 1.0, 1.0), Bounds::Point);
        let mut target_index = CellIndex::new(grid);

        let mut ownership = CellOwnershipTable::default();
        ownership.assign(cell, StationId::new(1));
        let update = ownership.apply_split(
            &SplitProposal {
                source_station: StationId::new(1),
                cells_to_move: vec![cell],
                moved_pressure_score: 10,
            },
            StationId::new(2),
        );
        assert_eq!(ownership.owner_of(cell), Some(StationId::new(2)));
        assert_eq!(update.moved_cells, vec![cell]);

        let report = CellMigrationExecutor::migrate_cells(
            &mut stations,
            &mut source_index,
            &mut target_index,
            StationId::new(1),
            StationId::new(2),
            &update.moved_cells,
            4,
        )
        .expect("cell migration should work");

        assert_eq!(report.entity_migrations.len(), 2);
        assert_eq!(target_index.entity_count(), 2);
        assert!(
            !stations
                .get(StationId::new(1))
                .expect("source")
                .get_by_id(EntityId::new(1))
                .expect("source ghost")
                .is_owned()
        );
        assert!(
            stations
                .get(StationId::new(2))
                .expect("target")
                .get_by_id(EntityId::new(1))
                .expect("target owner")
                .is_owned()
        );
    }

    #[test]
    fn split_scheduler_plans_and_executes_hot_cell_move() {
        let grid = GridSpec::new(16.0).expect("valid grid");
        let hot_cell = CellCoord3::new(0, 0, 0);
        let mut stations = StationSet::default();
        let mut source = station(1, 10);
        let handle = source
            .spawn_owned(
                EntityId::new(1),
                Position3::new(1.0, 1.0, 1.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        stations.push(source);
        stations.push(station(2, 10));

        let mut source_index = CellIndex::new(grid);
        source_index.upsert(handle, Position3::new(1.0, 1.0, 1.0), Bounds::Point);
        let mut indexes = StationIndexSet::default();
        indexes.insert(StationId::new(1), source_index);
        indexes.insert(StationId::new(2), CellIndex::new(grid));

        let samples = vec![
            StationLoadSample {
                station_id: StationId::new(1),
                owned_entities: 100,
                subscribers: 100,
                tick_cost_units: 1000,
                cells: vec![CellLoadSample {
                    cell: hot_cell,
                    owned_entities: 100,
                    subscribers: 100,
                    event_pressure: 10,
                    ..CellLoadSample::default()
                }],
                ..StationLoadSample::default()
            },
            StationLoadSample {
                station_id: StationId::new(2),
                owned_entities: 1,
                cells: vec![CellLoadSample {
                    cell: CellCoord3::new(10, 0, 0),
                    owned_entities: 1,
                    ..CellLoadSample::default()
                }],
                ..StationLoadSample::default()
            },
        ];
        let scheduler = SplitScheduler::new(SplitSchedulerConfig {
            thresholds: HotspotThresholds {
                max_station_entities: 10,
                max_station_subscribers: 10,
                max_cell_pressure: 10,
                ..HotspotThresholds::default()
            },
            max_actions_per_pass: 1,
            max_cells_per_action: 1,
            ghost_ttl_ticks: 4,
        });
        let schedule = scheduler.plan(&samples);
        assert_eq!(schedule.actions.len(), 1);
        assert_eq!(schedule.actions[0].target_station, StationId::new(2));

        let mut ownership = CellOwnershipTable::default();
        ownership.assign(hot_cell, StationId::new(1));
        let report = scheduler
            .execute(&schedule, &mut stations, &mut indexes, &mut ownership)
            .expect("execute should work");

        assert_eq!(ownership.owner_of(hot_cell), Some(StationId::new(2)));
        assert_eq!(report.cell_migrations.len(), 1);
        assert_eq!(report.cell_migrations[0].entity_migrations.len(), 1);
        assert_eq!(
            indexes
                .get(StationId::new(2))
                .expect("target index")
                .entity_count(),
            1
        );
    }
}
