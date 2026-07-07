//! Multi-station orchestration helpers for SectorSync.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use sectorsync_core::prelude::{
    BarrierId, BarrierScope, BarrierState, CommandQueueMode, EntityHandle, EntityId,
    HandoffTransfer, OwnerEpoch, RuntimeBarrier, SnapshotVersion, Station, StationError, StationId,
    StationSnapshot, Tick,
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
        Bounds, InstanceId, NodeId, PolicyId, Position3, StationConfig,
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
}
