//! Station-local entity storage and runtime primitives.

use std::collections::HashMap;

use crate::entity::{DirtyMask, EntityRecord, EntityRole};
use crate::ids::{
    EntityHandle, EntityId, InstanceId, NodeId, OwnerEpoch, PolicyId, StationId, Tick,
};
use crate::snapshot::{SnapshotMeta, SnapshotVersion, StationSnapshot};
use crate::spatial::{Bounds, Position3};

/// Station runtime configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationConfig {
    /// Station id.
    pub station_id: StationId,
    /// Node id selected by the embedding application.
    pub node_id: NodeId,
    /// World instance id.
    pub instance_id: InstanceId,
    /// Fixed authoritative tick rate in hertz.
    pub tick_rate_hz: u16,
}

/// Station-local storage and metadata.
#[derive(Clone, Debug)]
pub struct Station {
    config: StationConfig,
    tick: Tick,
    owner_epoch: OwnerEpoch,
    records: Vec<Option<EntityRecord>>,
    generations: Vec<u32>,
    free: Vec<u32>,
    by_id: HashMap<EntityId, EntityHandle>,
}

impl Station {
    /// Creates an empty station.
    pub fn new(config: StationConfig) -> Self {
        Self {
            config,
            tick: Tick::new(0),
            owner_epoch: OwnerEpoch::new(0),
            records: Vec::new(),
            generations: Vec::new(),
            free: Vec::new(),
            by_id: HashMap::new(),
        }
    }

    /// Returns station configuration.
    pub const fn config(&self) -> StationConfig {
        self.config
    }

    /// Current station tick.
    pub const fn tick(&self) -> Tick {
        self.tick
    }

    /// Current owner epoch.
    pub const fn owner_epoch(&self) -> OwnerEpoch {
        self.owner_epoch
    }

    /// Number of live entity records.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Returns whether the station has no entity records.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Advances the authoritative station tick.
    pub fn advance_tick(&mut self) {
        self.tick = Tick::new(self.tick.get().saturating_add(1));
    }

    /// Spawns an authoritative entity in this station.
    pub fn spawn_owned(
        &mut self,
        id: EntityId,
        position: Position3,
        bounds: Bounds,
        policy_id: PolicyId,
    ) -> Result<EntityHandle, StationError> {
        if self.by_id.contains_key(&id) {
            return Err(StationError::DuplicateEntity(id));
        }

        let handle = self.allocate_handle();
        let record = EntityRecord::owned(id, handle, position, bounds, policy_id, self.owner_epoch);
        self.insert_allocated(handle, record);
        Ok(handle)
    }

    /// Inserts or refreshes a read-only ghost entity.
    pub fn upsert_ghost(
        &mut self,
        id: EntityId,
        position: Position3,
        bounds: Bounds,
        policy_id: PolicyId,
        owner_station: StationId,
        owner_epoch: OwnerEpoch,
        expires_at: Tick,
    ) -> EntityHandle {
        if let Some(handle) = self.by_id.get(&id).copied() {
            if let Some(record) = self.get_mut(handle) {
                record.position = position;
                record.bounds = bounds;
                record.policy_id = policy_id;
                record.role = EntityRole::Ghost {
                    owner_station,
                    owner_epoch,
                    expires_at,
                };
                record.dirty.insert(DirtyMask::TRANSFORM);
            }
            return handle;
        }

        let handle = self.allocate_handle();
        let record = EntityRecord::ghost(
            id,
            handle,
            position,
            bounds,
            policy_id,
            owner_station,
            owner_epoch,
            expires_at,
        );
        self.insert_allocated(handle, record);
        handle
    }

    /// Gets an entity by handle if the generation is still valid.
    pub fn get(&self, handle: EntityHandle) -> Option<&EntityRecord> {
        let index = usize::try_from(handle.index()).ok()?;
        let generation = self.generations.get(index).copied()?;
        if generation != handle.generation() {
            return None;
        }
        self.records.get(index)?.as_ref()
    }

    /// Gets a mutable entity record by handle.
    pub fn get_mut(&mut self, handle: EntityHandle) -> Option<&mut EntityRecord> {
        let index = usize::try_from(handle.index()).ok()?;
        let generation = self.generations.get(index).copied()?;
        if generation != handle.generation() {
            return None;
        }
        self.records.get_mut(index)?.as_mut()
    }

    /// Gets an entity by stable id.
    pub fn get_by_id(&self, id: EntityId) -> Option<&EntityRecord> {
        let handle = self.by_id.get(&id).copied()?;
        self.get(handle)
    }

    /// Moves an authoritative entity and marks transform dirty.
    pub fn move_owned(
        &mut self,
        handle: EntityHandle,
        position: Position3,
    ) -> Result<(), StationError> {
        let record = self
            .get_mut(handle)
            .ok_or(StationError::MissingEntityHandle(handle))?;
        if !record.is_owned() {
            return Err(StationError::NotOwner(record.id));
        }
        record.position = position;
        record.dirty.insert(DirtyMask::TRANSFORM);
        Ok(())
    }

    /// Iterates over live records.
    pub fn iter(&self) -> impl Iterator<Item = &EntityRecord> {
        self.records.iter().filter_map(Option::as_ref)
    }

    /// Exports an in-memory station snapshot.
    pub fn snapshot(&self, version: SnapshotVersion) -> StationSnapshot {
        let entities = self.iter().cloned().collect::<Vec<_>>();
        StationSnapshot {
            meta: SnapshotMeta {
                instance_id: self.config.instance_id,
                station_id: self.config.station_id,
                tick: self.tick,
                entity_count: entities.len(),
                owner_epoch: self.owner_epoch,
                version,
            },
            entities,
        }
    }

    /// Restores station state from a snapshot.
    pub fn restore(config: StationConfig, snapshot: StationSnapshot) -> Result<Self, StationError> {
        if snapshot.meta.station_id != config.station_id {
            return Err(StationError::SnapshotStationMismatch {
                expected: config.station_id,
                actual: snapshot.meta.station_id,
            });
        }

        let mut station = Self::new(config);
        station.tick = snapshot.meta.tick;
        station.owner_epoch = snapshot.meta.owner_epoch;

        for mut record in snapshot.entities {
            if station.by_id.contains_key(&record.id) {
                return Err(StationError::DuplicateEntity(record.id));
            }
            let handle = station.allocate_handle();
            record.handle = handle;
            station.insert_allocated(handle, record);
        }

        Ok(station)
    }

    fn allocate_handle(&mut self) -> EntityHandle {
        if let Some(index) = self.free.pop() {
            let generation = self.generations[index as usize];
            EntityHandle::new(index, generation)
        } else {
            let index =
                u32::try_from(self.records.len()).expect("station entity capacity exceeded");
            self.records.push(None);
            self.generations.push(0);
            EntityHandle::new(index, 0)
        }
    }

    fn insert_allocated(&mut self, handle: EntityHandle, record: EntityRecord) {
        let index = handle.index() as usize;
        self.by_id.insert(record.id, handle);
        self.records[index] = Some(record);
    }
}

/// Station operation error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StationError {
    /// Entity id already exists in this station.
    DuplicateEntity(EntityId),
    /// Entity handle is missing or stale.
    MissingEntityHandle(EntityHandle),
    /// Operation requires an authoritative entity.
    NotOwner(EntityId),
    /// Snapshot was captured from a different station.
    SnapshotStationMismatch {
        /// Expected station id.
        expected: StationId,
        /// Actual snapshot station id.
        actual: StationId,
    },
}

impl core::fmt::Display for StationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::DuplicateEntity(id) => write!(f, "duplicate entity id {}", id.get()),
            Self::MissingEntityHandle(handle) => {
                write!(
                    f,
                    "missing entity handle index={} generation={}",
                    handle.index(),
                    handle.generation()
                )
            }
            Self::NotOwner(id) => write!(f, "entity {} is not authoritative here", id.get()),
            Self::SnapshotStationMismatch { expected, actual } => write!(
                f,
                "snapshot station mismatch: expected {}, got {}",
                expected.get(),
                actual.get()
            ),
        }
    }
}

impl std::error::Error for StationError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> StationConfig {
        StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(7),
            tick_rate_hz: 20,
        }
    }

    #[test]
    fn owned_entities_can_move_and_snapshot_restore() {
        let mut station = Station::new(config());
        let handle = station
            .spawn_owned(
                EntityId::new(42),
                Position3::new(1.0, 2.0, 3.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");

        station
            .move_owned(handle, Position3::new(2.0, 3.0, 4.0))
            .expect("owned move should work");
        station.advance_tick();

        let snapshot = station.snapshot(SnapshotVersion::default());
        let restored = Station::restore(config(), snapshot).expect("restore should work");

        assert_eq!(restored.len(), 1);
        assert_eq!(restored.tick(), Tick::new(1));
        assert_eq!(
            restored
                .get_by_id(EntityId::new(42))
                .expect("entity should exist")
                .position,
            Position3::new(2.0, 3.0, 4.0)
        );
    }

    #[test]
    fn ghosts_cannot_be_moved_as_owned() {
        let mut station = Station::new(config());
        let handle = station.upsert_ghost(
            EntityId::new(5),
            Position3::new(0.0, 0.0, 0.0),
            Bounds::Point,
            PolicyId::new(0),
            StationId::new(2),
            OwnerEpoch::new(1),
            Tick::new(10),
        );

        let error = station
            .move_owned(handle, Position3::new(1.0, 0.0, 0.0))
            .expect_err("ghost move should fail");
        assert_eq!(error, StationError::NotOwner(EntityId::new(5)));
    }
}
