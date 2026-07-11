//! Station-local entity storage and runtime primitives.

use std::collections::HashMap;

use crate::entity::{DirtyMask, EntityRecord, EntityRole, EntityTags};
use crate::handoff::HandoffTransfer;
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
        Self::with_capacity(config, 0)
    }

    /// Creates an empty station with capacity for local entity records.
    pub fn with_capacity(config: StationConfig, entity_capacity: usize) -> Self {
        Self {
            config,
            tick: Tick::new(0),
            owner_epoch: OwnerEpoch::new(0),
            records: Vec::with_capacity(entity_capacity),
            generations: Vec::with_capacity(entity_capacity),
            free: Vec::new(),
            by_id: HashMap::with_capacity(entity_capacity),
        }
    }

    /// Reserves capacity for at least `additional` more local entities.
    pub fn reserve_entities(&mut self, additional: usize) {
        self.records.reserve(additional);
        self.generations.reserve(additional);
        self.by_id.reserve(additional);
    }

    /// Reserves recycled-handle slots for caller-expected despawn churn.
    pub fn reserve_free_handles(&mut self, additional: usize) {
        self.free.reserve(additional);
    }

    /// Entity record slots currently retained without another allocation.
    pub fn entity_capacity(&self) -> usize {
        self.records.capacity().min(self.generations.capacity())
    }

    /// Entity-id lookup entries currently retained without another rehash.
    pub fn id_index_capacity(&self) -> usize {
        self.by_id.capacity()
    }

    /// Recycled handle indexes currently retained without another allocation.
    pub fn free_list_capacity(&self) -> usize {
        self.free.capacity()
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

    /// Reserves and returns the next owner epoch for this station.
    pub fn next_owner_epoch(&mut self) -> OwnerEpoch {
        self.owner_epoch = OwnerEpoch::new(self.owner_epoch.get().saturating_add(1));
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
    #[allow(clippy::too_many_arguments)]
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

    /// Gets a mutable entity record by stable id.
    pub fn get_by_id_mut(&mut self, id: EntityId) -> Option<&mut EntityRecord> {
        let handle = self.by_id.get(&id).copied()?;
        self.get_mut(handle)
    }

    /// Gets a station-local handle by stable id.
    pub fn handle_by_id(&self, id: EntityId) -> Option<EntityHandle> {
        self.by_id.get(&id).copied()
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

    /// Replaces authoritative entity tags and marks tags dirty.
    pub fn set_tags(&mut self, handle: EntityHandle, tags: EntityTags) -> Result<(), StationError> {
        let record = self
            .get_mut(handle)
            .ok_or(StationError::MissingEntityHandle(handle))?;
        if !record.is_owned() {
            return Err(StationError::NotOwner(record.id));
        }
        record.tags = tags;
        record.dirty.insert(DirtyMask::TAGS);
        Ok(())
    }

    /// Clears selected dirty bits for a local entity record.
    pub fn clear_dirty(
        &mut self,
        handle: EntityHandle,
        mask: DirtyMask,
    ) -> Result<(), StationError> {
        let record = self
            .get_mut(handle)
            .ok_or(StationError::MissingEntityHandle(handle))?;
        record.dirty.remove(mask);
        Ok(())
    }

    /// Iterates over live records.
    pub fn iter(&self) -> impl Iterator<Item = &EntityRecord> {
        self.records.iter().filter_map(Option::as_ref)
    }

    /// Removes an entity record by handle.
    pub fn remove(&mut self, handle: EntityHandle) -> Result<EntityRecord, StationError> {
        let index = usize::try_from(handle.index())
            .map_err(|_| StationError::MissingEntityHandle(handle))?;
        let generation = self
            .generations
            .get(index)
            .copied()
            .ok_or(StationError::MissingEntityHandle(handle))?;
        if generation != handle.generation() {
            return Err(StationError::MissingEntityHandle(handle));
        }

        let record = self.records[index]
            .take()
            .ok_or(StationError::MissingEntityHandle(handle))?;
        self.by_id.remove(&record.id);
        self.generations[index] = self.generations[index].saturating_add(1);
        self.free.push(handle.index());
        Ok(record)
    }

    /// Removes an entity record by stable id.
    pub fn remove_by_id(&mut self, id: EntityId) -> Result<EntityRecord, StationError> {
        let handle = self
            .by_id
            .get(&id)
            .copied()
            .ok_or(StationError::MissingEntity(id))?;
        self.remove(handle)
    }

    /// Prepares an outgoing handoff transfer without mutating ownership yet.
    pub fn prepare_outgoing_handoff(
        &self,
        entity_id: EntityId,
        target_station: StationId,
        target_owner_epoch: OwnerEpoch,
        source_ghost_expires_at: Tick,
    ) -> Result<HandoffTransfer, StationError> {
        if target_station == self.config.station_id {
            return Err(StationError::HandoffTargetIsSource(target_station));
        }

        let entity = self
            .get_by_id(entity_id)
            .ok_or(StationError::MissingEntity(entity_id))?;
        if !entity.is_owned() {
            return Err(StationError::NotOwner(entity_id));
        }

        Ok(HandoffTransfer {
            entity_id,
            source_station: self.config.station_id,
            target_station,
            source_owner_epoch: entity.role.owner_epoch(),
            target_owner_epoch,
            prepared_at: self.tick,
            source_ghost_expires_at,
            entity: entity.clone(),
        })
    }

    /// Prewarms or refreshes a target-side ghost before owner commit.
    pub fn prewarm_handoff_ghost(
        &mut self,
        transfer: &HandoffTransfer,
    ) -> Result<EntityHandle, StationError> {
        if transfer.target_station != self.config.station_id {
            return Err(StationError::WrongHandoffTarget {
                expected: self.config.station_id,
                actual: transfer.target_station,
            });
        }

        Ok(self.upsert_ghost(
            transfer.entity_id,
            transfer.entity.position,
            transfer.entity.bounds,
            transfer.entity.policy_id,
            transfer.source_station,
            transfer.source_owner_epoch,
            transfer.source_ghost_expires_at,
        ))
    }

    /// Commits the target side of an incoming handoff and becomes authoritative.
    pub fn commit_incoming_handoff(
        &mut self,
        transfer: HandoffTransfer,
    ) -> Result<EntityHandle, StationError> {
        if transfer.target_station != self.config.station_id {
            return Err(StationError::WrongHandoffTarget {
                expected: self.config.station_id,
                actual: transfer.target_station,
            });
        }

        if let Some(handle) = self.handle_by_id(transfer.entity_id) {
            let record = self
                .get_mut(handle)
                .ok_or(StationError::MissingEntityHandle(handle))?;
            if record.is_owned() {
                return Err(StationError::AlreadyOwner(transfer.entity_id));
            }

            *record = transfer.entity;
            record.handle = handle;
            record.role = EntityRole::Owned {
                owner_epoch: transfer.target_owner_epoch,
            };
            record.dirty.insert(DirtyMask::TRANSFORM);
            self.owner_epoch = transfer.target_owner_epoch;
            return Ok(handle);
        }

        let handle = self.allocate_handle();
        let mut record = transfer.entity;
        record.handle = handle;
        record.role = EntityRole::Owned {
            owner_epoch: transfer.target_owner_epoch,
        };
        record.dirty.insert(DirtyMask::TRANSFORM);
        self.owner_epoch = transfer.target_owner_epoch;
        self.insert_allocated(handle, record);
        Ok(handle)
    }

    /// Commits the source side of an outgoing handoff and keeps a short-lived ghost.
    pub fn commit_outgoing_handoff(
        &mut self,
        transfer: &HandoffTransfer,
    ) -> Result<EntityHandle, StationError> {
        if transfer.source_station != self.config.station_id {
            return Err(StationError::WrongHandoffSource {
                expected: self.config.station_id,
                actual: transfer.source_station,
            });
        }

        let handle = self
            .handle_by_id(transfer.entity_id)
            .ok_or(StationError::MissingEntity(transfer.entity_id))?;
        let record = self
            .get_mut(handle)
            .ok_or(StationError::MissingEntityHandle(handle))?;
        if !record.is_owned() {
            return Err(StationError::NotOwner(transfer.entity_id));
        }

        record.role = EntityRole::Ghost {
            owner_station: transfer.target_station,
            owner_epoch: transfer.target_owner_epoch,
            expires_at: transfer.source_ghost_expires_at,
        };
        record.dirty.insert(DirtyMask::TRANSFORM);
        Ok(handle)
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
    /// Entity id does not exist in this station.
    MissingEntity(EntityId),
    /// Entity handle is missing or stale.
    MissingEntityHandle(EntityHandle),
    /// Operation requires an authoritative entity.
    NotOwner(EntityId),
    /// This station is already authoritative for the entity.
    AlreadyOwner(EntityId),
    /// Handoff target cannot be the source station.
    HandoffTargetIsSource(StationId),
    /// Incoming transfer was addressed to a different target station.
    WrongHandoffTarget {
        /// Expected target station id.
        expected: StationId,
        /// Actual target station id in transfer.
        actual: StationId,
    },
    /// Outgoing transfer was addressed from a different source station.
    WrongHandoffSource {
        /// Expected source station id.
        expected: StationId,
        /// Actual source station id in transfer.
        actual: StationId,
    },
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
            Self::MissingEntity(id) => write!(f, "missing entity id {}", id.get()),
            Self::MissingEntityHandle(handle) => {
                write!(
                    f,
                    "missing entity handle index={} generation={}",
                    handle.index(),
                    handle.generation()
                )
            }
            Self::NotOwner(id) => write!(f, "entity {} is not authoritative here", id.get()),
            Self::AlreadyOwner(id) => {
                write!(f, "entity {} is already authoritative here", id.get())
            }
            Self::HandoffTargetIsSource(station_id) => {
                write!(
                    f,
                    "handoff target {} is the source station",
                    station_id.get()
                )
            }
            Self::WrongHandoffTarget { expected, actual } => write!(
                f,
                "wrong handoff target: expected {}, got {}",
                expected.get(),
                actual.get()
            ),
            Self::WrongHandoffSource { expected, actual } => write!(
                f,
                "wrong handoff source: expected {}, got {}",
                expected.get(),
                actual.get()
            ),
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

    fn config_with_station(station_id: u32) -> StationConfig {
        StationConfig {
            station_id: StationId::new(station_id),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(7),
            tick_rate_hz: 20,
        }
    }

    #[test]
    fn explicit_entity_capacity_is_retained_and_grows_on_request() {
        let mut station = Station::with_capacity(config(), 8);

        assert!(station.entity_capacity() >= 8);
        assert!(station.id_index_capacity() >= 8);
        assert_eq!(station.free_list_capacity(), 0);

        station.reserve_entities(32);
        assert!(station.entity_capacity() >= 32);
        assert!(station.id_index_capacity() >= 32);
        assert_eq!(station.free_list_capacity(), 0);
        station.reserve_free_handles(8);
        assert!(station.free_list_capacity() >= 8);

        let handle = station
            .spawn_owned(
                EntityId::new(1),
                Position3::new(0.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("reserved station should spawn");
        assert_eq!(handle, EntityHandle::new(0, 0));
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

    #[test]
    fn owned_tags_can_be_replaced_and_mark_dirty() {
        let mut station = Station::new(config());
        let handle = station
            .spawn_owned(
                EntityId::new(6),
                Position3::new(0.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        let tags = EntityTags::from_bits(0b101);

        station
            .set_tags(handle, tags)
            .expect("set tags should work");
        let record = station.get(handle).expect("entity should exist");

        assert_eq!(record.tags, tags);
        assert!(record.dirty.contains(DirtyMask::TAGS));
    }

    #[test]
    fn selected_dirty_bits_can_be_cleared() {
        let mut station = Station::new(config());
        let handle = station
            .spawn_owned(
                EntityId::new(8),
                Position3::new(0.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        station
            .set_tags(handle, EntityTags::from_bits(1))
            .expect("set tags should work");

        station
            .clear_dirty(handle, DirtyMask::TAGS)
            .expect("clear dirty should work");
        let record = station.get(handle).expect("entity should exist");

        assert!(!record.dirty.contains(DirtyMask::TAGS));
        assert!(record.dirty.contains(DirtyMask::TRANSFORM));
    }

    #[test]
    fn ghost_tags_cannot_be_replaced_as_owned() {
        let mut station = Station::new(config());
        let handle = station.upsert_ghost(
            EntityId::new(7),
            Position3::new(0.0, 0.0, 0.0),
            Bounds::Point,
            PolicyId::new(0),
            StationId::new(2),
            OwnerEpoch::new(1),
            Tick::new(10),
        );

        let error = station
            .set_tags(handle, EntityTags::from_bits(1))
            .expect_err("ghost tags should fail");
        assert_eq!(error, StationError::NotOwner(EntityId::new(7)));
    }

    #[test]
    fn two_phase_handoff_prewarms_target_and_commits_owner_switch() {
        let mut source = Station::new(config_with_station(1));
        let mut target = Station::new(config_with_station(2));

        source
            .spawn_owned(
                EntityId::new(9),
                Position3::new(10.0, 20.0, 30.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        source.advance_tick();

        let target_epoch = target.next_owner_epoch();
        let transfer = source
            .prepare_outgoing_handoff(
                EntityId::new(9),
                StationId::new(2),
                target_epoch,
                Tick::new(8),
            )
            .expect("prepare should work");

        let ghost_handle = target
            .prewarm_handoff_ghost(&transfer)
            .expect("prewarm should work");
        assert!(!target.get(ghost_handle).expect("ghost exists").is_owned());

        let owner_handle = target
            .commit_incoming_handoff(transfer.clone())
            .expect("incoming commit should work");
        assert!(target.get(owner_handle).expect("owner exists").is_owned());

        let source_handle = source
            .commit_outgoing_handoff(&transfer)
            .expect("outgoing commit should work");
        let source_record = source.get(source_handle).expect("source ghost exists");
        assert!(!source_record.is_owned());
        assert_eq!(source_record.role.owner_epoch(), target_epoch);
    }
}
