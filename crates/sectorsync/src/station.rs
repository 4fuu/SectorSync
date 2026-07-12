//! Coherent Station-local product state.

use sectorsync_core::{
    command::{CommandQueueLimits, CommandQueues},
    component::{ComponentDescriptor, ComponentStore, ComponentStoreError},
    entity::{EntityRecord, EntityTags},
    handoff::HandoffTransfer,
    ids::{EntityHandle, EntityId, OwnerEpoch, PolicyId, StationId, Tick},
    spatial::{Bounds, GridSpec, Position3},
    spatial_index::{CellIndex, CellIndexUpdate, CellIndexUpdateScratch},
    station::{Station, StationConfig, StationError},
};

/// Capacity and topology configuration for [`StationRuntime`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StationRuntimeConfig {
    /// Station authority and tick metadata.
    pub station: StationConfig,
    /// Station-local spatial grid.
    pub grid: GridSpec,
    /// Expected live entity records.
    pub entity_capacity: usize,
    /// Expected occupied spatial cells.
    pub occupied_cell_capacity: usize,
    /// Expected recycled-handle count retained for despawn churn.
    pub free_handle_capacity: usize,
    /// Optional bounded Station-local command queues.
    pub command_queue_limits: Option<CommandQueueLimits>,
}

impl StationRuntimeConfig {
    /// Creates configuration without preallocated entity or cell storage.
    pub const fn new(station: StationConfig, grid: GridSpec) -> Self {
        Self {
            station,
            grid,
            entity_capacity: 0,
            occupied_cell_capacity: 0,
            free_handle_capacity: 0,
            command_queue_limits: None,
        }
    }

    /// Preallocates the expected live entity and occupied-cell counts.
    #[must_use]
    pub const fn with_capacity(
        mut self,
        entity_capacity: usize,
        occupied_cell_capacity: usize,
    ) -> Self {
        self.entity_capacity = entity_capacity;
        self.occupied_cell_capacity = occupied_cell_capacity;
        self
    }

    /// Preallocates recycled handles for expected despawn churn.
    #[must_use]
    pub const fn with_free_handle_capacity(mut self, capacity: usize) -> Self {
        self.free_handle_capacity = capacity;
        self
    }

    /// Adds bounded Station-local command queues to the product state.
    #[must_use]
    pub const fn with_command_queues(mut self, limits: CommandQueueLimits) -> Self {
        self.command_queue_limits = Some(limits);
        self
    }
}

/// One authoritative entity spawn request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpawnEntity {
    /// Stable entity id.
    pub id: EntityId,
    /// Initial world position.
    pub position: Position3,
    /// Spatial bounds.
    pub bounds: Bounds,
    /// Compiled synchronization policy id.
    pub policy_id: PolicyId,
}

/// One read-only ghost upsert request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GhostEntity {
    /// Stable entity id.
    pub id: EntityId,
    /// Latest replicated position.
    pub position: Position3,
    /// Spatial bounds.
    pub bounds: Bounds,
    /// Compiled synchronization policy id.
    pub policy_id: PolicyId,
    /// Authoritative Station.
    pub owner_station: StationId,
    /// Authoritative owner epoch.
    pub owner_epoch: OwnerEpoch,
    /// Tick after which the ghost may be discarded.
    pub expires_at: Tick,
}

/// Result of inserting or refreshing Station-local spatial state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationEntityUpdateReport {
    /// Station-local entity handle.
    pub handle: EntityHandle,
    /// Spatial membership outcome.
    pub index_update: CellIndexUpdate,
}

impl SpawnEntity {
    /// Creates an authoritative spawn request.
    pub const fn new(
        id: EntityId,
        position: Position3,
        bounds: Bounds,
        policy_id: PolicyId,
    ) -> Self {
        Self {
            id,
            position,
            bounds,
            policy_id,
        }
    }
}

/// Result of moving one authoritative entity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationMoveReport {
    /// Spatial membership update performed after the authoritative move.
    pub index_update: CellIndexUpdate,
}

/// Result of removing one Station-local entity and its middleware state.
#[derive(Clone, Debug, PartialEq)]
pub struct DespawnReport {
    /// Removed entity record.
    pub entity: EntityRecord,
    /// Whether the entity had spatial membership to remove.
    pub index_removed: bool,
    /// Component blobs removed with the entity.
    pub components_removed: usize,
}

/// Retained-capacity observation for [`StationRuntime`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StationRuntimeCapacities {
    /// Station entity slots.
    pub station_entities: usize,
    /// Station entity-id lookup entries.
    pub station_id_index: usize,
    /// Station recycled handles.
    pub station_free_handles: usize,
    /// Spatial entity membership entries.
    pub indexed_entities: usize,
    /// Spatial occupied-cell entries.
    pub occupied_cells: usize,
    /// Reusable multi-cell update coordinates.
    pub index_update_cells: usize,
    /// Dense plus sparse component column slots.
    pub component_columns: usize,
    /// Ready command queue slots, or zero when queues are disabled.
    pub command_ready: usize,
    /// Barrier command queue slots, or zero when queues are disabled.
    pub command_barrier: usize,
}

/// Capacity change produced by explicit retained-storage reclamation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StationRuntimeReclaimReport {
    /// Capacities before reclamation.
    pub before: StationRuntimeCapacities,
    /// Capacities after reclamation.
    pub after: StationRuntimeCapacities,
}

/// Product-path Station operation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StationRuntimeError {
    /// Authority or entity-state failure.
    Station(StationError),
    /// Component schema, codec, or payload failure.
    Component(ComponentStoreError),
}

impl core::fmt::Display for StationRuntimeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Station(error) => write!(f, "{error}"),
            Self::Component(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for StationRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Station(error) => Some(error),
            Self::Component(error) => Some(error),
        }
    }
}

impl From<StationError> for StationRuntimeError {
    fn from(error: StationError) -> Self {
        Self::Station(error)
    }
}

impl From<ComponentStoreError> for StationRuntimeError {
    fn from(error: ComponentStoreError) -> Self {
        Self::Component(error)
    }
}

/// Coherent owner of `SectorSync`'s normal Station-local middleware state.
#[derive(Clone, Debug)]
pub struct StationRuntime {
    station: Station,
    index: CellIndex,
    components: ComponentStore,
    commands: Option<CommandQueues>,
    index_update_scratch: CellIndexUpdateScratch,
}

impl StationRuntime {
    /// Creates empty Station-local state with explicit topology and capacities.
    pub fn new(config: StationRuntimeConfig) -> Self {
        let mut station = Station::with_capacity(config.station, config.entity_capacity);
        station.reserve_free_handles(config.free_handle_capacity);
        Self {
            station,
            index: CellIndex::with_capacity(
                config.grid,
                config.entity_capacity,
                config.occupied_cell_capacity,
            ),
            components: ComponentStore::default(),
            commands: config.command_queue_limits.map(CommandQueues::new),
            index_update_scratch: CellIndexUpdateScratch::default(),
        }
    }

    /// Returns Station authority and entity state.
    pub const fn station(&self) -> &Station {
        &self.station
    }

    /// Returns the Station-local spatial index.
    pub const fn index(&self) -> &CellIndex {
        &self.index
    }

    /// Returns the Station-local component store.
    pub const fn components(&self) -> &ComponentStore {
        &self.components
    }

    /// Returns configured Station-local command queues, if enabled.
    pub const fn command_queues(&self) -> Option<&CommandQueues> {
        self.commands.as_ref()
    }

    /// Returns mutable configured command queues, if enabled.
    pub const fn command_queues_mut(&mut self) -> Option<&mut CommandQueues> {
        self.commands.as_mut()
    }

    /// Returns current retained-capacity observations.
    pub fn retained_capacities(&self) -> StationRuntimeCapacities {
        StationRuntimeCapacities {
            station_entities: self.station.entity_capacity(),
            station_id_index: self.station.id_index_capacity(),
            station_free_handles: self.station.free_list_capacity(),
            indexed_entities: self.index.entity_capacity(),
            occupied_cells: self.index.occupied_cell_capacity(),
            index_update_cells: self.index_update_scratch.retained_cell_capacity(),
            component_columns: self.components.column_slots_capacity(),
            command_ready: self
                .commands
                .as_ref()
                .map_or(0, CommandQueues::total_ready_retained_capacity),
            command_barrier: self
                .commands
                .as_ref()
                .map_or(0, CommandQueues::barrier_buffer_retained_capacity),
        }
    }

    /// Releases unused retained storage without changing live middleware state.
    pub fn reclaim_retained_capacity(&mut self) -> StationRuntimeReclaimReport {
        let before = self.retained_capacities();
        self.station.reclaim_retained_capacity();
        self.index.reclaim_retained_capacity();
        self.index_update_scratch.reclaim_retained_capacity();
        self.components.reclaim_retained_capacity();
        if let Some(commands) = &mut self.commands {
            commands.reclaim_retained_capacity();
        }
        StationRuntimeReclaimReport {
            before,
            after: self.retained_capacities(),
        }
    }

    /// Advances the Station tick on the caller thread.
    pub fn advance_tick(&mut self) {
        self.station.advance_tick();
    }

    /// Spawns an authoritative entity and inserts its spatial membership.
    pub fn spawn_owned(&mut self, spawn: SpawnEntity) -> Result<EntityHandle, StationError> {
        let handle =
            self.station
                .spawn_owned(spawn.id, spawn.position, spawn.bounds, spawn.policy_id)?;
        let update = self.upsert_index(handle, spawn.position, spawn.bounds);
        debug_assert_eq!(update, CellIndexUpdate::Inserted);
        Ok(handle)
    }

    /// Inserts or refreshes a read-only ghost and its spatial membership.
    pub fn upsert_ghost(&mut self, ghost: GhostEntity) -> StationEntityUpdateReport {
        let handle = self.station.upsert_ghost(
            ghost.id,
            ghost.position,
            ghost.bounds,
            ghost.policy_id,
            ghost.owner_station,
            ghost.owner_epoch,
            ghost.expires_at,
        );
        let index_update = self.upsert_index(handle, ghost.position, ghost.bounds);
        StationEntityUpdateReport {
            handle,
            index_update,
        }
    }

    /// Moves an authoritative entity and updates spatial membership.
    pub fn move_owned(
        &mut self,
        handle: EntityHandle,
        position: Position3,
    ) -> Result<StationMoveReport, StationError> {
        self.station.move_owned(handle, position)?;
        let record = self
            .station
            .get(handle)
            .expect("successful move retains the authoritative entity");
        let (position, bounds) = (record.position, record.bounds);
        let index_update = self.upsert_index(handle, position, bounds);
        Ok(StationMoveReport { index_update })
    }

    /// Removes an entity, its spatial membership, and all component blobs.
    pub fn despawn(&mut self, handle: EntityHandle) -> Result<DespawnReport, StationError> {
        let entity = self.station.remove(handle)?;
        let index_removed = self.index.remove(handle);
        let components_removed = self.components.clear_entity(handle);
        Ok(DespawnReport {
            entity,
            index_removed,
            components_removed,
        })
    }

    /// Replaces authoritative tags through the Station authority guard.
    pub fn set_tags(&mut self, handle: EntityHandle, tags: EntityTags) -> Result<(), StationError> {
        self.station.set_tags(handle, tags)
    }

    /// Copies an opaque component value into retained storage after checking authority.
    pub fn set_component_blob(
        &mut self,
        descriptor: &ComponentDescriptor,
        handle: EntityHandle,
        version: u64,
        bytes: &[u8],
    ) -> Result<(), StationRuntimeError> {
        self.ensure_owned(handle)?;
        self.components
            .set_blob_from_slice(descriptor, handle, version, bytes)?;
        Ok(())
    }

    /// Prepares an outgoing two-phase handoff without changing ownership.
    pub fn prepare_outgoing_handoff(
        &self,
        entity_id: EntityId,
        target_station: StationId,
        target_owner_epoch: OwnerEpoch,
        source_ghost_expires_at: Tick,
    ) -> Result<HandoffTransfer, StationError> {
        self.station.prepare_outgoing_handoff(
            entity_id,
            target_station,
            target_owner_epoch,
            source_ghost_expires_at,
        )
    }

    /// Prewarms a target ghost and synchronizes its spatial membership.
    pub fn prewarm_handoff_ghost(
        &mut self,
        transfer: &HandoffTransfer,
    ) -> Result<StationEntityUpdateReport, StationError> {
        let handle = self.station.prewarm_handoff_ghost(transfer)?;
        let record = self
            .station
            .get(handle)
            .expect("successful handoff prewarm retains the ghost");
        let (position, bounds) = (record.position, record.bounds);
        let index_update = self.upsert_index(handle, position, bounds);
        Ok(StationEntityUpdateReport {
            handle,
            index_update,
        })
    }

    /// Commits an incoming owner handoff and synchronizes spatial membership.
    pub fn commit_incoming_handoff(
        &mut self,
        transfer: HandoffTransfer,
    ) -> Result<StationEntityUpdateReport, StationError> {
        let position = transfer.entity.position;
        let bounds = transfer.entity.bounds;
        let handle = self.station.commit_incoming_handoff(transfer)?;
        let index_update = self.upsert_index(handle, position, bounds);
        Ok(StationEntityUpdateReport {
            handle,
            index_update,
        })
    }

    /// Commits the source side of an owner handoff, retaining spatial ghost visibility.
    pub fn commit_outgoing_handoff(
        &mut self,
        transfer: &HandoffTransfer,
    ) -> Result<EntityHandle, StationError> {
        self.station.commit_outgoing_handoff(transfer)
    }

    /// Returns mutable low-level state for a custom integration.
    ///
    /// The caller must restore Station, spatial-index, component, and queue
    /// invariants before invoking another product-path operation.
    pub fn low_level_parts_mut(
        &mut self,
    ) -> (
        &mut Station,
        &mut CellIndex,
        &mut ComponentStore,
        Option<&mut CommandQueues>,
    ) {
        (
            &mut self.station,
            &mut self.index,
            &mut self.components,
            self.commands.as_mut(),
        )
    }

    fn upsert_index(
        &mut self,
        handle: EntityHandle,
        position: Position3,
        bounds: Bounds,
    ) -> CellIndexUpdate {
        self.index
            .upsert_with_scratch(handle, position, bounds, &mut self.index_update_scratch)
            .update
    }

    fn ensure_owned(&self, handle: EntityHandle) -> Result<(), StationError> {
        let record = self
            .station
            .get(handle)
            .ok_or(StationError::MissingEntityHandle(handle))?;
        if record.is_owned() {
            Ok(())
        } else {
            Err(StationError::NotOwner(record.id))
        }
    }

    /// Consumes the facade and returns its low-level state without conversion.
    pub fn into_parts(self) -> (Station, CellIndex, ComponentStore, Option<CommandQueues>) {
        (self.station, self.index, self.components, self.commands)
    }
}

#[cfg(test)]
mod tests {
    use sectorsync_core::{
        component::{ComponentMigrationMode, ComponentSyncMode},
        ids::{ComponentId, InstanceId, NodeId, StationId, Tick},
    };

    use super::*;

    fn config() -> StationRuntimeConfig {
        StationRuntimeConfig::new(
            StationConfig {
                station_id: StationId::new(1),
                node_id: NodeId::new(2),
                instance_id: InstanceId::new(3),
                tick_rate_hz: 20,
            },
            GridSpec::new(16.0).expect("valid grid"),
        )
        .with_capacity(8, 8)
    }

    fn config_for_station(station_id: u32) -> StationRuntimeConfig {
        StationRuntimeConfig::new(
            StationConfig {
                station_id: StationId::new(station_id),
                node_id: NodeId::new(2),
                instance_id: InstanceId::new(3),
                tick_rate_hz: 20,
            },
            GridSpec::new(16.0).expect("valid grid"),
        )
        .with_capacity(8, 8)
    }

    fn descriptor() -> ComponentDescriptor {
        ComponentDescriptor::sparse_blob(
            ComponentId::new(1),
            "health",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            4,
        )
    }

    #[test]
    fn product_path_keeps_station_and_index_coherent() {
        let mut runtime = StationRuntime::new(config());
        let start = Position3::new(1.0, 2.0, 3.0);
        let handle = runtime
            .spawn_owned(SpawnEntity::new(
                EntityId::new(10),
                start,
                Bounds::Point,
                PolicyId::new(1),
            ))
            .expect("spawn should succeed");

        assert_eq!(
            runtime.station().get(handle).expect("entity").position,
            start
        );
        assert_eq!(runtime.index().query_sphere(start, 1.0), vec![handle]);

        let moved = Position3::new(33.0, 2.0, 3.0);
        let report = runtime
            .move_owned(handle, moved)
            .expect("move should succeed");
        assert_eq!(report.index_update, CellIndexUpdate::Relocated);
        assert!(runtime.index().query_sphere(start, 1.0).is_empty());
        assert_eq!(runtime.index().query_sphere(moved, 1.0), vec![handle]);

        let report = runtime.despawn(handle).expect("despawn should succeed");
        assert!(report.index_removed);
        assert_eq!(report.components_removed, 0);
        assert!(runtime.station().is_empty());
        assert!(runtime.index().query_sphere(moved, 1.0).is_empty());
    }

    #[test]
    fn failed_station_operations_do_not_mutate_the_index() {
        let mut runtime = StationRuntime::new(config());
        let missing = EntityHandle::new(99, 0);
        let error = runtime
            .move_owned(missing, Position3::new(1.0, 0.0, 0.0))
            .expect_err("missing move should fail");
        assert_eq!(error, StationError::MissingEntityHandle(missing));
        assert_eq!(runtime.index().entity_count(), 0);

        let error = runtime
            .despawn(missing)
            .expect_err("missing despawn should fail");
        assert_eq!(error, StationError::MissingEntityHandle(missing));
        assert_eq!(runtime.index().entity_count(), 0);
    }

    #[test]
    fn component_writes_require_authority_and_despawn_clears_storage() {
        let mut runtime = StationRuntime::new(config());
        let position = Position3::new(1.0, 0.0, 1.0);
        let owned = runtime
            .spawn_owned(SpawnEntity::new(
                EntityId::new(1),
                position,
                Bounds::Point,
                PolicyId::new(1),
            ))
            .expect("owned");
        runtime
            .set_component_blob(&descriptor(), owned, 1, &[1, 2, 3, 4])
            .expect("owned component write");

        let ghost = runtime.upsert_ghost(GhostEntity {
            id: EntityId::new(2),
            position,
            bounds: Bounds::Point,
            policy_id: PolicyId::new(1),
            owner_station: StationId::new(9),
            owner_epoch: OwnerEpoch::new(1),
            expires_at: Tick::new(10),
        });
        let error = runtime
            .set_component_blob(&descriptor(), ghost.handle, 1, &[1, 2, 3, 4])
            .expect_err("ghost component write must fail");
        assert_eq!(
            error,
            StationRuntimeError::Station(StationError::NotOwner(EntityId::new(2)))
        );

        let report = runtime.despawn(owned).expect("despawn");
        assert_eq!(report.components_removed, 1);
        assert!(
            runtime
                .components()
                .get_blob(ComponentId::new(1), owned)
                .is_none()
        );
    }

    #[test]
    fn handoff_keeps_source_and_target_spatial_membership() {
        let mut source = StationRuntime::new(config_for_station(1));
        let mut target = StationRuntime::new(config_for_station(2));
        let position = Position3::new(20.0, 0.0, 20.0);
        source
            .spawn_owned(SpawnEntity::new(
                EntityId::new(7),
                position,
                Bounds::Point,
                PolicyId::new(1),
            ))
            .expect("source owner");
        let transfer = source
            .prepare_outgoing_handoff(
                EntityId::new(7),
                StationId::new(2),
                OwnerEpoch::new(1),
                Tick::new(10),
            )
            .expect("prepare");

        target
            .prewarm_handoff_ghost(&transfer)
            .expect("target prewarm");
        let target_owner = target
            .commit_incoming_handoff(transfer.clone())
            .expect("target commit");
        let source_ghost = source
            .commit_outgoing_handoff(&transfer)
            .expect("source commit");

        assert!(
            target
                .station()
                .get(target_owner.handle)
                .expect("target")
                .is_owned()
        );
        assert!(
            !source
                .station()
                .get(source_ghost)
                .expect("source")
                .is_owned()
        );
        assert_eq!(
            target.index().query_sphere(position, 1.0),
            vec![target_owner.handle]
        );
        assert_eq!(
            source.index().query_sphere(position, 1.0),
            vec![source_ghost]
        );
    }

    #[test]
    fn optional_commands_and_reclaim_are_explicit() {
        let config =
            config()
                .with_free_handle_capacity(32)
                .with_command_queues(CommandQueueLimits {
                    high: 4,
                    normal: 8,
                    low: 4,
                });
        let mut runtime = StationRuntime::new(config);
        assert!(runtime.command_queues().is_some());
        let before = runtime.retained_capacities();
        assert!(before.station_entities >= 8);
        assert!(before.station_free_handles >= 32);

        let report = runtime.reclaim_retained_capacity();

        assert_eq!(report.before, before);
        assert!(report.after.station_entities <= report.before.station_entities);
        assert!(report.after.station_free_handles <= report.before.station_free_handles);
        assert!(runtime.command_queues_mut().is_some());
    }
}
