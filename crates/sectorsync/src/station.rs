//! Coherent Station-local product state.

use sectorsync_core::{
    component::ComponentStore,
    entity::EntityRecord,
    ids::{EntityHandle, EntityId, PolicyId},
    spatial::{Bounds, GridSpec, Position3},
    spatial_index::{CellIndex, CellIndexUpdate},
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
}

impl StationRuntimeConfig {
    /// Creates configuration without preallocated entity or cell storage.
    pub const fn new(station: StationConfig, grid: GridSpec) -> Self {
        Self {
            station,
            grid,
            entity_capacity: 0,
            occupied_cell_capacity: 0,
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

/// Coherent owner of `SectorSync`'s normal Station-local middleware state.
#[derive(Clone, Debug)]
pub struct StationRuntime {
    station: Station,
    index: CellIndex,
    components: ComponentStore,
}

impl StationRuntime {
    /// Creates empty Station-local state with explicit topology and capacities.
    pub fn new(config: StationRuntimeConfig) -> Self {
        Self {
            station: Station::with_capacity(config.station, config.entity_capacity),
            index: CellIndex::with_capacity(
                config.grid,
                config.entity_capacity,
                config.occupied_cell_capacity,
            ),
            components: ComponentStore::default(),
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

    /// Advances the Station tick on the caller thread.
    pub fn advance_tick(&mut self) {
        self.station.advance_tick();
    }

    /// Spawns an authoritative entity and inserts its spatial membership.
    pub fn spawn_owned(&mut self, spawn: SpawnEntity) -> Result<EntityHandle, StationError> {
        let handle =
            self.station
                .spawn_owned(spawn.id, spawn.position, spawn.bounds, spawn.policy_id)?;
        let update = self
            .index
            .upsert_tracked(handle, spawn.position, spawn.bounds);
        debug_assert_eq!(update, CellIndexUpdate::Inserted);
        Ok(handle)
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
        let index_update = self
            .index
            .upsert_tracked(handle, record.position, record.bounds);
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

    /// Consumes the facade and returns its low-level state without conversion.
    pub fn into_parts(self) -> (Station, CellIndex, ComponentStore) {
        (self.station, self.index, self.components)
    }
}

#[cfg(test)]
mod tests {
    use sectorsync_core::ids::{InstanceId, NodeId, StationId};

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
}
