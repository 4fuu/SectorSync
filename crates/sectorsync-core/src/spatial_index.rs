//! Low-level cell index for station-local AOI candidate queries.

use std::collections::{HashMap, HashSet};

use crate::ids::EntityHandle;
use crate::spatial::{Aabb3, Bounds, CellCoord3, GridSpec, Position3};

/// Station-local 3D cell index.
#[derive(Clone, Debug)]
pub struct CellIndex {
    grid: GridSpec,
    cells: HashMap<CellCoord3, Vec<EntityHandle>>,
    entity_cells: HashMap<EntityHandle, Vec<CellCoord3>>,
}

impl CellIndex {
    /// Creates an empty cell index.
    pub fn new(grid: GridSpec) -> Self {
        Self {
            grid,
            cells: HashMap::new(),
            entity_cells: HashMap::new(),
        }
    }

    /// Returns the grid spec.
    pub const fn grid(&self) -> GridSpec {
        self.grid
    }

    /// Inserts or updates an entity in all cells touched by its bounds.
    pub fn upsert(&mut self, handle: EntityHandle, position: Position3, bounds: Bounds) {
        self.remove(handle);
        let cells = self.grid.cells_for_bounds(position, bounds);
        for cell in &cells {
            self.cells.entry(*cell).or_default().push(handle);
        }
        self.entity_cells.insert(handle, cells);
    }

    /// Removes an entity from the index.
    pub fn remove(&mut self, handle: EntityHandle) -> bool {
        let Some(cells) = self.entity_cells.remove(&handle) else {
            return false;
        };

        for cell in cells {
            let remove_cell = if let Some(handles) = self.cells.get_mut(&cell) {
                handles.retain(|candidate| *candidate != handle);
                handles.is_empty()
            } else {
                false
            };
            if remove_cell {
                self.cells.remove(&cell);
            }
        }

        true
    }

    /// Queries candidate handles overlapping an AABB.
    pub fn query_aabb(&self, aabb: Aabb3) -> Vec<EntityHandle> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for cell in self.grid.cells_for_aabb(aabb) {
            if let Some(handles) = self.cells.get(&cell) {
                for handle in handles {
                    if seen.insert(*handle) {
                        out.push(*handle);
                    }
                }
            }
        }
        out
    }

    /// Queries candidate handles inside cells touched by a sphere.
    pub fn query_sphere(&self, center: Position3, radius: f32) -> Vec<EntityHandle> {
        self.query_aabb(Bounds::Sphere { radius }.to_aabb(center))
    }

    /// Number of indexed entities.
    pub fn entity_count(&self) -> usize {
        self.entity_cells.len()
    }

    /// Number of non-empty cells.
    pub fn occupied_cell_count(&self) -> usize {
        self.cells.len()
    }
}
