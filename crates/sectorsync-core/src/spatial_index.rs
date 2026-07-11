//! Low-level cell index for station-local AOI candidate queries.

use std::collections::{HashMap, HashSet};

use crate::ids::EntityHandle;
use crate::spatial::{Aabb3, Bounds, CellCoord3, GridSpec, Position3};

const MAX_DENSE_DEDUP_SLOTS: usize = 262_144;

/// Occupancy count for one non-empty cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellOccupancy {
    /// Cell coordinate.
    pub cell: CellCoord3,
    /// Number of indexed entity handles in the cell.
    pub entities: usize,
}

/// Result of inserting or updating one entity in a [`CellIndex`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellIndexUpdate {
    /// The handle was not indexed and has been inserted.
    Inserted,
    /// The handle already occupied the same cells; index storage was untouched.
    Unchanged,
    /// The handle moved to a different set of cells.
    Relocated,
}

/// Strategy used by the last scratch-backed cell query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CellQueryStrategy {
    /// Probe every cell touched by the query bounds.
    #[default]
    Grid,
    /// Scan non-empty cells when the query covers a larger sparse volume.
    OccupiedCells,
}

/// Work counters from the last scratch-backed cell query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellQueryStats {
    /// Query strategy selected from query volume and current index occupancy.
    pub strategy: CellQueryStrategy,
    /// Grid cells probed directly by a grid query.
    pub grid_cells_probed: usize,
    /// Non-empty cells inspected by an occupied-cell scan.
    pub occupied_cells_scanned: usize,
    /// Non-empty cells overlapping the query bounds.
    pub matched_cells: usize,
    /// Unique candidate handles produced by the query.
    pub candidate_handles: usize,
}

/// Reusable scratch storage for allocation-aware cell queries.
#[derive(Clone, Debug, Default)]
pub struct CellQueryScratch {
    seen_dense: Vec<u64>,
    seen_collisions: Vec<u32>,
    seen_sparse: HashSet<EntityHandle>,
    query_epoch: u32,
    handles: Vec<EntityHandle>,
    matching_cells: Vec<CellCoord3>,
    stats: CellQueryStats,
}

impl CellQueryScratch {
    /// Clears retained query results while keeping allocated capacity.
    pub fn clear(&mut self) {
        self.begin_query();
        self.handles.clear();
        self.matching_cells.clear();
        self.stats = CellQueryStats::default();
    }

    /// Returns handles produced by the last query.
    pub fn handles(&self) -> &[EntityHandle] {
        &self.handles
    }

    /// Number of handles produced by the last query.
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// Returns whether the last query produced no handles.
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Work counters produced by the last query.
    pub const fn stats(&self) -> CellQueryStats {
        self.stats
    }

    /// Capacity retained for unique candidate handles.
    pub fn handle_capacity(&self) -> usize {
        self.handles.capacity()
    }

    /// Capacity retained by the candidate deduplication set.
    pub fn dedup_capacity(&self) -> usize {
        self.seen_dense
            .capacity()
            .saturating_add(self.seen_collisions.capacity())
            .saturating_add(self.seen_sparse.capacity())
    }

    /// Capacity retained for occupied cells matched by sparse queries.
    pub fn matching_cell_capacity(&self) -> usize {
        self.matching_cells.capacity()
    }

    fn begin_query(&mut self) {
        self.query_epoch = self.query_epoch.wrapping_add(1);
        if self.query_epoch == 0 {
            self.seen_dense.fill(0);
            self.seen_collisions.fill(0);
            self.query_epoch = 1;
        }
        self.seen_sparse.clear();
    }

    fn insert_seen(&mut self, handle: EntityHandle) -> bool {
        let Ok(index) = usize::try_from(handle.index()) else {
            return self.seen_sparse.insert(handle);
        };
        if index >= MAX_DENSE_DEDUP_SLOTS {
            return self.seen_sparse.insert(handle);
        }
        if index >= self.seen_dense.len() {
            self.seen_dense.resize(index + 1, 0);
            self.seen_collisions.resize(index + 1, 0);
        }
        let marker = (u64::from(self.query_epoch) << 32) | u64::from(handle.generation());
        let previous = self.seen_dense[index];
        let previous_epoch = u32::try_from(previous >> 32).expect("marker epoch fits u32");
        if previous_epoch != self.query_epoch {
            self.seen_dense[index] = marker;
            true
        } else if self.seen_collisions[index] == self.query_epoch {
            self.seen_sparse.insert(handle)
        } else if previous == marker {
            false
        } else {
            self.seen_collisions[index] = self.query_epoch;
            let previous_generation =
                u32::try_from(previous & u64::from(u32::MAX)).expect("marker generation fits u32");
            self.seen_sparse
                .insert(EntityHandle::new(handle.index(), previous_generation));
            self.seen_sparse.insert(handle)
        }
    }
}

/// Internal compact membership representation for point and bounded entities.
#[derive(Clone, Debug)]
enum CellMembership {
    Point(CellCoord3),
    Multiple(Vec<CellCoord3>),
}

impl CellMembership {
    fn as_slice(&self) -> &[CellCoord3] {
        match self {
            Self::Point(cell) => std::slice::from_ref(cell),
            Self::Multiple(cells) => cells,
        }
    }

    fn matches_range(&self, min: CellCoord3, max: CellCoord3) -> bool {
        cells_match_range(self.as_slice(), min, max)
    }
}

/// Station-local 3D cell index.
#[derive(Clone, Debug)]
pub struct CellIndex {
    grid: GridSpec,
    cells: HashMap<CellCoord3, Vec<EntityHandle>>,
    entity_cells: HashMap<EntityHandle, CellMembership>,
}

impl CellIndex {
    /// Creates an empty cell index.
    pub fn new(grid: GridSpec) -> Self {
        Self::with_capacity(grid, 0, 0)
    }

    /// Creates an empty index with explicit entity and occupied-cell capacity.
    pub fn with_capacity(
        grid: GridSpec,
        entity_capacity: usize,
        occupied_cell_capacity: usize,
    ) -> Self {
        Self {
            grid,
            cells: HashMap::with_capacity(occupied_cell_capacity),
            entity_cells: HashMap::with_capacity(entity_capacity),
        }
    }

    /// Reserves capacity for additional indexed entities and occupied cells.
    pub fn reserve(&mut self, additional_entities: usize, additional_cells: usize) {
        self.entity_cells.reserve(additional_entities);
        self.cells.reserve(additional_cells);
    }

    /// Indexed-entity entries currently retained without another rehash.
    pub fn entity_capacity(&self) -> usize {
        self.entity_cells.capacity()
    }

    /// Occupied-cell entries currently retained without another rehash.
    pub fn occupied_cell_capacity(&self) -> usize {
        self.cells.capacity()
    }

    /// Returns the grid spec.
    pub const fn grid(&self) -> GridSpec {
        self.grid
    }

    /// Inserts or updates an entity in all cells touched by its bounds.
    pub fn upsert(&mut self, handle: EntityHandle, position: Position3, bounds: Bounds) {
        self.upsert_tracked(handle, position, bounds);
    }

    /// Inserts or updates an entity and reports whether index membership changed.
    pub fn upsert_tracked(
        &mut self,
        handle: EntityHandle,
        position: Position3,
        bounds: Bounds,
    ) -> CellIndexUpdate {
        let cells = if bounds == Bounds::Point {
            let cell = self.grid.cell_at(position);
            if let Some(old_cell) = self
                .entity_cells
                .get(&handle)
                .and_then(|current| match current {
                    CellMembership::Point(cell) => Some(*cell),
                    CellMembership::Multiple(_) => None,
                })
            {
                if old_cell == cell {
                    return CellIndexUpdate::Unchanged;
                }
                self.remove_handle_from_cell(old_cell, handle);
                self.cells.entry(cell).or_default().push(handle);
                *self
                    .entity_cells
                    .get_mut(&handle)
                    .expect("indexed point entity retains its cell membership") =
                    CellMembership::Point(cell);
                return CellIndexUpdate::Relocated;
            }
            CellMembership::Point(cell)
        } else {
            let aabb = bounds.to_aabb(position);
            let min = self.grid.cell_at(aabb.min);
            let max = self.grid.cell_at(aabb.max);
            if self
                .entity_cells
                .get(&handle)
                .is_some_and(|current| current.matches_range(min, max))
            {
                return CellIndexUpdate::Unchanged;
            }
            CellMembership::Multiple(collect_cell_range(min, max))
        };
        let existed = self.remove(handle);
        for cell in cells.as_slice() {
            self.cells.entry(*cell).or_default().push(handle);
        }
        self.entity_cells.insert(handle, cells);
        if existed {
            CellIndexUpdate::Relocated
        } else {
            CellIndexUpdate::Inserted
        }
    }

    /// Removes an entity from the index.
    pub fn remove(&mut self, handle: EntityHandle) -> bool {
        let Some(cells) = self.entity_cells.remove(&handle) else {
            return false;
        };

        for cell in cells.as_slice() {
            self.remove_handle_from_cell(*cell, handle);
        }

        true
    }

    fn remove_handle_from_cell(&mut self, cell: CellCoord3, handle: EntityHandle) {
        let remove_cell = if let Some(handles) = self.cells.get_mut(&cell) {
            if let Some(index) = handles.iter().position(|candidate| *candidate == handle) {
                handles.remove(index);
            }
            handles.is_empty()
        } else {
            false
        };
        if remove_cell {
            self.cells.remove(&cell);
        }
    }

    /// Queries candidate handles overlapping an AABB.
    pub fn query_aabb(&self, aabb: Aabb3) -> Vec<EntityHandle> {
        let mut scratch = CellQueryScratch::default();
        self.query_aabb_into(aabb, &mut scratch);
        scratch.handles
    }

    /// Queries candidate handles overlapping an AABB using caller scratch.
    pub fn query_aabb_into<'a>(
        &self,
        aabb: Aabb3,
        scratch: &'a mut CellQueryScratch,
    ) -> &'a [EntityHandle] {
        scratch.clear();
        let min = self.grid.cell_at(aabb.min);
        let max = self.grid.cell_at(aabb.max);

        let grid_cells = query_cell_volume(min, max);
        if grid_cells <= self.cells.len() {
            scratch.stats.strategy = CellQueryStrategy::Grid;
            scratch.stats.grid_cells_probed = grid_cells;
            for x in min.x..=max.x {
                for y in min.y..=max.y {
                    for z in min.z..=max.z {
                        self.collect_cell(CellCoord3::new(x, y, z), scratch);
                    }
                }
            }
        } else {
            scratch.stats.strategy = CellQueryStrategy::OccupiedCells;
            scratch.stats.occupied_cells_scanned = self.cells.len();
            scratch.matching_cells.extend(
                self.cells
                    .keys()
                    .copied()
                    .filter(|cell| cell_in_range(*cell, min, max)),
            );
            scratch.matching_cells.sort_unstable();
            for index in 0..scratch.matching_cells.len() {
                self.collect_cell(scratch.matching_cells[index], scratch);
            }
        }

        scratch.stats.candidate_handles = scratch.handles.len();

        scratch.handles()
    }

    /// Queries candidate handles inside cells touched by a sphere.
    pub fn query_sphere(&self, center: Position3, radius: f32) -> Vec<EntityHandle> {
        self.query_aabb(Bounds::Sphere { radius }.to_aabb(center))
    }

    /// Queries candidate handles inside cells touched by a sphere using caller scratch.
    pub fn query_sphere_into<'a>(
        &self,
        center: Position3,
        radius: f32,
        scratch: &'a mut CellQueryScratch,
    ) -> &'a [EntityHandle] {
        self.query_aabb_into(Bounds::Sphere { radius }.to_aabb(center), scratch)
    }

    fn collect_cell(&self, cell: CellCoord3, scratch: &mut CellQueryScratch) {
        if let Some(handles) = self.cells.get(&cell) {
            scratch.stats.matched_cells = scratch.stats.matched_cells.saturating_add(1);
            for handle in handles {
                if scratch.insert_seen(*handle) {
                    scratch.handles.push(*handle);
                }
            }
        }
    }

    /// Returns handles indexed directly in one cell.
    pub fn handles_in_cell(&self, cell: CellCoord3) -> Vec<EntityHandle> {
        self.cells.get(&cell).cloned().unwrap_or_default()
    }

    /// Returns handles indexed directly in one cell without allocating.
    pub fn handles_in_cell_slice(&self, cell: CellCoord3) -> &[EntityHandle] {
        self.cells.get(&cell).map_or(&[], Vec::as_slice)
    }

    /// Returns cells currently occupied by one entity handle.
    pub fn cells_for_handle(&self, handle: EntityHandle) -> Option<&[CellCoord3]> {
        self.entity_cells.get(&handle).map(CellMembership::as_slice)
    }

    /// Number of indexed entities.
    pub fn entity_count(&self) -> usize {
        self.entity_cells.len()
    }

    /// Number of entities using allocation-free single-cell membership.
    pub fn point_membership_count(&self) -> usize {
        self.entity_cells
            .values()
            .filter(|membership| matches!(membership, CellMembership::Point(_)))
            .count()
    }

    /// Number of non-empty cells.
    pub fn occupied_cell_count(&self) -> usize {
        self.cells.len()
    }

    /// Returns deterministic occupancy counts for all non-empty cells.
    pub fn cell_occupancy(&self) -> Vec<CellOccupancy> {
        let mut cells = Vec::with_capacity(self.cells.len());
        self.cell_occupancy_into(&mut cells);
        cells
    }

    /// Writes deterministic occupancy counts into caller-owned reusable storage.
    pub fn cell_occupancy_into(&self, out: &mut Vec<CellOccupancy>) {
        out.clear();
        out.extend(self.cells.iter().map(|(cell, handles)| CellOccupancy {
            cell: *cell,
            entities: handles.len(),
        }));
        out.sort_by_key(|occupancy| occupancy.cell);
    }
}

fn cells_match_range(cells: &[CellCoord3], min: CellCoord3, max: CellCoord3) -> bool {
    if cells.len() != query_cell_volume(min, max) {
        return false;
    }
    let mut cells = cells.iter();
    for x in min.x..=max.x {
        for y in min.y..=max.y {
            for z in min.z..=max.z {
                if cells.next() != Some(&CellCoord3::new(x, y, z)) {
                    return false;
                }
            }
        }
    }
    cells.next().is_none()
}

fn collect_cell_range(min: CellCoord3, max: CellCoord3) -> Vec<CellCoord3> {
    let mut cells = Vec::with_capacity(query_cell_volume(min, max));
    for x in min.x..=max.x {
        for y in min.y..=max.y {
            for z in min.z..=max.z {
                cells.push(CellCoord3::new(x, y, z));
            }
        }
    }
    cells
}

fn query_cell_volume(min: CellCoord3, max: CellCoord3) -> usize {
    fn axis_cells(min: i32, max: i32) -> usize {
        if max < min {
            return 0;
        }
        usize::try_from(i64::from(max) - i64::from(min) + 1).unwrap_or(usize::MAX)
    }

    axis_cells(min.x, max.x)
        .saturating_mul(axis_cells(min.y, max.y))
        .saturating_mul(axis_cells(min.z, max.z))
}

const fn cell_in_range(cell: CellCoord3, min: CellCoord3, max: CellCoord3) -> bool {
    cell.x >= min.x
        && cell.x <= max.x
        && cell.y >= min.y
        && cell.y <= max.y
        && cell.z >= min.z
        && cell.z <= max.z
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_index_capacity_is_retained_and_grows_on_request() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::with_capacity(grid, 8, 4);

        assert!(index.entity_capacity() >= 8);
        assert!(index.occupied_cell_capacity() >= 4);
        index.reserve(32, 16);
        assert!(index.entity_capacity() >= 32);
        assert!(index.occupied_cell_capacity() >= 16);

        let handle = EntityHandle::new(1, 0);
        index.upsert(handle, Position3::new(1.0, 2.0, 3.0), Bounds::Point);
        assert_eq!(
            index.query_sphere(Position3::new(1.0, 2.0, 3.0), 1.0),
            vec![handle]
        );
    }

    #[test]
    fn tracked_upsert_skips_same_cell_point_updates() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::with_capacity(grid, 1, 2);
        let handle = EntityHandle::new(1, 0);
        let first_cell = grid.cell_at(Position3::new(1.0, 2.0, 3.0));
        let second_cell = grid.cell_at(Position3::new(11.0, 2.0, 3.0));

        assert_eq!(
            index.upsert_tracked(handle, Position3::new(1.0, 2.0, 3.0), Bounds::Point),
            CellIndexUpdate::Inserted
        );
        let entity_capacity = index.entity_capacity();
        let cell_capacity = index.occupied_cell_capacity();
        assert_eq!(
            index.upsert_tracked(handle, Position3::new(9.0, 2.0, 3.0), Bounds::Point),
            CellIndexUpdate::Unchanged
        );
        assert_eq!(index.handles_in_cell_slice(first_cell), &[handle]);
        assert_eq!(index.entity_capacity(), entity_capacity);
        assert_eq!(index.occupied_cell_capacity(), cell_capacity);

        assert!(matches!(
            index.entity_cells.get(&handle),
            Some(CellMembership::Point(cell)) if *cell == first_cell
        ));
        assert_eq!(
            index.upsert_tracked(handle, Position3::new(11.0, 2.0, 3.0), Bounds::Point),
            CellIndexUpdate::Relocated
        );
        assert!(matches!(
            index.entity_cells.get(&handle),
            Some(CellMembership::Point(cell)) if *cell == second_cell
        ));
        assert_eq!(index.point_membership_count(), 1);
        assert!(index.handles_in_cell_slice(first_cell).is_empty());
        assert_eq!(index.handles_in_cell_slice(second_cell), &[handle]);
    }

    #[test]
    fn tracked_upsert_skips_unchanged_multi_cell_bounds() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::new(grid);
        let handle = EntityHandle::new(1, 0);
        let bounds = Bounds::Sphere { radius: 2.0 };

        assert_eq!(
            index.upsert_tracked(handle, Position3::new(9.0, 0.0, 0.0), bounds),
            CellIndexUpdate::Inserted
        );
        let retained_cells = index
            .entity_cells
            .get(&handle)
            .expect("bounded entity has cells")
            .as_slice()
            .as_ptr();
        assert_eq!(
            index.upsert_tracked(handle, Position3::new(9.5, 0.0, 0.0), bounds),
            CellIndexUpdate::Unchanged
        );
        assert_eq!(
            index
                .entity_cells
                .get(&handle)
                .expect("unchanged bounds retain cells")
                .as_slice()
                .as_ptr(),
            retained_cells
        );

        let relocated_position = Position3::new(12.5, 0.0, 0.0);
        assert_eq!(
            index.upsert_tracked(handle, relocated_position, bounds),
            CellIndexUpdate::Relocated
        );
        assert_eq!(
            index.cells_for_handle(handle),
            Some(grid.cells_for_bounds(relocated_position, bounds).as_slice())
        );
    }

    #[test]
    fn index_exposes_handles_by_cell() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::new(grid);
        let handle = EntityHandle::new(1, 0);
        index.upsert(handle, Position3::new(1.0, 2.0, 3.0), Bounds::Point);
        let cell = grid.cell_at(Position3::new(1.0, 2.0, 3.0));

        assert_eq!(index.handles_in_cell(cell), vec![handle]);
        assert_eq!(index.handles_in_cell_slice(cell), &[handle]);
        assert!(
            index
                .handles_in_cell_slice(CellCoord3::new(99, 99, 99))
                .is_empty()
        );
        assert_eq!(index.cells_for_handle(handle), Some([cell].as_slice()));
    }

    #[test]
    fn occupancy_output_is_sorted_and_reuses_capacity() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::new(grid);
        let left = EntityHandle::new(1, 0);
        let right = EntityHandle::new(2, 0);
        index.upsert(right, Position3::new(21.0, 0.0, 0.0), Bounds::Point);
        index.upsert(left, Position3::new(-11.0, 0.0, 0.0), Bounds::Point);

        let expected = index.cell_occupancy();
        let mut occupancy = Vec::new();
        index.cell_occupancy_into(&mut occupancy);
        assert_eq!(occupancy, expected);
        assert!(
            occupancy
                .windows(2)
                .all(|cells| cells[0].cell < cells[1].cell)
        );

        let retained = occupancy.as_ptr();
        index.cell_occupancy_into(&mut occupancy);
        assert_eq!(occupancy, expected);
        assert_eq!(occupancy.as_ptr(), retained);
    }

    #[test]
    fn scratch_query_deduplicates_and_reuses_storage() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::new(grid);
        let handle = EntityHandle::new(1, 0);
        index.upsert(
            handle,
            Position3::new(9.0, 0.0, 0.0),
            Bounds::Sphere { radius: 2.0 },
        );
        let mut scratch = CellQueryScratch::default();

        let first = index.query_aabb_into(
            Bounds::Sphere { radius: 4.0 }.to_aabb(Position3::new(10.0, 0.0, 0.0)),
            &mut scratch,
        );
        assert_eq!(first, &[handle]);
        assert_eq!(scratch.len(), 1);
        assert_eq!(scratch.stats().strategy, CellQueryStrategy::Grid);
        assert_eq!(scratch.stats().candidate_handles, 1);
        assert!(scratch.handle_capacity() >= 1);
        assert!(scratch.dedup_capacity() >= 1);

        let second = index.query_aabb_into(
            Bounds::Point.to_aabb(Position3::new(100.0, 0.0, 0.0)),
            &mut scratch,
        );
        assert!(second.is_empty());
        assert!(scratch.is_empty());
    }

    #[test]
    fn dense_dedup_preserves_generations_and_bounds_sparse_handles() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::new(grid);
        let old = EntityHandle::new(7, 1);
        let current = EntityHandle::new(7, 2);
        let sparse = EntityHandle::new(u32::MAX, 0);
        let spanning = Bounds::Sphere { radius: 2.0 };
        index.upsert(old, Position3::new(9.0, 0.0, 0.0), spanning);
        index.upsert(current, Position3::new(9.0, 0.0, 0.0), spanning);
        index.upsert(sparse, Position3::new(9.0, 0.0, 0.0), Bounds::Point);
        let mut scratch = CellQueryScratch::default();

        let handles = index.query_aabb_into(
            spanning.to_aabb(Position3::new(10.0, 0.0, 0.0)),
            &mut scratch,
        );

        assert_eq!(handles, &[old, current, sparse]);
        assert!(scratch.dedup_capacity() < MAX_DENSE_DEDUP_SLOTS);
    }

    #[test]
    fn sparse_large_query_scans_occupied_cells_deterministically() {
        let grid = GridSpec::new(10.0).expect("valid grid");
        let mut index = CellIndex::new(grid);
        let high = EntityHandle::new(2, 0);
        let low = EntityHandle::new(1, 0);
        index.upsert(high, Position3::new(95.0, 0.0, 0.0), Bounds::Point);
        index.upsert(low, Position3::new(-95.0, 0.0, 0.0), Bounds::Point);
        let mut scratch = CellQueryScratch::default();

        let handles = index.query_aabb_into(
            Aabb3::new(
                Position3::new(-100.0, -100.0, -100.0),
                Position3::new(100.0, 100.0, 100.0),
            ),
            &mut scratch,
        );

        assert_eq!(handles, &[low, high]);
        assert_eq!(scratch.stats().strategy, CellQueryStrategy::OccupiedCells);
        assert_eq!(scratch.stats().occupied_cells_scanned, 2);
        assert_eq!(scratch.stats().matched_cells, 2);
        assert_eq!(scratch.stats().candidate_handles, 2);
        assert!(scratch.matching_cell_capacity() >= 2);
    }
}
