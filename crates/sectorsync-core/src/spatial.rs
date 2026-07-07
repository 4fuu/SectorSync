//! 3D spatial primitives and uniform-grid helpers.

/// 3D world-space position.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Position3 {
    /// X coordinate.
    pub x: f32,
    /// Y coordinate.
    pub y: f32,
    /// Z coordinate.
    pub z: f32,
}

impl Position3 {
    /// Creates a new position.
    pub const fn new(x: f32, y: f32, z: f32) -> Self {
        Self { x, y, z }
    }

    /// Squared distance to another position.
    pub fn distance_squared(self, other: Self) -> f32 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;
        dx.mul_add(dx, dy.mul_add(dy, dz * dz))
    }

    /// Returns this position as a vector from the origin.
    pub const fn to_vec3(self) -> Vec3 {
        Vec3::new(self.x, self.y, self.z)
    }
}

/// 3D vector used for extents and offsets.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Vec3 {
    /// X component.
    pub x: f32,
    /// Y component.
    pub y: f32,
    /// Z component.
    pub z: f32,
}

impl Vec3 {
    /// Creates a new vector.
    pub const fn new(x: f32, y: f32, z: f32) -> Self {
        Self { x, y, z }
    }

    /// Dot product with another vector.
    pub fn dot(self, other: Self) -> f32 {
        self.x
            .mul_add(other.x, self.y.mul_add(other.y, self.z * other.z))
    }
}

/// Axis-aligned bounding box.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Aabb3 {
    /// Minimum corner.
    pub min: Position3,
    /// Maximum corner.
    pub max: Position3,
}

impl Aabb3 {
    /// Creates a new AABB.
    pub const fn new(min: Position3, max: Position3) -> Self {
        Self { min, max }
    }

    /// Creates an AABB centered on `center`.
    pub fn from_center_half_extents(center: Position3, half_extents: Vec3) -> Self {
        Self {
            min: Position3::new(
                center.x - half_extents.x,
                center.y - half_extents.y,
                center.z - half_extents.z,
            ),
            max: Position3::new(
                center.x + half_extents.x,
                center.y + half_extents.y,
                center.z + half_extents.z,
            ),
        }
    }

    /// Returns whether two AABBs overlap.
    pub fn overlaps(self, other: Self) -> bool {
        self.min.x <= other.max.x
            && self.max.x >= other.min.x
            && self.min.y <= other.max.y
            && self.max.y >= other.min.y
            && self.min.z <= other.max.z
            && self.max.z >= other.min.z
    }
}

/// Plane represented as `normal dot position + distance >= 0`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Plane3 {
    /// Inward-facing plane normal.
    pub normal: Vec3,
    /// Signed distance term.
    pub distance: f32,
}

impl Plane3 {
    /// Creates a plane from a normal and distance term.
    pub const fn new(normal: Vec3, distance: f32) -> Self {
        Self { normal, distance }
    }

    /// Creates a plane passing through a point.
    pub fn from_normal_and_point(normal: Vec3, point: Position3) -> Self {
        Self {
            normal,
            distance: -normal.dot(point.to_vec3()),
        }
    }

    /// Signed distance from a position to this plane.
    pub fn signed_distance_to_position(self, position: Position3) -> f32 {
        self.normal.dot(position.to_vec3()) + self.distance
    }

    /// Returns whether an AABB intersects this plane's positive half-space.
    pub fn intersects_aabb(self, aabb: Aabb3) -> bool {
        let positive = Position3::new(
            if self.normal.x >= 0.0 {
                aabb.max.x
            } else {
                aabb.min.x
            },
            if self.normal.y >= 0.0 {
                aabb.max.y
            } else {
                aabb.min.y
            },
            if self.normal.z >= 0.0 {
                aabb.max.z
            } else {
                aabb.min.z
            },
        );
        self.signed_distance_to_position(positive) >= 0.0
    }
}

/// Six-plane 3D visibility volume.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Frustum3 {
    /// Inward-facing clipping planes.
    pub planes: [Plane3; 6],
}

impl Frustum3 {
    /// Creates a frustum from six inward-facing planes.
    pub const fn from_planes(planes: [Plane3; 6]) -> Self {
        Self { planes }
    }

    /// Creates an axis-aligned six-plane volume from an AABB.
    pub fn from_aabb(aabb: Aabb3) -> Self {
        Self::from_planes([
            Plane3::new(Vec3::new(1.0, 0.0, 0.0), -aabb.min.x),
            Plane3::new(Vec3::new(-1.0, 0.0, 0.0), aabb.max.x),
            Plane3::new(Vec3::new(0.0, 1.0, 0.0), -aabb.min.y),
            Plane3::new(Vec3::new(0.0, -1.0, 0.0), aabb.max.y),
            Plane3::new(Vec3::new(0.0, 0.0, 1.0), -aabb.min.z),
            Plane3::new(Vec3::new(0.0, 0.0, -1.0), aabb.max.z),
        ])
    }

    /// Returns whether an AABB intersects every clipping half-space.
    pub fn intersects_aabb(self, aabb: Aabb3) -> bool {
        self.planes.iter().all(|plane| plane.intersects_aabb(aabb))
    }

    /// Returns whether bounds at a position intersect this frustum.
    pub fn intersects_bounds(self, position: Position3, bounds: Bounds) -> bool {
        self.intersects_aabb(bounds.to_aabb(position))
    }
}

/// Entity bounds for spatial indexing and AOI overlap.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Bounds {
    /// Point-sized entity.
    Point,
    /// Spherical entity bounds.
    Sphere {
        /// Sphere radius in world units.
        radius: f32,
    },
    /// Axis-aligned entity bounds represented by half extents.
    Aabb {
        /// Half extents in world units.
        half_extents: Vec3,
    },
}

impl Default for Bounds {
    fn default() -> Self {
        Self::Point
    }
}

impl Bounds {
    /// Converts bounds at `position` into an AABB.
    pub fn to_aabb(self, position: Position3) -> Aabb3 {
        match self {
            Self::Point => Aabb3::new(position, position),
            Self::Sphere { radius } => {
                Aabb3::from_center_half_extents(position, Vec3::new(radius, radius, radius))
            }
            Self::Aabb { half_extents } => Aabb3::from_center_half_extents(position, half_extents),
        }
    }
}

/// Integer 3D cell coordinate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellCoord3 {
    /// X cell coordinate.
    pub x: i32,
    /// Y cell coordinate.
    pub y: i32,
    /// Z cell coordinate.
    pub z: i32,
}

impl CellCoord3 {
    /// Creates a new cell coordinate.
    pub const fn new(x: i32, y: i32, z: i32) -> Self {
        Self { x, y, z }
    }
}

/// Uniform 3D grid configuration.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GridSpec {
    cell_size: f32,
}

impl GridSpec {
    /// Creates a grid spec when `cell_size` is finite and positive.
    pub fn new(cell_size: f32) -> Result<Self, GridSpecError> {
        if cell_size.is_finite() && cell_size > 0.0 {
            Ok(Self { cell_size })
        } else {
            Err(GridSpecError::InvalidCellSize)
        }
    }

    /// Returns cell size in world units.
    pub const fn cell_size(self) -> f32 {
        self.cell_size
    }

    /// Maps a world-space position to a cell coordinate.
    pub fn cell_at(self, position: Position3) -> CellCoord3 {
        let inv = 1.0 / self.cell_size;
        CellCoord3::new(
            (position.x * inv).floor() as i32,
            (position.y * inv).floor() as i32,
            (position.z * inv).floor() as i32,
        )
    }

    /// Returns all cells touched by an AABB.
    pub fn cells_for_aabb(self, aabb: Aabb3) -> Vec<CellCoord3> {
        let min = self.cell_at(aabb.min);
        let max = self.cell_at(aabb.max);
        let width = (i64::from(max.x) - i64::from(min.x) + 1)
            * (i64::from(max.y) - i64::from(min.y) + 1)
            * (i64::from(max.z) - i64::from(min.z) + 1);
        let mut cells = Vec::with_capacity(width.max(0) as usize);

        for x in min.x..=max.x {
            for y in min.y..=max.y {
                for z in min.z..=max.z {
                    cells.push(CellCoord3::new(x, y, z));
                }
            }
        }

        cells
    }

    /// Returns all cells touched by entity bounds at `position`.
    pub fn cells_for_bounds(self, position: Position3, bounds: Bounds) -> Vec<CellCoord3> {
        self.cells_for_aabb(bounds.to_aabb(position))
    }
}

/// Grid configuration error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GridSpecError {
    /// Cell size must be positive and finite.
    InvalidCellSize,
}

impl core::fmt::Display for GridSpecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidCellSize => f.write_str("cell size must be finite and positive"),
        }
    }
}

impl std::error::Error for GridSpecError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frustum_from_aabb_accepts_intersecting_bounds_and_rejects_outside() {
        let frustum = Frustum3::from_aabb(Aabb3::new(
            Position3::new(0.0, -10.0, -10.0),
            Position3::new(50.0, 10.0, 10.0),
        ));

        assert!(frustum.intersects_bounds(Position3::new(25.0, 0.0, 0.0), Bounds::Point));
        assert!(frustum.intersects_bounds(
            Position3::new(55.0, 0.0, 0.0),
            Bounds::Sphere { radius: 8.0 }
        ));
        assert!(!frustum.intersects_bounds(Position3::new(-5.0, 0.0, 0.0), Bounds::Point));
        assert!(!frustum.intersects_bounds(Position3::new(25.0, 20.0, 0.0), Bounds::Point));
    }
}
