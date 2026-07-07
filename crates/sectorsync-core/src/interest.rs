//! Interest query primitives used by replication planning.

use crate::entity::EntityRecord;
use crate::ids::ClientId;
use crate::spatial::{Frustum3, Position3};

/// Viewer-side interest query.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ViewerQuery {
    /// Client requesting visible or interesting entities.
    pub client_id: ClientId,
    /// Viewer position.
    pub position: Position3,
    /// Primary spherical interest radius.
    pub radius: f32,
    /// Optional maximum number of selected entities.
    pub max_entities: usize,
}

impl ViewerQuery {
    /// Returns squared interest radius.
    pub fn radius_squared(self) -> f32 {
        self.radius * self.radius
    }
}

/// Visibility hook. Embedders can provide frustum or occlusion-aware filters.
pub trait VisibilityFilter {
    /// Returns whether an entity is visible enough to be considered.
    fn is_visible(&self, viewer: &ViewerQuery, entity: &EntityRecord) -> bool;
}

/// Range-only visibility filter.
#[derive(Clone, Copy, Debug, Default)]
pub struct RangeOnlyVisibility;

impl VisibilityFilter for RangeOnlyVisibility {
    fn is_visible(&self, viewer: &ViewerQuery, entity: &EntityRecord) -> bool {
        entity.position.distance_squared(viewer.position) <= viewer.radius_squared()
    }
}

/// Frustum visibility filter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrustumVisibility {
    /// Six-plane visibility volume.
    pub frustum: Frustum3,
}

impl FrustumVisibility {
    /// Creates a frustum visibility filter.
    pub const fn new(frustum: Frustum3) -> Self {
        Self { frustum }
    }
}

impl VisibilityFilter for FrustumVisibility {
    fn is_visible(&self, _viewer: &ViewerQuery, entity: &EntityRecord) -> bool {
        self.frustum
            .intersects_bounds(entity.position, entity.bounds)
    }
}

/// Visibility filter that requires both child filters to pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AndVisibility<A, B> {
    /// First filter.
    pub left: A,
    /// Second filter.
    pub right: B,
}

impl<A, B> AndVisibility<A, B> {
    /// Creates a filter that accepts only entities accepted by both filters.
    pub const fn new(left: A, right: B) -> Self {
        Self { left, right }
    }
}

impl<A, B> VisibilityFilter for AndVisibility<A, B>
where
    A: VisibilityFilter,
    B: VisibilityFilter,
{
    fn is_visible(&self, viewer: &ViewerQuery, entity: &EntityRecord) -> bool {
        self.left.is_visible(viewer, entity) && self.right.is_visible(viewer, entity)
    }
}
