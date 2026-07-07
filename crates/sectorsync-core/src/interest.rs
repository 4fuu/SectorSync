//! Interest query primitives used by replication planning.

use crate::entity::EntityRecord;
use crate::ids::ClientId;
use crate::spatial::Position3;

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
