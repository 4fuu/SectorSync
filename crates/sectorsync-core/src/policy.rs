//! Runtime-configurable, hot-path-compiled sync policies.

use crate::ids::PolicyId;

/// Compiled sync policy used by hot-path replication planning.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompiledSyncPolicy {
    /// Small policy id used by entities.
    pub id: PolicyId,
    /// Minimum update rate in hertz.
    pub min_hz: u16,
    /// Maximum update rate in hertz.
    pub max_hz: u16,
    /// Primary interest radius in world units.
    pub interest_radius: f32,
    /// Weight used when client budget is tight.
    pub priority_weight: u16,
    /// Whether this entity can be represented by ghosts.
    pub allow_ghost: bool,
    /// Whether this entity can be aggregated at low detail.
    pub allow_aggregate: bool,
}

impl CompiledSyncPolicy {
    /// Creates a compiled policy.
    pub const fn new(id: PolicyId, min_hz: u16, max_hz: u16, interest_radius: f32) -> Self {
        Self {
            id,
            min_hz,
            max_hz,
            interest_radius,
            priority_weight: 1,
            allow_ghost: true,
            allow_aggregate: false,
        }
    }
}

/// Dense policy table indexed by `PolicyId`.
#[derive(Clone, Debug, Default)]
pub struct PolicyTable {
    policies: Vec<Option<CompiledSyncPolicy>>,
}

impl PolicyTable {
    /// Inserts or replaces a compiled policy.
    pub fn set(&mut self, policy: CompiledSyncPolicy) {
        let index = usize::from(policy.id.get());
        if self.policies.len() <= index {
            self.policies.resize(index + 1, None);
        }
        self.policies[index] = Some(policy);
    }

    /// Gets a policy by id.
    pub fn get(&self, id: PolicyId) -> Option<&CompiledSyncPolicy> {
        self.policies
            .get(usize::from(id.get()))
            .and_then(Option::as_ref)
    }

    /// Number of slots in the dense policy table.
    pub fn slot_count(&self) -> usize {
        self.policies.len()
    }
}
