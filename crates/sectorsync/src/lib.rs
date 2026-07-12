//! Fast-by-default product facade for `SectorSync`.
//!
//! Use [`prelude`] for the normal coherent Station path. Applications that need
//! to assemble individual middleware stages can use the published low-level
//! crates directly or access them through [`low_level`].

pub mod client;
pub mod prelude;
pub mod replication;
pub mod station;

/// Explicit access to the low-level `SectorSync` crates.
pub mod low_level {
    /// Core authority, spatial, component, policy, and replication primitives.
    pub use sectorsync_core as core;
    /// Runtime bridges, load sampling, scheduling, barriers, and migration.
    pub use sectorsync_runtime as runtime;
    /// Packet transports, reliable delivery, and security hooks.
    pub use sectorsync_transport as transport;
    /// Bounded binary wire frames and codecs.
    pub use sectorsync_wire as wire;
}
