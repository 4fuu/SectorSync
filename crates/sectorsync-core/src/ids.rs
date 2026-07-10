//! Strongly typed identifiers used across `SectorSync`.

macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident, $inner:ty) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($inner);

        impl $name {
            /// Creates a new identifier from its raw representation.
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the raw identifier value.
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self::new(value)
            }
        }
    };
}

id_type!(/// Stable entity identifier visible to embedders.
EntityId, u64);
id_type!(/// Connected or simulated client identifier.
ClientId, u64);
id_type!(/// Client command identifier.
CommandId, u64);
id_type!(/// Station identifier selected by the embedding application.
StationId, u32);
id_type!(/// Logical node identifier selected by the embedding application.
NodeId, u32);
id_type!(/// World instance identifier.
InstanceId, u64);
id_type!(/// Small compiled synchronization policy identifier.
PolicyId, u16);
id_type!(/// Registered component identifier.
ComponentId, u16);
id_type!(/// Cross-station event identifier used for idempotency.
EventId, u64);
id_type!(/// Runtime barrier identifier.
BarrierId, u64);
id_type!(/// Fixed simulation tick number.
Tick, u64);
id_type!(/// Entity ownership epoch used during handoff.
OwnerEpoch, u64);

/// Dense, generation-checked handle for station-local entity storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityHandle {
    index: u32,
    generation: u32,
}

impl EntityHandle {
    /// Creates a station-local handle.
    pub const fn new(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }

    /// Dense storage index.
    pub const fn index(self) -> u32 {
        self.index
    }

    /// Generation used to detect stale handles.
    pub const fn generation(self) -> u32 {
        self.generation
    }
}
