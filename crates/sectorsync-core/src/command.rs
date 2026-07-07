//! Client command envelopes and validation decisions.

use crate::ids::{ClientId, CommandId, EntityId, Tick};

/// Client command priority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandPriority {
    /// Normal gameplay command.
    Normal,
    /// Latency-sensitive command.
    High,
    /// Low-priority command that may be delayed under pressure.
    Low,
}

/// Command envelope accepted by the generic command pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandEnvelope {
    /// Command id used for replay and audit.
    pub id: CommandId,
    /// Client that submitted the command.
    pub client_id: ClientId,
    /// Entity the command intends to control.
    pub entity_id: EntityId,
    /// Client-side sequence number.
    pub sequence: u64,
    /// Server tick observed when the command entered the runtime.
    pub received_at: Tick,
    /// Game-defined command kind.
    pub kind: u32,
    /// Command priority.
    pub priority: CommandPriority,
    /// Opaque payload owned by the embedding game.
    pub payload: Vec<u8>,
}

/// Result of command validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandDecision {
    /// Command can be applied.
    Accept,
    /// Command is invalid and should not be applied.
    Reject {
        /// Machine-readable reject reason.
        reason: CommandRejectReason,
    },
    /// Command should be treated as suspicious for audit purposes.
    FlagSuspicious {
        /// Suspicion score chosen by the embedding application.
        score: u32,
        /// Machine-readable reject or audit reason.
        reason: CommandRejectReason,
    },
}

/// Generic command reject reasons.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandRejectReason {
    /// Command failed schema or size validation.
    InvalidSchema,
    /// Command was submitted too frequently.
    RateLimited,
    /// Command was stale or replayed.
    ReplayOrStale,
    /// Command targeted an entity not owned by this station.
    NotOwner,
    /// Game-specific validator rejected the command.
    GameRule,
}
