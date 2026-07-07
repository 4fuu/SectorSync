//! Wire frame traits and default frame shapes for SectorSync.

#![forbid(unsafe_code)]

use sectorsync_core::prelude::{ClientId, Tick};

/// Runtime frame kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    /// Replication update frame.
    Replication,
    /// Command acknowledgement frame.
    CommandAck,
    /// Runtime barrier notification.
    Barrier,
}

/// Replication frame metadata produced per client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationFrame {
    /// Target client.
    pub client_id: ClientId,
    /// Server tick represented by this frame.
    pub server_tick: Tick,
    /// Number of entity updates in this frame.
    pub entity_count: u32,
    /// Estimated payload bytes before transport overhead.
    pub estimated_payload_bytes: u32,
}

/// Encodes frames into bytes.
pub trait FrameEncoder {
    /// Encoder error type.
    type Error;

    /// Encodes a replication frame into `out`.
    fn encode_replication(
        &mut self,
        frame: &ReplicationFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;
}

/// Simple little-endian binary frame encoder.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinaryFrameEncoder;

impl FrameEncoder for BinaryFrameEncoder {
    type Error = core::convert::Infallible;

    fn encode_replication(
        &mut self,
        frame: &ReplicationFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::Replication as u8);
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.server_tick.get().to_le_bytes());
        out.extend_from_slice(&frame.entity_count.to_le_bytes());
        out.extend_from_slice(&frame.estimated_payload_bytes.to_le_bytes());
        Ok(())
    }
}
