//! Wire frame traits and default frame shapes for SectorSync.

#![forbid(unsafe_code)]

use sectorsync_core::prelude::{BarrierId, BarrierState, ClientId, CommandId, Tick};

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

impl FrameKind {
    /// Converts a byte into a frame kind.
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Replication),
            1 => Some(Self::CommandAck),
            2 => Some(Self::Barrier),
            _ => None,
        }
    }
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

/// Command acknowledgement frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandAckFrame {
    /// Target client.
    pub client_id: ClientId,
    /// Acknowledged command.
    pub command_id: CommandId,
    /// Server tick at acknowledgement.
    pub server_tick: Tick,
    /// Whether the command was accepted by the runtime pipeline.
    pub accepted: bool,
    /// Game/runtime reject reason code.
    pub reason_code: u16,
}

/// Runtime barrier notification frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BarrierFrame {
    /// Target client.
    pub client_id: ClientId,
    /// Barrier id.
    pub barrier_id: BarrierId,
    /// Server tick associated with this barrier state.
    pub server_tick: Tick,
    /// Current barrier state.
    pub state: BarrierState,
}

/// Decoded runtime frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeFrame {
    /// Replication update.
    Replication(ReplicationFrame),
    /// Command acknowledgement.
    CommandAck(CommandAckFrame),
    /// Barrier notification.
    Barrier(BarrierFrame),
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

    /// Encodes a command acknowledgement frame into `out`.
    fn encode_command_ack(
        &mut self,
        frame: &CommandAckFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Encodes a barrier frame into `out`.
    fn encode_barrier(
        &mut self,
        frame: &BarrierFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;
}

/// Decodes frames from bytes.
pub trait FrameDecoder {
    /// Decoder error type.
    type Error;

    /// Decodes one runtime frame.
    fn decode(&mut self, input: &[u8]) -> Result<RuntimeFrame, Self::Error>;
}

/// Binary decode error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinaryDecodeError {
    /// Input is empty.
    Empty,
    /// Unknown frame kind byte.
    UnknownFrameKind(u8),
    /// Frame ended before all fields were available.
    Truncated {
        /// Required bytes.
        needed: usize,
        /// Available bytes.
        available: usize,
    },
    /// Barrier state byte is invalid.
    InvalidBarrierState(u8),
    /// Trailing bytes were present after a complete frame.
    TrailingBytes(usize),
}

impl core::fmt::Display for BinaryDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => f.write_str("empty frame"),
            Self::UnknownFrameKind(kind) => write!(f, "unknown frame kind {kind}"),
            Self::Truncated { needed, available } => {
                write!(f, "truncated frame: needed {needed}, available {available}")
            }
            Self::InvalidBarrierState(state) => write!(f, "invalid barrier state {state}"),
            Self::TrailingBytes(bytes) => write!(f, "frame has {bytes} trailing bytes"),
        }
    }
}

impl std::error::Error for BinaryDecodeError {}

/// Simple little-endian binary frame decoder.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinaryFrameDecoder;

impl FrameDecoder for BinaryFrameDecoder {
    type Error = BinaryDecodeError;

    fn decode(&mut self, input: &[u8]) -> Result<RuntimeFrame, Self::Error> {
        let mut cursor = Cursor::new(input);
        let kind_byte = cursor.read_u8()?;
        let kind = FrameKind::from_byte(kind_byte)
            .ok_or(BinaryDecodeError::UnknownFrameKind(kind_byte))?;
        let frame = match kind {
            FrameKind::Replication => RuntimeFrame::Replication(ReplicationFrame {
                client_id: ClientId::new(cursor.read_u64()?),
                server_tick: Tick::new(cursor.read_u64()?),
                entity_count: cursor.read_u32()?,
                estimated_payload_bytes: cursor.read_u32()?,
            }),
            FrameKind::CommandAck => RuntimeFrame::CommandAck(CommandAckFrame {
                client_id: ClientId::new(cursor.read_u64()?),
                command_id: CommandId::new(cursor.read_u64()?),
                server_tick: Tick::new(cursor.read_u64()?),
                accepted: cursor.read_u8()? != 0,
                reason_code: cursor.read_u16()?,
            }),
            FrameKind::Barrier => RuntimeFrame::Barrier(BarrierFrame {
                client_id: ClientId::new(cursor.read_u64()?),
                barrier_id: BarrierId::new(cursor.read_u64()?),
                server_tick: Tick::new(cursor.read_u64()?),
                state: decode_barrier_state(cursor.read_u8()?)?,
            }),
        };
        cursor.finish()?;
        Ok(frame)
    }
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

    fn encode_command_ack(
        &mut self,
        frame: &CommandAckFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::CommandAck as u8);
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.command_id.get().to_le_bytes());
        out.extend_from_slice(&frame.server_tick.get().to_le_bytes());
        out.push(u8::from(frame.accepted));
        out.extend_from_slice(&frame.reason_code.to_le_bytes());
        Ok(())
    }

    fn encode_barrier(
        &mut self,
        frame: &BarrierFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::Barrier as u8);
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.barrier_id.get().to_le_bytes());
        out.extend_from_slice(&frame.server_tick.get().to_le_bytes());
        out.push(encode_barrier_state(frame.state));
        Ok(())
    }
}

fn encode_barrier_state(state: BarrierState) -> u8 {
    match state {
        BarrierState::Running => 0,
        BarrierState::Requested => 1,
        BarrierState::WaitingTickBoundary => 2,
        BarrierState::Frozen => 3,
        BarrierState::Resuming => 4,
    }
}

fn decode_barrier_state(state: u8) -> Result<BarrierState, BinaryDecodeError> {
    match state {
        0 => Ok(BarrierState::Running),
        1 => Ok(BarrierState::Requested),
        2 => Ok(BarrierState::WaitingTickBoundary),
        3 => Ok(BarrierState::Frozen),
        4 => Ok(BarrierState::Resuming),
        _ => Err(BinaryDecodeError::InvalidBarrierState(state)),
    }
}

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, BinaryDecodeError> {
        self.require(1)?;
        let value = self.input[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, BinaryDecodeError> {
        let bytes = self.read_array::<2>()?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, BinaryDecodeError> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, BinaryDecodeError> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], BinaryDecodeError> {
        self.require(N)?;
        let mut out = [0_u8; N];
        out.copy_from_slice(&self.input[self.offset..self.offset + N]);
        self.offset += N;
        Ok(out)
    }

    fn require(&self, count: usize) -> Result<(), BinaryDecodeError> {
        let needed = self.offset.saturating_add(count);
        if needed > self.input.len() {
            Err(BinaryDecodeError::Truncated {
                needed,
                available: self.input.len(),
            })
        } else {
            Ok(())
        }
    }

    fn finish(&self) -> Result<(), BinaryDecodeError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(BinaryDecodeError::TrailingBytes(
                self.input.len().saturating_sub(self.offset),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_codec_roundtrips_replication_frame() {
        let frame = ReplicationFrame {
            client_id: ClientId::new(9),
            server_tick: Tick::new(42),
            entity_count: 17,
            estimated_payload_bytes: 544,
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_replication(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::Replication(frame));
    }

    #[test]
    fn binary_codec_roundtrips_command_ack_frame() {
        let frame = CommandAckFrame {
            client_id: ClientId::new(1),
            command_id: CommandId::new(2),
            server_tick: Tick::new(3),
            accepted: false,
            reason_code: 7,
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_command_ack(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::CommandAck(frame));
    }

    #[test]
    fn binary_codec_roundtrips_barrier_frame() {
        let frame = BarrierFrame {
            client_id: ClientId::new(1),
            barrier_id: BarrierId::new(99),
            server_tick: Tick::new(11),
            state: BarrierState::Frozen,
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_barrier(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::Barrier(frame));
    }
}
