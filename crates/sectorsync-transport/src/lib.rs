//! Transport traits and fake transport support for SectorSync.

#![forbid(unsafe_code)]

use sectorsync_core::prelude::ClientId;

/// Outbound packet after wire encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundPacket {
    /// Target client.
    pub client_id: ClientId,
    /// Encoded bytes.
    pub bytes: Vec<u8>,
}

/// Packet sink abstraction. Real network transports should implement this at
/// batch boundaries rather than per-entity boundaries.
pub trait TransportSink {
    /// Transport error type.
    type Error;

    /// Sends one encoded packet.
    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error>;
}

/// Fake transport for benchmarks and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FakeTransport {
    packets_sent: usize,
    bytes_sent: usize,
}

impl FakeTransport {
    /// Returns sent packet count.
    pub const fn packets_sent(&self) -> usize {
        self.packets_sent
    }

    /// Returns sent byte count.
    pub const fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }
}

impl TransportSink for FakeTransport {
    type Error = core::convert::Infallible;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        self.packets_sent += 1;
        self.bytes_sent += packet.bytes.len();
        Ok(())
    }
}
