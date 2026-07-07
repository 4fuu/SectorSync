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

/// Batch of outbound packets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PacketBatch {
    /// Packets to send together.
    pub packets: Vec<OutboundPacket>,
}

impl PacketBatch {
    /// Creates an empty batch.
    pub const fn new() -> Self {
        Self {
            packets: Vec::new(),
        }
    }

    /// Adds one packet to the batch.
    pub fn push(&mut self, packet: OutboundPacket) {
        self.packets.push(packet);
    }

    /// Returns packet count.
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    /// Returns whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Returns total byte count.
    pub fn bytes_len(&self) -> usize {
        self.packets.iter().map(|packet| packet.bytes.len()).sum()
    }
}

/// Packet sink abstraction. Real network transports should implement this at
/// batch boundaries rather than per-entity boundaries.
pub trait TransportSink {
    /// Transport error type.
    type Error;

    /// Sends one encoded packet.
    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error>;

    /// Sends a packet batch.
    fn send_batch(&mut self, batch: PacketBatch) -> Result<(), Self::Error> {
        for packet in batch.packets {
            self.send(packet)?;
        }
        Ok(())
    }
}

/// Transport sink error produced by wrappers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportError<E> {
    /// Inner sink failed.
    Inner(E),
    /// A packet or batch exceeded byte budget.
    ByteBudgetExceeded {
        /// Configured budget.
        budget: usize,
        /// Actual bytes.
        actual: usize,
    },
}

impl<E: core::fmt::Display> core::fmt::Display for TransportError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Inner(error) => write!(f, "{error}"),
            Self::ByteBudgetExceeded { budget, actual } => {
                write!(
                    f,
                    "transport byte budget exceeded: budget {budget}, actual {actual}"
                )
            }
        }
    }
}

impl<E> std::error::Error for TransportError<E> where E: std::error::Error {}

/// Byte-budget guard for any transport sink.
#[derive(Clone, Debug)]
pub struct BudgetedTransport<T> {
    inner: T,
    max_packet_bytes: usize,
    max_batch_bytes: usize,
}

impl<T> BudgetedTransport<T> {
    /// Creates a budgeted transport wrapper.
    pub const fn new(inner: T, max_packet_bytes: usize, max_batch_bytes: usize) -> Self {
        Self {
            inner,
            max_packet_bytes,
            max_batch_bytes,
        }
    }

    /// Returns the inner transport.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Borrows the inner transport.
    pub const fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: TransportSink> TransportSink for BudgetedTransport<T> {
    type Error = TransportError<T::Error>;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        let bytes = packet.bytes.len();
        if bytes > self.max_packet_bytes {
            return Err(TransportError::ByteBudgetExceeded {
                budget: self.max_packet_bytes,
                actual: bytes,
            });
        }
        self.inner.send(packet).map_err(TransportError::Inner)
    }

    fn send_batch(&mut self, batch: PacketBatch) -> Result<(), Self::Error> {
        let bytes = batch.bytes_len();
        if bytes > self.max_batch_bytes {
            return Err(TransportError::ByteBudgetExceeded {
                budget: self.max_batch_bytes,
                actual: bytes,
            });
        }
        for packet in &batch.packets {
            if packet.bytes.len() > self.max_packet_bytes {
                return Err(TransportError::ByteBudgetExceeded {
                    budget: self.max_packet_bytes,
                    actual: packet.bytes.len(),
                });
            }
        }
        self.inner.send_batch(batch).map_err(TransportError::Inner)
    }
}

/// Fake transport for benchmarks and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FakeTransport {
    packets_sent: usize,
    bytes_sent: usize,
    batches_sent: usize,
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

    /// Returns sent batch count.
    pub const fn batches_sent(&self) -> usize {
        self.batches_sent
    }
}

impl TransportSink for FakeTransport {
    type Error = core::convert::Infallible;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        self.packets_sent += 1;
        self.bytes_sent += packet.bytes.len();
        Ok(())
    }

    fn send_batch(&mut self, batch: PacketBatch) -> Result<(), Self::Error> {
        self.batches_sent += 1;
        self.packets_sent += batch.packets.len();
        self.bytes_sent += batch.bytes_len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(bytes: usize) -> OutboundPacket {
        OutboundPacket {
            client_id: ClientId::new(1),
            bytes: vec![0; bytes],
        }
    }

    #[test]
    fn fake_transport_counts_batches_without_storing_packets() {
        let mut batch = PacketBatch::new();
        batch.push(packet(3));
        batch.push(packet(5));

        let mut transport = FakeTransport::default();
        transport
            .send_batch(batch)
            .expect("fake transport is infallible");

        assert_eq!(transport.batches_sent(), 1);
        assert_eq!(transport.packets_sent(), 2);
        assert_eq!(transport.bytes_sent(), 8);
    }

    #[test]
    fn budgeted_transport_rejects_large_batch() {
        let mut batch = PacketBatch::new();
        batch.push(packet(8));
        batch.push(packet(8));

        let mut transport = BudgetedTransport::new(FakeTransport::default(), 16, 12);
        let error = transport
            .send_batch(batch)
            .expect_err("batch should exceed budget");
        assert_eq!(
            error,
            TransportError::ByteBudgetExceeded {
                budget: 12,
                actual: 16
            }
        );
        assert_eq!(transport.inner().packets_sent(), 0);
    }
}
