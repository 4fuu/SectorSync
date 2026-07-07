//! Transport traits and fake transport support for SectorSync.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

use sectorsync_core::prelude::ClientId;

/// Default maximum UDP datagram bytes read by `UdpTransport`.
pub const DEFAULT_UDP_RECV_BUFFER_SIZE: usize = 16 * 1024;

/// Outbound packet after wire encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundPacket {
    /// Target client.
    pub client_id: ClientId,
    /// Encoded bytes.
    pub bytes: Vec<u8>,
}

/// Inbound packet before wire decoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundPacket {
    /// Known client id for `remote_addr`, if the transport has one registered.
    pub client_id: Option<ClientId>,
    /// Address the datagram came from.
    pub remote_addr: SocketAddr,
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

/// Non-blocking packet receive abstraction.
pub trait TransportReceiver {
    /// Transport error type.
    type Error;

    /// Attempts to receive one encoded packet.
    ///
    /// Implementations should return `Ok(None)` when no packet is currently
    /// available instead of blocking the caller's station tick.
    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error>;
}

/// Error produced by the standard UDP transport adapter.
#[derive(Debug)]
pub enum UdpTransportError {
    /// No address has been registered for the target client.
    UnknownClient(ClientId),
    /// Underlying socket error.
    Io(io::Error),
}

impl core::fmt::Display for UdpTransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownClient(client_id) => {
                write!(f, "udp target client {} is not registered", client_id.get())
            }
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for UdpTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnknownClient(_) => None,
            Self::Io(error) => Some(error),
        }
    }
}

impl From<io::Error> for UdpTransportError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Lightweight `std::net::UdpSocket` transport adapter.
///
/// This adapter intentionally operates at packet boundaries. Reliability,
/// encryption, authentication, reconnects, and gateway/session semantics are
/// expected to live outside the core SectorSync hot path.
#[derive(Debug)]
pub struct UdpTransport {
    socket: UdpSocket,
    clients: HashMap<ClientId, SocketAddr>,
    addr_to_client: HashMap<SocketAddr, ClientId>,
    recv_buffer: Vec<u8>,
}

impl UdpTransport {
    /// Binds a non-blocking UDP socket.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        Self::from_socket(socket)
    }

    /// Wraps an existing UDP socket and configures it as non-blocking.
    pub fn from_socket(socket: UdpSocket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            clients: HashMap::new(),
            addr_to_client: HashMap::new(),
            recv_buffer: vec![0; DEFAULT_UDP_RECV_BUFFER_SIZE],
        })
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Borrows the underlying socket.
    pub const fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    /// Mutably borrows the underlying socket.
    pub fn socket_mut(&mut self) -> &mut UdpSocket {
        &mut self.socket
    }

    /// Registers or replaces the address for a client.
    pub fn register_client(
        &mut self,
        client_id: ClientId,
        addr: SocketAddr,
    ) -> Option<SocketAddr> {
        let old_addr = self.clients.insert(client_id, addr);
        if let Some(old_addr) = old_addr {
            self.addr_to_client.remove(&old_addr);
        }
        if let Some(old_client) = self.addr_to_client.insert(addr, client_id) {
            if old_client != client_id {
                self.clients.remove(&old_client);
            }
        }
        old_addr
    }

    /// Removes a registered client address.
    pub fn unregister_client(&mut self, client_id: ClientId) -> Option<SocketAddr> {
        let addr = self.clients.remove(&client_id)?;
        self.addr_to_client.remove(&addr);
        Some(addr)
    }

    /// Returns a registered address for a client.
    pub fn client_addr(&self, client_id: ClientId) -> Option<SocketAddr> {
        self.clients.get(&client_id).copied()
    }

    /// Returns the registered client id for a remote address.
    pub fn client_for_addr(&self, addr: SocketAddr) -> Option<ClientId> {
        self.addr_to_client.get(&addr).copied()
    }

    /// Sets the reusable receive buffer size.
    ///
    /// Datagram payloads larger than this buffer may be truncated by the OS.
    /// Keep replication frames under the transport MTU/budget in normal use.
    pub fn set_recv_buffer_size(&mut self, bytes: usize) {
        self.recv_buffer.resize(bytes.max(1), 0);
    }

    /// Returns the reusable receive buffer size.
    pub fn recv_buffer_size(&self) -> usize {
        self.recv_buffer.len()
    }
}

impl TransportSink for UdpTransport {
    type Error = UdpTransportError;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        let addr = self
            .clients
            .get(&packet.client_id)
            .copied()
            .ok_or(UdpTransportError::UnknownClient(packet.client_id))?;
        let sent = self.socket.send_to(&packet.bytes, addr)?;
        if sent == packet.bytes.len() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "udp socket reported a partial datagram send",
            )
            .into())
        }
    }
}

impl TransportReceiver for UdpTransport {
    type Error = UdpTransportError;

    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error> {
        match self.socket.recv_from(&mut self.recv_buffer) {
            Ok((len, remote_addr)) => Ok(Some(InboundPacket {
                client_id: self.addr_to_client.get(&remote_addr).copied(),
                remote_addr,
                bytes: self.recv_buffer[..len].to_vec(),
            })),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
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
    use std::thread;
    use std::time::Duration;

    fn packet(bytes: usize) -> OutboundPacket {
        OutboundPacket {
            client_id: ClientId::new(1),
            bytes: vec![0; bytes],
        }
    }

    fn recv_with_retry(transport: &mut UdpTransport) -> InboundPacket {
        for _ in 0..50 {
            if let Some(packet) = transport.try_recv().expect("udp receive should work") {
                return packet;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("udp packet was not received");
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

    #[test]
    fn udp_transport_sends_and_receives_registered_client() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let mut server = UdpTransport::bind("127.0.0.1:0").expect("server should bind");
        let mut client = UdpTransport::bind("127.0.0.1:0").expect("client should bind");
        let server_addr = server.local_addr().expect("server addr should exist");
        let client_addr = client.local_addr().expect("client addr should exist");

        server.register_client(client_id, client_addr);
        client.register_client(server_id, server_addr);

        client
            .send(OutboundPacket {
                client_id: server_id,
                bytes: b"command".to_vec(),
            })
            .expect("client should send");
        let inbound = recv_with_retry(&mut server);
        assert_eq!(inbound.client_id, Some(client_id));
        assert_eq!(inbound.remote_addr, client_addr);
        assert_eq!(inbound.bytes, b"command");

        server
            .send(OutboundPacket {
                client_id,
                bytes: b"replication".to_vec(),
            })
            .expect("server should send");
        let inbound = recv_with_retry(&mut client);
        assert_eq!(inbound.client_id, Some(server_id));
        assert_eq!(inbound.remote_addr, server_addr);
        assert_eq!(inbound.bytes, b"replication");
    }

    #[test]
    fn udp_transport_rejects_unknown_client() {
        let mut transport = UdpTransport::bind("127.0.0.1:0").expect("transport should bind");
        let error = transport
            .send(OutboundPacket {
                client_id: ClientId::new(99),
                bytes: Vec::new(),
            })
            .expect_err("unknown client should fail");

        match error {
            UdpTransportError::UnknownClient(client_id) => {
                assert_eq!(client_id, ClientId::new(99));
            }
            UdpTransportError::Io(error) => panic!("unexpected io error: {error}"),
        }
    }
}
