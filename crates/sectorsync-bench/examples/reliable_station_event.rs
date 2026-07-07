//! Reliable station event SDK example.

use sectorsync_core::prelude::{
    EventId, EventKind, EventPriority, InstanceId, NodeId, Station, StationConfig, StationEvent,
    StationId, Tick,
};
use sectorsync_runtime::{EventRouter, StationScheduler, StationSet};
use sectorsync_transport::{
    InMemoryStationTransport, ReliableStationConfig, ReliableStationEndpoint,
    StationOutboundPacket, StationTransportLimits, StationTransportReceiver,
};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, FrameDecoder, FrameEncoder, RuntimeFrame,
    StationEventFrame,
};

fn main() {
    let source_station = StationId::new(1);
    let target_station = StationId::new(2);

    let mut stations = StationSet::default();
    stations.push(station(source_station));
    stations.push(station(target_station));

    let mut router = EventRouter::default();
    router.register_stations(&stations);

    let mut transport = InMemoryStationTransport::new(StationTransportLimits {
        max_queued_packets_per_station: 8,
        max_packet_bytes: 512,
    });
    transport.register_station(source_station);
    transport.register_station(target_station);

    let reliable_config = ReliableStationConfig {
        max_in_flight_per_target: 8,
        retry_after_ticks: 2,
        max_attempts: 4,
        max_payload_bytes: 256,
        max_delivered_history: 16,
    };
    let mut source_endpoint = ReliableStationEndpoint::new(reliable_config);
    let mut target_endpoint = ReliableStationEndpoint::new(reliable_config);

    let event = StationEvent {
        id: EventId::new(300),
        source: source_station,
        target: target_station,
        source_tick: Tick::new(0),
        target_tick: Tick::new(2),
        priority: EventPriority::Critical,
        kind: EventKind::Custom(99),
    };
    let mut event_bytes = Vec::new();
    BinaryFrameEncoder
        .encode_station_event(&StationEventFrame::from_event(&event), &mut event_bytes)
        .expect("station event should encode");

    let sequence = source_endpoint
        .send(
            &mut transport,
            StationOutboundPacket {
                source_station,
                target_station,
                bytes: event_bytes,
            },
            0,
        )
        .expect("reliable station event should send");
    let retry = source_endpoint
        .retry_due(&mut transport, 2)
        .expect("due reliable packet should retry");
    assert_eq!(retry.retried, 1);
    assert_eq!(transport.queued_len(target_station), Some(2));

    let first_raw = transport
        .try_recv_station(target_station)
        .expect("target receive should work")
        .expect("first reliable packet should exist");
    let delivered = target_endpoint
        .handle_inbound(&mut transport, first_raw)
        .expect("first reliable packet should handle")
        .expect("first reliable packet should deliver");
    let RuntimeFrame::StationEvent(frame) = BinaryFrameDecoder
        .decode(&delivered.bytes)
        .expect("station event payload should decode")
    else {
        panic!("reliable payload should contain a station event frame");
    };
    assert_eq!(frame.source_station, source_station);
    assert_eq!(frame.target_station, target_station);
    router
        .route(frame.into_event())
        .expect("decoded event should route");

    let duplicate_raw = transport
        .try_recv_station(target_station)
        .expect("target receive should work")
        .expect("duplicate reliable packet should exist");
    assert!(
        target_endpoint
            .handle_inbound(&mut transport, duplicate_raw)
            .expect("duplicate reliable packet should handle")
            .is_none()
    );

    let mut acknowledgements = 0;
    while let Some(ack) = transport
        .try_recv_station(source_station)
        .expect("source ACK receive should work")
    {
        acknowledgements += 1;
        assert!(
            source_endpoint
                .handle_inbound(&mut transport, ack)
                .expect("ACK should handle")
                .is_none()
        );
    }
    assert_eq!(acknowledgements, 2);
    assert_eq!(source_endpoint.sender.in_flight_len(), 0);

    let mut scheduler = StationScheduler::default();
    scheduler.advance_all(&mut stations);
    assert!(
        scheduler
            .drain_ready_events(&stations, &mut router)
            .expect("early drain should work")
            .is_empty()
    );
    scheduler.advance_all(&mut stations);
    let drained = scheduler
        .drain_ready_events(&stations, &mut router)
        .expect("target tick drain should work");
    assert_eq!(drained, vec![event]);

    println!(
        "reliable_station_event sequence={} retries={} delivered={} duplicates={} acks={} routed={} drained={}",
        sequence,
        source_endpoint.sender.stats().retries_sent,
        target_endpoint.receiver.stats().data_delivered,
        target_endpoint.receiver.stats().duplicates_suppressed,
        acknowledgements,
        router.stats().routed_events,
        router.stats().drained_events
    );
}

fn station(station_id: StationId) -> Station {
    Station::new(StationConfig {
        station_id,
        node_id: NodeId::new(0),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    })
}
