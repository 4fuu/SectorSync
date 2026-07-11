//! Station event transport SDK example.

use sectorsync_core::prelude::{
    EventId, EventKind, EventPriority, InstanceId, NodeId, Station, StationConfig, StationEvent,
    StationId, Tick,
};
use sectorsync_runtime::{EventRouter, StationEventTransportBridge, StationScheduler, StationSet};
use sectorsync_transport::{InMemoryStationTransport, StationTransportLimits};

fn main() {
    let mut stations = StationSet::default();
    stations.push(station(1));
    stations.push(station(2));

    let mut router = EventRouter::default();
    router.register_stations(&stations);

    let mut transport = InMemoryStationTransport::new(StationTransportLimits {
        max_queued_packets_per_station: 8,
        max_packet_bytes: 512,
    });
    transport.register_station(StationId::new(2));

    let mut bridge = StationEventTransportBridge::default();
    let event = StationEvent {
        id: EventId::new(100),
        source: StationId::new(1),
        target: StationId::new(2),
        source_tick: Tick::new(0),
        target_tick: Tick::new(2),
        priority: EventPriority::Important,
        kind: EventKind::Custom(77),
    };

    bridge
        .send_event(&mut transport, &event)
        .expect("event should encode and enter station transport");
    assert_eq!(transport.queued_len(StationId::new(2)), Some(1));

    let pump = bridge
        .pump_target(&mut transport, &mut router, StationId::new(2), 4)
        .expect("target station should receive and route event");
    assert_eq!(pump.packets_received, 1);
    assert_eq!(pump.events_routed, 1);

    let mut scheduler = StationScheduler::default();
    let mut ready_events = Vec::new();
    scheduler.advance_all(&mut stations);
    scheduler
        .drain_ready_events_into(&stations, &mut router, &mut ready_events)
        .expect("router drain should work");
    assert!(ready_events.is_empty());

    scheduler.advance_all(&mut stations);
    scheduler
        .drain_ready_events_into(&stations, &mut router, &mut ready_events)
        .expect("router drain should work");
    assert_eq!(ready_events, vec![event]);

    println!(
        "station_event_transport sent_events={} packets={} routed={} drained={}",
        bridge.stats().events_sent,
        transport.stats().packets_sent,
        router.stats().routed_events,
        router.stats().drained_events
    );
}

fn station(station_id: u32) -> Station {
    Station::new(StationConfig {
        station_id: StationId::new(station_id),
        node_id: NodeId::new(0),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    })
}
