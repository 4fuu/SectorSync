//! Guarded deterministic multi-room gameplay-shaped benchmark.

use std::collections::VecDeque;
use std::env;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CommandEnvelope, CommandId, CommandIngress, CommandPriority,
    CommandQueueLimits, CommandQueues, ComponentDescriptor, ComponentId, ComponentMigrationMode,
    ComponentStore, ComponentSyncMode, EntityHandle, EntityId, EventId, EventKind, EventPriority,
    EventQueueLimits, GatewayConfig, GatewaySessionTable, GridSpec, InstanceId, NodeId, PolicyId,
    PolicyTable, Position3, RangeOnlyVisibility, ReplicationBatchScratch, ReplicationBudget,
    ReplicationPlanner, ReplicationScratch, ReplicationTracker, ReplicationTrackerConfig, Station,
    StationConfig, StationEvent, StationId, Tick, ViewerQuery,
};
use sectorsync_runtime::EventRouter;
use sectorsync_transport::{
    ClientTransportLimits, InMemoryTransportEndpoint, InMemoryTransportHub, OutboundPacket,
    TransportReceiver, TransportSink,
};
use sectorsync_wire::{
    BinaryFrameDecoder, ComponentSelection, ReplicationFrameBuilder, ReplicationFrameLimits,
};

const SMOKE_ROOMS: usize = 20;
const SMOKE_ENTITIES_PER_ROOM: usize = 128;
const SMOKE_TICKS: usize = 30;
const STANDARD_ROOMS: usize = 100;
const STANDARD_ENTITIES_PER_ROOM: usize = 256;
const STANDARD_TICKS: usize = 180;
const LARGE_ROOMS: usize = 500;
const LARGE_ENTITIES_PER_ROOM: usize = 512;
const LARGE_TICKS: usize = 300;
const MIN_PLAYERS: usize = 4;
const MAX_PLAYERS: usize = 10;
const MAX_PACKET_BYTES: usize = 16 * 1024;
const TIME_BUDGET: Duration = Duration::from_secs(10);
const TRANSFORM_COMPONENT: ComponentId = ComponentId::new(1);
const STATE_COMPONENT: ComponentId = ComponentId::new(2);
const INVENTORY_COMPONENT: ComponentId = ComponentId::new(3);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Profile {
    Smoke,
    Standard,
    Large,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    requested_profile: Profile,
    profile: Profile,
    rooms: usize,
    entities_per_room: usize,
    ticks: usize,
    allow_heavy: bool,
    heavy_profile_denied: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            requested_profile: Profile::Smoke,
            profile: Profile::Smoke,
            rooms: SMOKE_ROOMS,
            entities_per_room: SMOKE_ENTITIES_PER_ROOM,
            ticks: SMOKE_TICKS,
            allow_heavy: false,
            heavy_profile_denied: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let allow_heavy = args.iter().any(|arg| arg == "--allow-heavy");
        let mut config = Self {
            allow_heavy,
            ..Self::default()
        };
        for arg in &args {
            if let Some(value) = arg.strip_prefix("--profile=") {
                config.requested_profile = match value {
                    "standard" => Profile::Standard,
                    "large" => Profile::Large,
                    _ => Profile::Smoke,
                };
            }
        }
        config.apply_profile();
        for arg in &args {
            if let Some(value) = arg.strip_prefix("--rooms=") {
                config.rooms = parse_usize(value, config.rooms).max(1);
            } else if let Some(value) = arg.strip_prefix("--entities-per-room=") {
                config.entities_per_room = parse_usize(value, config.entities_per_room).max(1);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = parse_usize(value, config.ticks).max(1);
            }
        }
        config.apply_guard();
        config
    }

    fn apply_profile(&mut self) {
        match self.requested_profile {
            Profile::Smoke => {}
            Profile::Standard if self.allow_heavy => {
                self.profile = Profile::Standard;
                self.rooms = STANDARD_ROOMS;
                self.entities_per_room = STANDARD_ENTITIES_PER_ROOM;
                self.ticks = STANDARD_TICKS;
            }
            Profile::Large if self.allow_heavy => {
                self.profile = Profile::Large;
                self.rooms = LARGE_ROOMS;
                self.entities_per_room = LARGE_ENTITIES_PER_ROOM;
                self.ticks = LARGE_TICKS;
            }
            Profile::Standard | Profile::Large => {
                self.heavy_profile_denied = true;
            }
        }
    }

    fn apply_guard(&mut self) {
        let before = (self.rooms, self.entities_per_room, self.ticks);
        let limits = if self.allow_heavy {
            (LARGE_ROOMS, LARGE_ENTITIES_PER_ROOM, LARGE_TICKS)
        } else {
            (SMOKE_ROOMS, SMOKE_ENTITIES_PER_ROOM, SMOKE_TICKS)
        };
        self.rooms = self.rooms.min(limits.0);
        self.entities_per_room = self.entities_per_room.clamp(MAX_PLAYERS, limits.1);
        self.ticks = self.ticks.min(limits.2);
        self.guard_applied = before != (self.rooms, self.entities_per_room, self.ticks);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoomClass {
    Idle,
    Active,
    Hot,
}

impl RoomClass {
    const fn for_room(room: usize) -> Self {
        match room % 10 {
            0 => Self::Hot,
            1..=4 => Self::Active,
            _ => Self::Idle,
        }
    }

    const fn command_interval(self) -> usize {
        match self {
            Self::Idle => 10,
            Self::Active => 2,
            Self::Hot => 1,
        }
    }

    const fn npc_updates(self) -> usize {
        match self {
            Self::Idle => 0,
            Self::Active => 4,
            Self::Hot => 16,
        }
    }

    const fn projectiles_per_tick(self) -> usize {
        match self {
            Self::Idle => 0,
            Self::Active => 1,
            Self::Hot => 2,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TickTrace {
    recreate_room: Option<usize>,
}

#[derive(Clone, Debug)]
struct Descriptors {
    transform: ComponentDescriptor,
    state: ComponentDescriptor,
    inventory: ComponentDescriptor,
}

impl Descriptors {
    fn new() -> Self {
        Self {
            transform: ComponentDescriptor::sparse_blob(
                TRANSFORM_COMPONENT,
                "benchmark.transform",
                ComponentSyncMode::Delta,
                ComponentMigrationMode::Copy,
                24,
            ),
            state: ComponentDescriptor::sparse_blob(
                STATE_COMPONENT,
                "benchmark.state",
                ComponentSyncMode::Delta,
                ComponentMigrationMode::Copy,
                8,
            ),
            inventory: ComponentDescriptor::sparse_blob(
                INVENTORY_COMPONENT,
                "benchmark.inventory",
                ComponentSyncMode::Delta,
                ComponentMigrationMode::Copy,
                64,
            ),
        }
    }
}

#[derive(Debug)]
struct Player {
    client_id: ClientId,
    entity_id: EntityId,
    handle: EntityHandle,
    transport: InMemoryTransportEndpoint,
}

#[derive(Clone, Copy, Debug)]
struct Projectile {
    handle: EntityHandle,
    expires_at: Tick,
}

#[derive(Debug)]
struct Room {
    id: usize,
    class: RoomClass,
    station: Station,
    index: CellIndex,
    components: ComponentStore,
    entities: Vec<EntityHandle>,
    players: Vec<Player>,
    projectiles: VecDeque<Projectile>,
    next_entity_local: usize,
    gateway: GatewaySessionTable,
    commands: CommandQueues,
    tracker: ReplicationTracker,
    planning: ReplicationScratch,
    plans: ReplicationBatchScratch,
    server_transport: InMemoryTransportEndpoint,
    dirty_handles: Vec<EntityHandle>,
    ready_events: Vec<StationEvent>,
}

#[derive(Clone, Debug, Default)]
struct Stats {
    ticks_completed: usize,
    rooms_created: usize,
    rooms_destroyed: usize,
    commands_admitted: usize,
    commands_applied: usize,
    events_routed: usize,
    events_drained: usize,
    entities_spawned: usize,
    entities_despawned: usize,
    movement_updates: usize,
    component_updates: usize,
    viewer_plans: usize,
    selected_entities: usize,
    unexamined_after_budget: usize,
    encoded_entities: usize,
    encoded_components: usize,
    packets_sent: usize,
    packets_received: usize,
    encoded_bytes: usize,
    decoded_bytes: usize,
    tracker_records: usize,
    tracker_acks: usize,
    packet_oversize: usize,
    world_checksum: u64,
    client_checksum: u64,
    retained_plan_slots: usize,
    retained_plan_entities: usize,
    retained_query_candidates: usize,
    retained_command_slots: usize,
    retained_component_slots: usize,
    time_budget_exhausted: bool,
    tick_ms: Vec<f64>,
    command_ms: Vec<f64>,
    simulation_ms: Vec<f64>,
    replication_ms: Vec<f64>,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let descriptors = Descriptors::new();
    let policies = policies();
    let trace = build_trace(config);
    let started = Instant::now();
    let stats = run(config, &descriptors, &policies, &trace);
    let elapsed = started.elapsed();
    print_report(config, &stats, elapsed);
    if !benchmark_ok(config, &stats) {
        std::process::exit(1);
    }
}

fn run(
    config: Config,
    descriptors: &Descriptors,
    policies: &PolicyTable,
    trace: &[TickTrace],
) -> Stats {
    let mut stats = Stats::default();
    let mut rooms = (0..config.rooms)
        .map(|room| create_room(room, config.entities_per_room, descriptors))
        .collect::<Vec<_>>();
    stats.rooms_created = rooms.len();
    let mut router = EventRouter::new(EventQueueLimits {
        critical: 16,
        important: 64,
        best_effort: 128,
    });
    for room in &rooms {
        router.register_station(room.station.config().station_id);
    }
    let selection = ComponentSelection {
        component_ids: vec![TRANSFORM_COMPONENT, STATE_COMPONENT, INVENTORY_COMPONENT],
    };
    let builder = ReplicationFrameBuilder::new(ReplicationFrameLimits {
        max_entity_deltas: 256,
        max_components_per_entity: 3,
        max_component_bytes: 64,
    });
    let budget = ReplicationBudget {
        max_entities: 256,
        max_bytes: 15 * 1024,
        estimated_entity_bytes: 96,
    };
    let run_started = Instant::now();

    for (tick_index, tick_trace) in trace.iter().enumerate() {
        if run_started.elapsed() >= TIME_BUDGET {
            stats.time_budget_exhausted = true;
            break;
        }
        let tick_started = Instant::now();
        if let Some(room_id) = tick_trace.recreate_room {
            rooms[room_id] = create_room(room_id, config.entities_per_room, descriptors);
            stats.rooms_destroyed = stats.rooms_destroyed.saturating_add(1);
            stats.rooms_created = stats.rooms_created.saturating_add(1);
        }
        for room in &mut rooms {
            room.station.advance_tick();
        }

        let command_started = Instant::now();
        for room in &mut rooms {
            enqueue_and_apply_commands(room, tick_index, descriptors, &mut stats);
        }
        stats
            .command_ms
            .push(command_started.elapsed().as_secs_f64() * 1_000.0);

        let simulation_started = Instant::now();
        for room in &mut rooms {
            update_npcs(room, tick_index, descriptors, &mut stats);
            update_projectiles(room, descriptors, &mut stats);
            route_gameplay_event(room, tick_index, &mut router, &mut stats);
        }
        for room in &mut rooms {
            drain_gameplay_events(room, descriptors, &mut router, &mut stats);
        }
        stats
            .simulation_ms
            .push(simulation_started.elapsed().as_secs_f64() * 1_000.0);

        let replication_started = Instant::now();
        for room in &mut rooms {
            replicate_room(room, policies, &selection, builder, budget, &mut stats);
        }
        stats
            .replication_ms
            .push(replication_started.elapsed().as_secs_f64() * 1_000.0);
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }

    for room in &rooms {
        stats.world_checksum = stats.world_checksum.wrapping_add(room_checksum(room));
        stats.tracker_records = stats.tracker_records.saturating_add(room.tracker.len());
        stats.tracker_acks = stats
            .tracker_acks
            .saturating_add(room.tracker.stats().acked_records);
        stats.retained_plan_slots = stats
            .retained_plan_slots
            .saturating_add(room.plans.retained_plan_slots());
        stats.retained_plan_entities = stats
            .retained_plan_entities
            .saturating_add(room.plans.retained_entity_capacity());
        stats.retained_query_candidates = stats
            .retained_query_candidates
            .saturating_add(room.planning.candidate_capacity());
        stats.retained_command_slots = stats
            .retained_command_slots
            .saturating_add(room.commands.total_ready_retained_capacity());
        stats.retained_component_slots = stats
            .retained_component_slots
            .saturating_add(room.components.column_slots_capacity());
    }
    stats
}

fn create_room(room_id: usize, entities_per_room: usize, descriptors: &Descriptors) -> Room {
    let station_id = StationId::new(u32::try_from(room_id + 1).expect("room count fits u32"));
    let players = MIN_PLAYERS + room_id % (MAX_PLAYERS - MIN_PLAYERS + 1);
    let capacity = entities_per_room.saturating_add(16);
    let mut station = Station::with_capacity(
        StationConfig {
            station_id,
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(u64::try_from(room_id + 1).expect("room id fits u64")),
            tick_rate_hz: 30,
        },
        capacity,
    );
    let grid = GridSpec::new(32.0).expect("valid benchmark grid");
    let mut index = CellIndex::with_capacity(grid, capacity, 64);
    let mut components = ComponentStore::default();
    components.reserve_component(TRANSFORM_COMPONENT, capacity);
    components.reserve_component(STATE_COMPONENT, capacity);
    components.reserve_component(INVENTORY_COMPONENT, players);
    let mut entities = Vec::with_capacity(capacity);
    let mut player_records = Vec::with_capacity(players);

    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 64,
        max_packet_bytes: MAX_PACKET_BYTES,
    });
    let server_client_id = server_client_id(room_id);
    let server_transport = hub
        .endpoint(server_client_id, benchmark_addr(room_id, MAX_PLAYERS))
        .expect("server endpoint should register");
    let mut gateway = GatewaySessionTable::new(GatewayConfig {
        max_sessions: players,
        reconnect_grace_ticks: 60,
        max_commands_per_tick: 4,
    });

    for local in 0..entities_per_room {
        let entity_id = room_entity_id(room_id, local);
        let position = entity_position(room_id, local);
        let handle = station
            .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(1))
            .expect("benchmark entity should spawn");
        index.upsert(handle, position, Bounds::Point);
        components
            .set_blob(&descriptors.transform, handle, 0, transform_bytes(position))
            .expect("transform should fit");
        components
            .set_blob(&descriptors.state, handle, 0, state_bytes(local as u64))
            .expect("state should fit");
        entities.push(handle);
        if local < players {
            components
                .set_blob(&descriptors.inventory, handle, 0, vec![0; 64])
                .expect("inventory should fit");
            let client_id = room_client_id(room_id, local);
            gateway
                .connect(client_id, station_id, Tick::new(0))
                .expect("benchmark client should connect");
            let transport = hub
                .endpoint(client_id, benchmark_addr(room_id, local))
                .expect("client endpoint should register");
            player_records.push(Player {
                client_id,
                entity_id,
                handle,
                transport,
            });
        }
    }

    let dirty_handles = entities.clone();
    Room {
        id: room_id,
        class: RoomClass::for_room(room_id),
        station,
        index,
        components,
        entities,
        players: player_records,
        projectiles: VecDeque::new(),
        next_entity_local: entities_per_room,
        gateway,
        commands: CommandQueues::new(CommandQueueLimits {
            high: 32,
            normal: 128,
            low: 32,
        }),
        tracker: ReplicationTracker::new(ReplicationTrackerConfig {
            max_entries: players.saturating_mul(capacity.saturating_add(1_024)),
        }),
        planning: ReplicationScratch::default(),
        plans: ReplicationBatchScratch::new(),
        server_transport,
        dirty_handles,
        ready_events: Vec::new(),
    }
}

fn enqueue_and_apply_commands(
    room: &mut Room,
    tick_index: usize,
    descriptors: &Descriptors,
    stats: &mut Stats,
) {
    if !tick_index.is_multiple_of(room.class.command_interval()) {
        return;
    }
    let now = room.station.tick();
    for (player_index, player) in room.players.iter().enumerate() {
        let sequence = u64::try_from(tick_index.saturating_mul(MAX_PLAYERS) + player_index + 1)
            .expect("guarded sequence fits u64");
        room.gateway
            .admit_sequence(player.client_id, sequence, now)
            .expect("deterministic command should admit");
        room.commands
            .push(
                CommandEnvelope {
                    id: CommandId::new(command_id(room.id, sequence)),
                    client_id: player.client_id,
                    entity_id: player.entity_id,
                    sequence,
                    received_at: now,
                    kind: 1,
                    priority: if player_index == 0 {
                        CommandPriority::High
                    } else {
                        CommandPriority::Normal
                    },
                    payload: sequence.to_le_bytes().to_vec(),
                },
                CommandIngress::RUNNING,
            )
            .expect("guarded command queue should admit");
        stats.commands_admitted = stats.commands_admitted.saturating_add(1);
    }

    while let Some(command) = room.commands.pop_next() {
        let handle = room
            .station
            .handle_by_id(command.entity_id)
            .expect("player entity should exist");
        let current = room
            .station
            .get(handle)
            .expect("player handle should resolve")
            .position;
        let position = moved_position(current, command.sequence);
        move_entity(room, handle, position, descriptors, stats);
        stats.commands_applied = stats.commands_applied.saturating_add(1);
    }
}

fn update_npcs(room: &mut Room, tick_index: usize, descriptors: &Descriptors, stats: &mut Stats) {
    let npc_start = room.players.len();
    let npc_count = room.entities.len().saturating_sub(npc_start);
    let updates = room.class.npc_updates().min(npc_count);
    for offset in 0..updates {
        let index = npc_start + (tick_index.saturating_mul(17) + offset) % npc_count.max(1);
        let handle = room.entities[index];
        let Some(current) = room.station.get(handle).map(|entity| entity.position) else {
            continue;
        };
        move_entity(
            room,
            handle,
            moved_position(
                current,
                u64::try_from(tick_index + offset).unwrap_or(u64::MAX),
            ),
            descriptors,
            stats,
        );
    }
}

fn update_projectiles(room: &mut Room, descriptors: &Descriptors, stats: &mut Stats) {
    let now = room.station.tick();
    while room
        .projectiles
        .front()
        .is_some_and(|projectile| projectile.expires_at.get() <= now.get())
    {
        let projectile = room.projectiles.pop_front().expect("front was checked");
        if room.station.remove(projectile.handle).is_ok() {
            room.index.remove(projectile.handle);
            room.components.clear_entity(projectile.handle);
            stats.entities_despawned = stats.entities_despawned.saturating_add(1);
        }
    }

    for offset in 0..room.class.projectiles_per_tick() {
        let local = room.next_entity_local;
        room.next_entity_local = room.next_entity_local.saturating_add(1);
        let entity_id = room_entity_id(room.id, local);
        let position = entity_position(room.id, local.saturating_add(offset));
        let handle = room
            .station
            .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(1))
            .expect("projectile should spawn");
        room.index.upsert(handle, position, Bounds::Point);
        room.components
            .set_blob(
                &descriptors.transform,
                handle,
                now.get(),
                transform_bytes(position),
            )
            .expect("projectile transform should fit");
        room.components
            .set_blob(
                &descriptors.state,
                handle,
                now.get(),
                state_bytes(local as u64),
            )
            .expect("projectile state should fit");
        room.projectiles.push_back(Projectile {
            handle,
            expires_at: Tick::new(now.get().saturating_add(5)),
        });
        room.dirty_handles.push(handle);
        stats.entities_spawned = stats.entities_spawned.saturating_add(1);
    }
}

fn route_gameplay_event(
    room: &Room,
    tick_index: usize,
    router: &mut EventRouter,
    stats: &mut Stats,
) {
    if room.class == RoomClass::Idle || !tick_index.is_multiple_of(5) {
        return;
    }
    let station_id = room.station.config().station_id;
    router
        .route(StationEvent {
            id: EventId::new(command_id(
                room.id,
                u64::try_from(tick_index + 1).unwrap_or(u64::MAX),
            )),
            source: station_id,
            target: station_id,
            source_tick: room.station.tick(),
            target_tick: room.station.tick(),
            priority: EventPriority::Important,
            kind: EventKind::Custom(1),
        })
        .expect("guarded event should route");
    stats.events_routed = stats.events_routed.saturating_add(1);
}

fn drain_gameplay_events(
    room: &mut Room,
    descriptors: &Descriptors,
    router: &mut EventRouter,
    stats: &mut Stats,
) {
    router
        .drain_ready_into(
            room.station.config().station_id,
            room.station.tick(),
            &mut room.ready_events,
        )
        .expect("room event queue should exist");
    let drained = room.ready_events.len();
    if drained != 0 {
        let handle = room.players[0].handle;
        room.components
            .set_blob_from_slice(
                &descriptors.state,
                handle,
                room.station.tick().get(),
                &state_bytes(room.station.tick().get()),
            )
            .expect("event state should fit");
        room.dirty_handles.push(handle);
        stats.component_updates = stats.component_updates.saturating_add(1);
    }
    stats.events_drained = stats.events_drained.saturating_add(drained);
}

fn room_viewers(room: &Room) -> Vec<ViewerQuery> {
    room.players
        .iter()
        .map(|player| ViewerQuery {
            client_id: player.client_id,
            position: room
                .station
                .get(player.handle)
                .expect("player should resolve")
                .position,
            radius: 256.0,
            max_entities: 256,
        })
        .collect()
}

fn replicate_room(
    room: &mut Room,
    policies: &PolicyTable,
    selection: &ComponentSelection,
    builder: ReplicationFrameBuilder,
    budget: ReplicationBudget,
    stats: &mut Stats,
) {
    let viewers = room_viewers(room);
    let components = &room.components;
    let plans = ReplicationPlanner::plan_for_viewers_work_bounded_into(
        &room.station,
        &room.index,
        policies,
        &viewers,
        &RangeOnlyVisibility,
        budget,
        |_, handle, _| components.has_dirty_selected(handle, &selection.component_ids),
        &mut room.planning,
        &mut room.plans,
    );
    stats.viewer_plans = stats.viewer_plans.saturating_add(plans.stats.viewers);
    stats.selected_entities = stats.selected_entities.saturating_add(plans.stats.selected);
    stats.unexamined_after_budget = stats
        .unexamined_after_budget
        .saturating_add(plans.stats.unexamined_after_budget);

    for (viewer, plan) in viewers.iter().zip(plans.plans) {
        let capacity =
            builder.sampled_binary_capacity_hint(&room.station, plan, &room.components, selection);
        let mut bytes = Vec::with_capacity(capacity);
        let build = builder
            .encode_binary_into(
                viewer.client_id,
                room.station.tick(),
                &room.station,
                plan,
                &room.components,
                selection,
                &mut bytes,
            )
            .expect("guarded replication frame should encode");
        stats.encoded_entities = stats
            .encoded_entities
            .saturating_add(build.encoded_entities);
        stats.encoded_components = stats
            .encoded_components
            .saturating_add(build.encoded_components);
        if bytes.len() > MAX_PACKET_BYTES {
            stats.packet_oversize = stats.packet_oversize.saturating_add(1);
            continue;
        }
        let byte_len = bytes.len();
        room.server_transport
            .send(OutboundPacket {
                client_id: viewer.client_id,
                bytes,
            })
            .expect("guarded in-memory packet should send");
        room.tracker
            .record_plan_sent(viewer.client_id, plan, room.station.tick())
            .expect("guarded tracker should retain plan");
        room.tracker
            .acknowledge_plan(viewer.client_id, plan, room.station.tick());
        stats.packets_sent = stats.packets_sent.saturating_add(1);
        stats.encoded_bytes = stats.encoded_bytes.saturating_add(byte_len);
    }

    for player in &mut room.players {
        while let Some(packet) = player
            .transport
            .try_recv()
            .expect("in-memory receive should succeed")
        {
            let mut decoder = BinaryFrameDecoder;
            let frame = decoder
                .decode_replication_ref(&packet.bytes)
                .expect("replication packet should validate");
            stats.client_checksum = checksum_word(stats.client_checksum, frame.client_id.get());
            for entity in frame.entities() {
                stats.client_checksum =
                    checksum_word(stats.client_checksum, entity.entity_id.get());
                for component in entity.components() {
                    stats.client_checksum = checksum_word(
                        stats.client_checksum,
                        u64::from(component.component_id.get()),
                    );
                    stats.client_checksum = checksum_word(stats.client_checksum, component.version);
                    stats.client_checksum = checksum_bytes(stats.client_checksum, component.bytes);
                }
            }
            stats.packets_received = stats.packets_received.saturating_add(1);
            stats.decoded_bytes = stats.decoded_bytes.saturating_add(packet.bytes.len());
        }
    }

    room.dirty_handles
        .sort_unstable_by_key(|handle| (handle.index(), handle.generation()));
    room.dirty_handles.dedup();
    for handle in room.dirty_handles.drain(..) {
        room.components.clear_dirty_for_entity(handle);
    }
}

fn move_entity(
    room: &mut Room,
    handle: EntityHandle,
    position: Position3,
    descriptors: &Descriptors,
    stats: &mut Stats,
) {
    room.station
        .move_owned(handle, position)
        .expect("benchmark owns moved entity");
    room.index.upsert_tracked(handle, position, Bounds::Point);
    room.components
        .set_blob_from_slice(
            &descriptors.transform,
            handle,
            room.station.tick().get(),
            &transform_bytes(position),
        )
        .expect("transform update should fit");
    room.dirty_handles.push(handle);
    stats.movement_updates = stats.movement_updates.saturating_add(1);
    stats.component_updates = stats.component_updates.saturating_add(1);
}

fn build_trace(config: Config) -> Vec<TickTrace> {
    (0..config.ticks)
        .map(|tick| TickTrace {
            recreate_room: (tick != 0 && tick.is_multiple_of(15))
                .then_some((tick / 15) % config.rooms),
        })
        .collect()
}

fn policies() -> PolicyTable {
    let mut policies = PolicyTable::default();
    policies.set(sectorsync_core::prelude::CompiledSyncPolicy::new(
        PolicyId::new(1),
        5,
        30,
        256.0,
    ));
    policies
}

fn room_checksum(room: &Room) -> u64 {
    let mut checksum = u64::try_from(room.id).unwrap_or(u64::MAX);
    for entity in room.station.iter() {
        checksum = checksum_word(checksum, entity.id.get());
        checksum = checksum_word(checksum, u64::from(entity.handle.index()));
        checksum = checksum_word(checksum, u64::from(entity.handle.generation()));
        checksum = checksum_word(checksum, u64::from(entity.position.x.to_bits()));
        checksum = checksum_word(checksum, u64::from(entity.position.z.to_bits()));
    }
    checksum
}

fn benchmark_ok(config: Config, stats: &Stats) -> bool {
    let expected_recreates = config.ticks.saturating_sub(1) / 15;
    !config.heavy_profile_denied
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && stats.commands_admitted == stats.commands_applied
        && stats.events_routed == stats.events_drained
        && stats.packets_sent == stats.packets_received
        && stats.encoded_bytes == stats.decoded_bytes
        && stats.packet_oversize == 0
        && stats.viewer_plans == stats.packets_sent
        && stats.selected_entities >= stats.encoded_entities
        && stats.encoded_entities > 0
        && stats.tracker_acks >= stats.tracker_records
        && stats.rooms_destroyed == expected_recreates
        && stats.rooms_created == config.rooms.saturating_add(expected_recreates)
        && stats.world_checksum != 0
        && stats.client_checksum != 0
}

#[allow(clippy::cast_precision_loss)]
fn print_report(config: Config, stats: &Stats, elapsed: Duration) {
    let expected_recreates = config.ticks.saturating_sub(1) / 15;
    println!("SectorSync dynamic gameplay benchmark");
    println!("requested_profile={:?}", config.requested_profile);
    println!("profile={:?}", config.profile);
    println!("allow_heavy={}", config.allow_heavy);
    println!("heavy_profile_denied={}", config.heavy_profile_denied);
    println!("guard_applied={}", config.guard_applied);
    println!("rooms={}", config.rooms);
    println!("players_min={MIN_PLAYERS}");
    println!("players_max={MAX_PLAYERS}");
    println!("entities_per_room={}", config.entities_per_room);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("rooms_created={}", stats.rooms_created);
    println!("rooms_destroyed={}", stats.rooms_destroyed);
    println!("expected_rooms_destroyed={expected_recreates}");
    println!("commands_admitted={}", stats.commands_admitted);
    println!("commands_applied={}", stats.commands_applied);
    println!("events_routed={}", stats.events_routed);
    println!("events_drained={}", stats.events_drained);
    println!("entities_spawned={}", stats.entities_spawned);
    println!("entities_despawned={}", stats.entities_despawned);
    println!("movement_updates={}", stats.movement_updates);
    println!("component_updates={}", stats.component_updates);
    println!("viewer_plans={}", stats.viewer_plans);
    println!("selected_entities={}", stats.selected_entities);
    println!("unexamined_after_budget={}", stats.unexamined_after_budget);
    println!("encoded_entities={}", stats.encoded_entities);
    println!(
        "selected_per_encoded={:.3}",
        stats.selected_entities as f64 / stats.encoded_entities.max(1) as f64
    );
    println!("encoded_components={}", stats.encoded_components);
    println!("packets_sent={}", stats.packets_sent);
    println!("packets_received={}", stats.packets_received);
    println!("encoded_bytes={}", stats.encoded_bytes);
    println!("decoded_bytes={}", stats.decoded_bytes);
    println!("tracker_records={}", stats.tracker_records);
    println!("tracker_acks={}", stats.tracker_acks);
    println!("packet_oversize={}", stats.packet_oversize);
    println!("world_checksum={}", stats.world_checksum);
    println!("client_checksum={}", stats.client_checksum);
    println!("retained_plan_slots={}", stats.retained_plan_slots);
    println!("retained_plan_entities={}", stats.retained_plan_entities);
    println!(
        "retained_query_candidates={}",
        stats.retained_query_candidates
    );
    println!("retained_command_slots={}", stats.retained_command_slots);
    println!(
        "retained_component_slots={}",
        stats.retained_component_slots
    );
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    print_percentiles("tick_ms", &stats.tick_ms);
    print_percentiles("command_ms", &stats.command_ms);
    print_percentiles("simulation_ms", &stats.simulation_ms);
    print_percentiles("replication_ms", &stats.replication_ms);
    println!(
        "threshold_command_conservation_ok={}",
        stats.commands_admitted == stats.commands_applied
    );
    println!(
        "threshold_event_conservation_ok={}",
        stats.events_routed == stats.events_drained
    );
    println!(
        "threshold_transport_conservation_ok={}",
        stats.packets_sent == stats.packets_received && stats.encoded_bytes == stats.decoded_bytes
    );
    println!("threshold_packet_budget_ok={}", stats.packet_oversize == 0);
    println!(
        "threshold_lifecycle_ok={}",
        stats.rooms_destroyed == expected_recreates
            && stats.rooms_created == config.rooms.saturating_add(expected_recreates)
    );
    println!("benchmark_ok={}", benchmark_ok(config, stats));
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1_000.0);
}

fn print_percentiles(name: &str, values: &[f64]) {
    println!("{name}_p50={:.3}", percentile(values, 0.50));
    println!("{name}_p95={:.3}", percentile(values, 0.95));
    println!("{name}_p99={:.3}", percentile(values, 0.99));
    println!("{name}_max={:.3}", percentile(values, 1.00));
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn parse_usize(value: &str, fallback: usize) -> usize {
    value.parse().unwrap_or(fallback)
}

fn room_entity_id(room: usize, local: usize) -> EntityId {
    EntityId::new(
        u64::try_from(room)
            .unwrap_or(u64::MAX)
            .saturating_mul(1_000_000)
            .saturating_add(u64::try_from(local + 1).unwrap_or(u64::MAX)),
    )
}

fn room_client_id(room: usize, player: usize) -> ClientId {
    ClientId::new(
        u64::try_from(room)
            .unwrap_or(u64::MAX)
            .saturating_mul(32)
            .saturating_add(u64::try_from(player + 1).unwrap_or(u64::MAX)),
    )
}

fn server_client_id(room: usize) -> ClientId {
    ClientId::new(10_000_000_u64.saturating_add(u64::try_from(room).unwrap_or(u64::MAX)))
}

fn command_id(room: usize, sequence: u64) -> u64 {
    u64::try_from(room)
        .unwrap_or(u64::MAX)
        .saturating_mul(1_000_000)
        .saturating_add(sequence)
}

fn benchmark_addr(room: usize, endpoint: usize) -> SocketAddr {
    let port = 10_000_usize
        .saturating_add(room.saturating_mul(MAX_PLAYERS + 1))
        .saturating_add(endpoint);
    SocketAddr::from((
        [127, 0, 0, 1],
        u16::try_from(port).expect("guarded benchmark port fits u16"),
    ))
}

fn entity_position(room: usize, local: usize) -> Position3 {
    let x = u16::try_from((local.saturating_mul(17) + room.saturating_mul(3)) % 192)
        .expect("coordinate fits u16");
    let z = u16::try_from((local.saturating_mul(29) + room.saturating_mul(5)) % 192)
        .expect("coordinate fits u16");
    Position3::new(f32::from(x), 0.0, f32::from(z))
}

fn moved_position(current: Position3, sequence: u64) -> Position3 {
    let delta_x = if sequence & 1 == 0 { 1.25 } else { -1.25 };
    let delta_z = if sequence & 2 == 0 { 0.75 } else { -0.75 };
    Position3::new(
        (current.x + delta_x).rem_euclid(192.0),
        current.y,
        (current.z + delta_z).rem_euclid(192.0),
    )
}

fn transform_bytes(position: Position3) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(24);
    bytes.extend_from_slice(&position.x.to_le_bytes());
    bytes.extend_from_slice(&position.y.to_le_bytes());
    bytes.extend_from_slice(&position.z.to_le_bytes());
    bytes.resize(24, 0);
    bytes
}

fn state_bytes(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

const fn checksum_word(checksum: u64, value: u64) -> u64 {
    checksum.wrapping_mul(0x0000_0100_0000_01b3) ^ value
}

fn checksum_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum = checksum_word(checksum, u64::from(*byte));
    }
    checksum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> std::vec::IntoIter<String> {
        values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn heavy_profiles_require_explicit_admission() {
        let denied = Config::from_args(args(&["--profile=large"]));
        assert_eq!(denied.profile, Profile::Smoke);
        assert!(denied.heavy_profile_denied);

        let admitted = Config::from_args(args(&["--profile=standard", "--allow-heavy"]));
        assert_eq!(admitted.profile, Profile::Standard);
        assert_eq!(admitted.rooms, STANDARD_ROOMS);
        assert!(!admitted.heavy_profile_denied);
    }

    #[test]
    fn manual_workload_is_clamped_without_heavy_admission() {
        let config = Config::from_args(args(&[
            "--rooms=500",
            "--entities-per-room=1024",
            "--ticks=1000",
        ]));
        assert_eq!(config.rooms, SMOKE_ROOMS);
        assert_eq!(config.entities_per_room, SMOKE_ENTITIES_PER_ROOM);
        assert_eq!(config.ticks, SMOKE_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn tiny_trace_is_deterministic_and_conserves_work() {
        let config = Config {
            rooms: 2,
            entities_per_room: 16,
            ticks: 3,
            ..Config::default()
        };
        let descriptors = Descriptors::new();
        let policies = policies();
        let trace = build_trace(config);
        let first = run(config, &descriptors, &policies, &trace);
        let second = run(config, &descriptors, &policies, &trace);

        assert!(benchmark_ok(config, &first));
        assert_eq!(first.commands_admitted, first.commands_applied);
        assert_eq!(first.packets_sent, first.packets_received);
        assert_eq!(first.world_checksum, second.world_checksum);
        assert_eq!(first.client_checksum, second.client_checksum);
    }
}
