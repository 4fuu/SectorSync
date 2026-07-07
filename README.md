# SectorSync

SectorSync is an embedded Rust library for high-performance, spatially aware
real-time entity replication.

It targets very large maps, many entities, many world instances, station-based
spatial ownership, low-latency command application, range/frustum culling,
interest management, adaptive update rates, hotspot splitting, and cross-station
event routing.

SectorSync is not a game engine or a full game server framework. It does not
own combat, inventory, quests, economy, persistence, deployment, service
discovery, or crash recovery. Game-specific systems are expected to integrate
through station-local APIs, command/event hooks, custom components, and
external transport/routing adapters.

## Core Direction

- Embedded Rust library first, not a daemon.
- CPU-first high-performance core; GPU acceleration is external or future
  optional adapter work.
- In-memory runtime state, with snapshot/restore/migration APIs but no built-in
  durable storage.
- Fixed 3D cell topology with dynamic station ownership.
- Exactly one authoritative owner station per entity at any point in time.
- Read-only ghost entities may exist in neighboring stations for AOI, visibility,
  prewarming, and query acceleration.
- Station-local execution is single-owner and mostly lock-free; multiple
  stations can run in parallel.
- Cross-station events are ordered at tick boundaries and do not use distributed
  transactions.
- Client connectivity, gateway processes, cluster orchestration, and production
  transport are integration concerns outside the core library.

## Phase 1 Scope

Phase 1 should produce a usable core library and a benchmark simulator:

- `sectorsync-core`: entity IDs, station IDs, cell IDs, 3D spatial grid,
  ownership model, station runtime primitives, command/event envelopes,
  sync policies, snapshot/restore hooks, migration primitives, and runtime
  barrier support.
- `sectorsync-bench`: deterministic workloads, simulated clients, simulated
  stations, baseline modes, and performance reports.
- `sectorsync-wire`: wire/frame traits and default frame types, including
  station event frames.
- `sectorsync-transport`: transport traits, fake transport support, byte-budget
  guards, bounded in-memory station packet transport, and lightweight
  standard-library UDP adapters for client and station packets.
- `sectorsync-runtime`: orchestration helpers for multi-station simulation.

The first implementation should stay resource-aware. The development machine is
not assumed to be a production benchmark host, so expensive tests must be
explicitly gated and default checks must stay lightweight.

## Workspace Layout

Current crates:

- `crates/sectorsync-core`: IDs, command envelopes, 3D spatial primitives,
  station-local entity storage, ghost/owner roles, dirty masks, compiled sync
  policies, custom component registry/storage, typed component codecs, schema
  helpers, generated-schema-friendly layout descriptors, cell indexing,
  interest queries, replication planning, bounded command/event queues, handoff
  transfer types, hotspot planning, gateway session/routing primitives, barrier
  metadata, and snapshot metadata.
- `crates/sectorsync-wire`: frame shapes plus default binary encode/decode for
  replication frames with entity/component delta payloads, client command
  ingress frames, internal gateway-to-station command dispatch frames, command
  acknowledgements, cross-station event frames, and barrier notifications. It
  also provides a replication frame builder that materializes dirty component
  deltas from a core replication plan.
- `crates/sectorsync-transport`: transport sink trait, batch packet API,
  byte-budget transport wrapper, fake transport for tests/benchmarks, bounded
  in-memory client packet hubs, bounded in-memory station-to-station packet
  transport, a non-blocking `std::net::UdpSocket` client packet adapter, and a
  non-blocking UDP station-to-station packet adapter with explicit station
  address registration. It also provides low-level reliable client and station
  packet helpers with bounded in-flight windows, ACKs, retries, timeout
  accounting, and duplicate suppression history, plus packet security envelope
  hooks for external authentication/encryption implementations, bounded replay
  windows, and bounded key lifecycle metadata for rotation/retirement/revocation
  policy.
- `crates/sectorsync-runtime`: in-process station collection helpers, a full
  runtime barrier controller for tick-boundary freeze/snapshot/resume flows, and
  an in-process entity migration executor built on two-phase handoff. It also
  includes dynamic cell ownership tables, conservative automatic split
  scheduling with cooldown/capacity/improvement guards, cell-level migration
  execution, a low-level deployment node/station route table, a station event
  router, bounded station event and command dispatch transport bridges, a
  bounded client replication send/receive transport bridge, a low-level client
  command/inbound-frame transport bridge, a runtime barrier notification
  transport bridge, a frozen snapshot upgrade executor, a bounded gateway client
  command transport bridge, a business-agnostic gateway command pipeline, and a
  simple station scheduler.
- `crates/sectorsync-bench`: deterministic lightweight benchmark executable.

Useful commands:

```bash
cargo test --workspace
cargo run -p sectorsync-bench -- --profile=smoke
cargo run -p sectorsync-bench -- --profile=smoke --baseline=full
cargo run -p sectorsync-bench --example sdk_flow
cargo run -p sectorsync-bench --example split_migration
cargo run -p sectorsync-bench --example split_tuning
cargo run -p sectorsync-bench --example replication_bridge
cargo run -p sectorsync-bench --example client_bridge
cargo run -p sectorsync-bench --example barrier_transport
cargo run -p sectorsync-bench --example barrier_upgrade
cargo run -p sectorsync-bench --example gateway_session
cargo run -p sectorsync-bench --example gateway_command_pipeline
cargo run -p sectorsync-bench --example gateway_deployment_dispatch
cargo run -p sectorsync-bench --example deployment_routing
cargo run -p sectorsync-bench --example udp_loopback
cargo run -p sectorsync-bench --example command_ingress
cargo run -p sectorsync-bench --example secure_command_ingress
cargo run -p sectorsync-bench --example secure_key_rotation
cargo run -p sectorsync-bench --example reliable_command_ingress
cargo run -p sectorsync-bench --example station_event_transport
cargo run -p sectorsync-bench --example udp_station_event
cargo run -p sectorsync-bench --example reliable_station_event
cargo run -p sectorsync-bench --example generated_schema
cargo run -p sectorsync-bench -- --profile=large --allow-heavy
```

The default smoke profile is intentionally small. Larger benchmark profiles must
be requested explicitly with `--allow-heavy`, for example `--profile=medium
--allow-heavy` or `--profile=large --allow-heavy`. Without `--allow-heavy`, a
heavy profile request stays on smoke-sized data and reports
`heavy_profile_denied=true`. Custom entity/client/station/tick values are also
clamped to a default-safe resource guard unless `--allow-heavy` is present; the
benchmark output reports `host_parallelism`, guard limits, and whether
`default_resource_guard_applied=true`.

## Performance Targets

The project is judged by multiple hard metrics together, not by a single number:

- Large entity scale.
- Large simulated client scale.
- Bounded per-client interest sets.
- Stable station tick latency.
- Low command-to-apply latency.
- Efficient downlink bandwidth estimates.
- High command/event/frame throughput.
- Hotspot detection, splitting, downgrade, or aggregation behavior.

The benchmark suite should include simple baselines such as full broadcast,
room broadcast, and naive grid AOI so SectorSync's policy-driven approach can be
measured against simpler strategies.

## Non-Goals

- Full ECS game framework.
- Built-in business persistence.
- Built-in crash recovery or failover.
- Built-in process manager or cluster scheduler.
- Mandatory GPU dependency.
- Mandatory production/full-featured client SDK in Phase 1.
- Dynamic script/WASM/plugin hot loading in Phase 1.

## Development Status

This repository is being built iteratively. The README and `AGENTS.md` are
living documents and should be updated whenever the architecture, rules, or
implementation scope changes materially.

Initial status:

- Git repository initialized on branch `main`.
- Rust workspace scaffolded.
- Core low-level SDK types exist for station ownership, 3D spatial indexing,
  interest queries, policy tables, replication planning, event queues, barriers,
  snapshots, commands, and fake transport integration.
- Runtime barrier controller can request scoped barriers, wait for station tick
  alignment, freeze, export snapshots, and resume.
- Runtime barrier notification bridge encodes barrier states into bounded client
  packet transport so pause/freeze/resume flows can notify clients without
  owning hot-update logic or connection management.
- Runtime barrier upgrade executor applies an external `RuntimeUpgradeHook` to
  frozen in-memory station snapshots and restores all migrated stations only
  after every migrated snapshot validates.
- Two-phase owner handoff primitives support target ghost prewarming, incoming
  owner commit, and source downgrade to short-lived ghost.
- Runtime migration executor can move an authoritative entity between in-process
  stations while leaving the old station with a short-lived ghost.
- Bounded command queues support priority ordering and barrier-aware
  buffer/reject/drain behavior.
- Gateway session table primitives support bounded client sessions, station
  routes, route epochs, reconnect generations, reconnect grace windows,
  disconnected-session expiry, replay/stale sequence rejection, and per-client
  per-tick command admission limits.
- Runtime gateway command pipeline decodes command frames, applies
  gateway/session metadata admission, queues accepted commands into target
  station queues, and encodes command ACKs for accepted or rejected commands
  without interpreting game payloads.
- Runtime gateway client command transport bridge pumps bounded client command
  packets into the gateway command pipeline, validates transport source metadata
  against the command frame client id, and sends produced ACKs back through
  client packet transport without owning sockets, reconnects, auth, or game
  validation.
- Deployment routing can resolve a connected gateway client's station route into
  node delivery metadata, including gateway/station/node route epochs. Runtime
  gateway command dispatch can return a stamped command envelope plus a
  deployment delivery route for external node transports.
- Wire codec supports client command ingress frames that convert into
  `CommandEnvelope` after the server stamps `received_at`, plus command ACK
  frames for the return path. Command payloads remain opaque to SectorSync.
- Wire codec supports internal command dispatch frames for gateway-to-station
  node delivery. These frames preserve the gateway-stamped `received_at` tick
  and target station while keeping command payloads opaque.
- Runtime command dispatch transport bridge encodes gateway-stamped command
  envelopes into internal dispatch frames, moves them through bounded station
  packet transport, validates packet targets, and enqueues decoded commands into
  target station command queues.
- Custom component registry and sparse blob storage allow external systems to
  register game-owned data without forcing a full ECS framework.
- Typed component codecs and schema helpers support compact user-defined
  component encoding without forcing a serialization framework.
- Generated component schema helpers support static field layout descriptors,
  stable schema hashes, validation for duplicate/overlapping/out-of-bounds
  fields, and registry integration for external code generators.
- Wire codec supports binary encode/decode for replication, command ACK, and
  barrier frames. Replication frames can carry concrete entity/component deltas.
- Replication frame builder converts `ReplicationPlan` + `ComponentStore` into
  concrete wire payloads with bounded entity/component materialization.
- Runtime replication transport bridge plans AOI for a viewer, builds a concrete
  replication frame from component storage, skips empty frames by default,
  encodes the frame, and submits it to bounded client packet transport.
- Runtime replication receive bridge consumes bounded client packet transport,
  validates optional source client metadata and target client id, decodes
  replication frames, and reports received entity/component counts.
- Runtime client transport bridge encodes client command frames to a configured
  server/gateway target and pumps client-bound ACK, replication, and barrier
  frames with source/target validation while leaving client state and game
  payload interpretation outside SectorSync.
- Transport SDK supports packet batches and byte-budget enforcement wrappers.
- Packet security helpers support bounded security envelopes, key ids, nonces,
  authentication tags, pluggable authenticator/cipher traits, explicit
  plaintext cipher mode for tests/integrations, bounded replay windows, and
  bounded key ring metadata for send-key selection, receive-key acceptance,
  activation, retirement, revocation, and expiration.
- Bounded in-memory client transport hubs support explicit local endpoints,
  per-client queue capacity, packet byte limits, source-client stamping, and
  delivery statistics for deterministic SDK tests or adapter prototypes.
- Bounded station-to-station packet transport supports explicit target station
  registration, per-station queue capacity, packet byte limits, and delivery
  statistics for in-process simulations or adapter prototypes.
- Standard UDP transport adapter supports non-blocking localhost/network packet
  send/receive, explicit client-to-address registration, and bounded reusable
  receive buffers while keeping reliability/session concerns outside the core.
- UDP station transport adapter supports one-local-station sockets, explicit
  station-to-address registration, endpoint checks, non-blocking receive, and
  byte/packet statistics for low-level cross-process station packet prototypes.
- Deployment route table primitives support bounded node registration, station
  placement routes, route epochs, node heartbeat timestamps, draining/offline
  state, per-node station capacity checks, route moves, route removal, and stale
  node detection/offline marking.
- Runtime event router queues cross-station events by target station and drains
  events once their target tick is ready.
- Runtime station event transport bridge encodes typed station events into wire
  frames, moves them through bounded station packet transport, validates packet
  endpoints, and routes decoded events into the target station router.
- Reliable client packet helpers wrap arbitrary client/server packet payloads
  with a bounded ACK/retry envelope, per-peer in-flight limits, payload budgets,
  timeout counters, required source-client identity, and bounded duplicate
  suppression history.
- Reliable station packet helpers wrap arbitrary station packet payloads with a
  bounded ACK/retry envelope, per-target in-flight limits, payload budgets,
  timeout counters, and bounded duplicate suppression history.
- Hotspot planner evaluates station/cell load samples and proposes high-pressure
  cells for external schedulers to move.
- Cell ownership table and cell migration executor can apply split proposals and
  migrate owner entities found in moved cells while refreshing source/target
  station indexes.
- Split scheduler can evaluate station load samples, choose a lower-load target,
  produce bounded split actions, update ownership, and execute cell migrations.
  It includes planning guards for source cooldown, minimum source/target score
  improvement, target score capacity after move, warm-target admission, and
  explicit skip counters for tuning.
- Smoke benchmark runs through planning, frame encoding, fake transport, and
  hotspot report fields. It also reports command enqueue/apply counts,
  gateway/deployment command dispatch transport counts, low-level
  client/gateway transport command/ACK/replication roundtrip counts, command
  latency in ticks, max queue depth, payload entity/component delta counts,
  tick timing estimates, host parallelism, default resource guard limits,
  threshold checks, and an aggregate `benchmark_ok` verdict.
- `cargo run -p sectorsync-bench --example sdk_flow` demonstrates an
  end-to-end embeddable SDK path: station, cell index, component store,
  replication plan, frame builder, binary codec, and fake transport.
- `cargo run -p sectorsync-bench --example split_migration` demonstrates a
  load-sample-driven split scheduler producing and executing a cell migration.
- `cargo run -p sectorsync-bench --example split_tuning` demonstrates split
  scheduler cooldown and target-capacity guard behavior without running a heavy
  benchmark profile.
- `cargo run -p sectorsync-bench --example replication_bridge` demonstrates a
  low-level downlink path: viewer AOI planning, replication frame building,
  bounded in-memory client transport send, receive, source/target validation,
  and decode.
- `cargo run -p sectorsync-bench --example client_bridge` demonstrates a
  low-level client/gateway SDK path: command frame send, gateway transport
  pump, ACK return, replication downlink, and client-bound frame pumping through
  bounded in-memory transport.
- `cargo run -p sectorsync-bench --example barrier_transport` demonstrates a
  runtime barrier freeze/snapshot/resume flow that sends Frozen and Running
  notifications through bounded client transport and receives them with the
  low-level client bridge.
- `cargo run -p sectorsync-bench --example barrier_upgrade` demonstrates a
  frozen in-memory snapshot migration hook and station restore flow without
  adding script loading or game-specific update logic to SectorSync.
- `cargo run -p sectorsync-bench --example gateway_session` demonstrates a
  low-level gateway session table connecting a client, routing commands into
  station command queues, rerouting to another station, rate-limiting a command,
  and reconnecting inside a grace window.
- `cargo run -p sectorsync-bench --example gateway_command_pipeline`
  demonstrates a reusable gateway command frame pipeline that turns command
  bytes into station queue entries and ACK bytes while preserving gateway
  rate-limit rejection.
- `cargo run -p sectorsync-bench --example gateway_deployment_dispatch`
  demonstrates gateway-admitted command bytes resolving to deployment node
  delivery routes before and after a station route move, then sending the
  stamped command envelope through bounded station transport into the target
  station command queue.
- `cargo run -p sectorsync-bench --example deployment_routing` demonstrates a
  low-level deployment route table registering nodes, assigning station routes,
  marking a node draining, moving a station route to another node, and marking a
  stale node offline.
- `cargo run -p sectorsync-bench --example udp_loopback` demonstrates a
  replication frame encoded by `sectorsync-wire`, sent through the UDP transport
  adapter over localhost, received, and decoded back into a runtime frame.
- `cargo run -p sectorsync-bench --example command_ingress` demonstrates a
  client command frame sent over UDP, decoded by the server, converted into a
  bounded command queue entry, applied, and acknowledged back to the client.
- `cargo run -p sectorsync-bench --example secure_command_ingress`
  demonstrates a client command and command ACK wrapped in packet security
  envelopes with an external authenticator hook, plus replay rejection for a
  duplicate secure command.
- `cargo run -p sectorsync-bench --example secure_key_rotation`
  demonstrates bounded packet key metadata driving initial send/open, rotated
  key selection, receive-only old-key retirement, and revoked-key rejection
  while leaving secret storage and key distribution outside SectorSync.
- `cargo run -p sectorsync-bench --example reliable_command_ingress`
  demonstrates a client command frame wrapped in a reliable client packet
  envelope, retried once, duplicate-suppressed at the server, applied, and then
  acknowledged back to the client through a separate reliable command ACK
  payload.
- `cargo run -p sectorsync-bench --example station_event_transport`
  demonstrates a typed cross-station event encoded into a wire frame, delivered
  through bounded station transport, pumped into the target router, and drained
  at the target tick.
- `cargo run -p sectorsync-bench --example udp_station_event` demonstrates the
  same station event bridge over localhost UDP station transports.
- `cargo run -p sectorsync-bench --example reliable_station_event`
  demonstrates a typed cross-station event encoded as a wire frame, wrapped in
  a reliable station packet envelope, retried once, duplicate-suppressed at the
  target, acknowledged, routed, and drained at the target tick.
- `cargo run -p sectorsync-bench --example generated_schema` demonstrates an
  externally generated component schema descriptor registering into the core
  registry, writing a typed component, and materializing a replication frame.

Not complete yet:

- Long-running split scheduler calibration against production telemetry and
  heavier workload profiles.
- Production authentication/encryption implementations, secret storage, key
  distribution, certificate rotation, NAT traversal, external service
  discovery, production cluster integration, and long-running reliability
  calibration beyond the low-level gateway/session, deployment routing, packet
  security hooks/key lifecycle metadata, reliable client/station packet helpers,
  and in-memory/UDP packet adapters.
- Production gateway process orchestration for client connectivity.
- Large-scale benchmark validation against the stated hard metrics.
