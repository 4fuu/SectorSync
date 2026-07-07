# Agent Instructions for SectorSync

## Local Command Rules

- Use `python3` for temporary Python scripts.
- If a Python project uses `uv`, run scripts with `uv run main.py` or
  `uv run python -c`.
- Prefer `rg` / `rg --files` for searches.
- Keep default checks lightweight. This machine is not a production benchmark
  host, so do not run heavy stress tests unless explicitly requested.
- When a benchmark must consume substantial CPU or memory, add a small default
  profile and gate larger profiles behind explicit arguments.
- Default Rust verification should start with `cargo test --workspace`.
- Use `cargo run -p sectorsync-bench -- --profile=smoke` for the default
  benchmark smoke test, including the lightweight gateway/deployment command
  dispatch transport workload.
- Use `cargo run -p sectorsync-bench --example replication_bridge` when changing
  runtime replication transport bridges, AOI-to-frame downlink flow, or client
  replication transport integration.
- Use `cargo run -p sectorsync-bench --example gateway_session` when changing
  gateway session/routing primitives, reconnect grace behavior, route epochs,
  replay checks, or per-client admission limits.
- Use `cargo run -p sectorsync-bench --example gateway_command_pipeline` when
  changing runtime gateway command frame decoding, gateway admission to station
  queue routing, or command ACK generation.
- Use `cargo run -p sectorsync-bench --example gateway_deployment_dispatch`
  when changing gateway-to-deployment delivery route resolution or gateway
  command dispatch reports/frames/transport bridges for external node
  transports.
- Use `cargo run -p sectorsync-bench --example deployment_routing` when changing
  deployment node/station route tables, node heartbeat/stale checks, draining
  behavior, route moves, or placement capacity rules.
- Use `cargo run -p sectorsync-bench --example udp_loopback` when changing the
  UDP transport adapter or wire/transport integration.
- Use `cargo run -p sectorsync-bench --example command_ingress` when changing
  command wire frames, command queues, or ingress/ACK integration.
- Use `cargo run -p sectorsync-bench --example secure_command_ingress` when
  changing packet security envelopes, authenticator/cipher hooks, replay
  windows, or secure command ingress examples.
- Use `cargo run -p sectorsync-bench --example secure_key_rotation` when
  changing packet key lifecycle metadata, send-key selection, receive-key
  acceptance, retirement, revocation, or expiration behavior.
- Use `cargo run -p sectorsync-bench --example reliable_command_ingress` when
  changing reliable client packet helpers, in-memory client transport hubs, or
  reliable command ingress examples.
- Use `cargo run -p sectorsync-bench --example station_event_transport` when
  changing station event frames, station transport, or event router bridging.
- Use `cargo run -p sectorsync-bench --example udp_station_event` when changing
  UDP station transport or generic station event bridge behavior.
- Use `cargo run -p sectorsync-bench --example reliable_station_event` when
  changing reliable station packet helpers, station event reliability examples,
  or ACK/retry/duplicate-suppression behavior.
- Use `cargo run -p sectorsync-bench --example generated_schema` when changing
  component schema helpers, generated layout descriptors, or schema hashes.
- Do not run `--profile=medium` or `--profile=large` as part of routine checks
  unless the user asks for heavier validation.
- Heavy benchmark profiles require `--allow-heavy`. Do not add a default path
  that runs heavy profiles implicitly.
- Manual benchmark scale overrides must remain guarded by default. If
  `--entities`, `--clients`, `--stations`, or `--ticks` can exceed smoke-safe
  values, keep them clamped unless `--allow-heavy` is present, and print the
  applied guard metadata.

## Project Boundary

SectorSync is a high-performance embedded Rust library for spatial real-time
entity replication. It is not a full game server framework.

The core library owns:

- 3D cell topology and spatial indexing.
- Dynamic station ownership.
- Entity authority and read-only ghost semantics.
- Station-local command/event application.
- AOI, range culling, frustum filtering hooks, and sync policy planning.
- Adaptive update-rate planning.
- Hotspot metrics and split/migration primitives.
- Full runtime barrier primitives for pause/snapshot/upgrade/resume.
- Snapshot/restore/migration interfaces.
- Benchmarkable low-level APIs.

The core library does not own:

- Combat, inventory, quests, economy, or other game business rules.
- Durable persistence, crash recovery, failover, or backups.
- Process management, service discovery, deployment, or cluster scheduling.
- Mandatory GPU execution.
- Production gateway or client SDK in the first phase.

## Architecture Rules

- Every entity has exactly one authoritative owner station at a time.
- Ghost entities are read-only. They can support AOI, visibility, prewarming,
  and candidate queries, but cannot make final state changes.
- Two-phase handoff must prewarm target ghosts before owner commit and must
  downgrade the old owner to a short-lived ghost after commit.
- Cell-level migration must update both ownership metadata and source/target
  spatial indexes. Do not move a cell without preserving source ghost visibility
  during the handoff window.
- Split scheduler changes should remain conservative by default: bounded actions,
  bounded moved cells, and preference for lower-load target stations.
- Runtime barrier work must preserve the sequence: request, align to tick
  boundary, freeze, snapshot or migrate, resume.
- Command queues must remain bounded and barrier-aware. Do not add unbounded
  command buffers on hot paths.
- Gateway/session primitives must remain low-level and bounded: session tables,
  route epochs, reconnect generations, replay/stale sequence checks, expiry,
  and per-client command admission limits are allowed; sockets, auth providers,
  NAT traversal, deployment discovery, durable account state, and production
  process orchestration stay outside core.
- Runtime gateway command pipelines may decode command frames, call gateway
  metadata admission, enqueue into bounded station command queues, and encode
  ACKs. They must not perform game-rule validation, anti-cheat decisions,
  account auth, reconnect loops, NAT traversal, blocking network IO, or
  unbounded buffering.
- Gateway-to-deployment dispatch may resolve admitted command routes into node
  delivery metadata and return stamped command envelopes for external
  transports. It must remain metadata-only: no service discovery, process
  supervision, remote procedure framework, hidden retries, or durable cluster
  state.
- Deployment routing primitives must remain metadata-only and bounded: node
  registration, heartbeat ticks, station placement routes, route epochs,
  draining/offline state, stale checks, and capacity guards are allowed;
  service discovery, process supervision, cloud APIs, durable cluster state, and
  automatic failover orchestration stay outside runtime.
- Command wire frames and command envelopes are business-agnostic containers.
  SectorSync may encode, decode, queue, stamp `received_at`, and acknowledge
  them, but schema validation, anti-cheat, and game-rule translation belong in
  external validators before commands are applied.
- Internal command dispatch frames may carry gateway-stamped command envelopes
  from a gateway process to a station node. They must preserve `received_at`
  and target station metadata without interpreting the command payload or
  replacing external service discovery and transport security.
- Command dispatch transport bridges may encode stamped commands into station
  packets, validate packet targets, and enqueue into bounded station command
  queues. They must not add hidden blocking IO, unbounded buffering, retry
  loops, service discovery, or game-rule validation.
- Reliable client packet helpers may carry command, command ACK, replication, or
  integration-defined frames, but they must not interpret game semantics or
  replace external command validation.
- Custom component work should keep SectorSync as a low-level SDK. Do not turn
  it into a mandatory ECS framework; expose descriptors, storage, and hooks.
- Component codecs should stay dependency-light by default. Prefer traits and
  explicit binary codecs over adding a mandatory serialization framework.
- Generated schema helpers are for external code generators and hand-written
  static descriptors. Do not add mandatory proc macros, build scripts, or
  runtime reflection to the core crate.
- Station-local APIs may be low-level and high-performance, but they must not
  bypass owner, dirty, replication-budget, barrier, or event-ordering invariants.
- Station internals should favor single-owner, lock-minimal execution.
- Multiple stations may run in parallel and communicate by bounded messages.
- Cross-station events should be tick-boundary ordered and idempotent where
  needed. Do not introduce distributed transactions in the core.
- Station-to-station transport must use station identifiers and bounded packet
  queues. Do not reuse client transport abstractions for station event routing.
- Station event transport bridges must validate packet endpoints against decoded
  frames before routing events into target queues.
- Reliable client packet helpers must preserve bounded per-peer in-flight
  windows, payload budgets, retry attempts, timeout accounting, required source
  client identity, and bounded duplicate suppression history. Do not hide a
  production gateway, authentication layer, reconnect loop, blocking wait, or
  unbounded replay buffer inside these helpers.
- Reliable station packet helpers must preserve bounded in-flight windows,
  payload budgets, retry attempts, timeout accounting, and bounded duplicate
  suppression history. Do not introduce unbounded replay buffers, hidden
  blocking waits, or per-entity reliability work in transport hot paths.
- UDP station transport instances represent one local station. They must reject
  source/target station mismatches instead of silently forwarding malformed
  station packets.
- Wire and transport abstractions must stay at frame/packet/batch boundaries.
  Avoid per-entity transport abstraction on hot paths.
- Transport implementations must be non-blocking or externally bounded at the
  station tick boundary. Do not introduce blocking receives, unbounded packet
  queues, or hidden per-entity network work in core transport adapters.
- Packet security helpers must remain framing and policy hooks: bounded
  envelopes, key ids, nonces, authenticator/cipher traits, and replay windows
  are allowed. Bounded key lifecycle metadata for activation, send-key
  selection, receive-key acceptance, retirement, revocation, and expiration is
  allowed, but secret material, mandatory crypto dependencies, hard-coded
  algorithms, key rotation services, certificate stores, and account auth
  systems stay outside SectorSync.
- The standard UDP adapter is a low-level packet adapter only. Reliability,
  encryption, authentication, reconnect, NAT traversal, and gateway/session
  semantics must not be hidden inside the UDP adapter. Use explicit reliable
  station packet helpers or outer integration layers when reliability is needed.
- UDP examples/tests must stay localhost-only, use bounded retry loops, and
  avoid long sleeps or external network dependencies.
- Replication frame changes must preserve entity/component delta payload support
  and maintain binary encode/decode roundtrip tests.
- Replication transport bridges may plan AOI, build bounded replication frames,
  encode wire bytes, and submit packets to client transport. They must not add
  hidden persistence, blocking client IO, unbounded per-client buffers, or game
  payload interpretation.
- SDK-level changes should include or update an example/integration test when
  they affect the expected external usage flow.
- Split/migration changes should keep `cargo run -p sectorsync-bench --example
  split_migration` working as the executable usage example.
- Split scheduler policy changes should also run
  `cargo run -p sectorsync-bench --example split_tuning` and preserve cooldown,
  target-capacity, and score-improvement guard behavior.
- Runtime-configurable sync policies must compile into compact hot-path data.
  Avoid hot-path scripts, hash maps, per-entity dynamic dispatch, or avoidable
  allocation.
- Keep GPU work outside the core. If acceleration is needed later, add optional
  adapter crates and keep CPU fallback semantics.

## Documentation Rules

- Keep `README.md` current when project scope, goals, or module layout changes.
- Keep this `AGENTS.md` current when development rules, safety constraints, or
  architectural invariants change.
- Prefer short design notes near the code being introduced. Avoid large stale
  design documents unless the implementation needs them.
- When a new crate, benchmark mode, runtime invariant, or public SDK boundary is
  introduced, update `README.md` in the same iteration.

## Git Rules

- Use multiple focused commits for meaningful milestones.
- Do not rewrite or discard user changes.
- Before committing, inspect `git status --short`.
- Commit messages should state the project-level milestone, not just file names.
