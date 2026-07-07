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
  benchmark smoke test.
- Use `cargo run -p sectorsync-bench --example udp_loopback` when changing the
  UDP transport adapter or wire/transport integration.
- Use `cargo run -p sectorsync-bench --example command_ingress` when changing
  command wire frames, command queues, or ingress/ACK integration.
- Use `cargo run -p sectorsync-bench --example station_event_transport` when
  changing station event frames, station transport, or event router bridging.
- Use `cargo run -p sectorsync-bench --example udp_station_event` when changing
  UDP station transport or generic station event bridge behavior.
- Use `cargo run -p sectorsync-bench --example generated_schema` when changing
  component schema helpers, generated layout descriptors, or schema hashes.
- Do not run `--profile=medium` or `--profile=large` as part of routine checks
  unless the user asks for heavier validation.
- Heavy benchmark profiles require `--allow-heavy`. Do not add a default path
  that runs heavy profiles implicitly.

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
- Command wire frames and command envelopes are business-agnostic containers.
  SectorSync may encode, decode, queue, stamp `received_at`, and acknowledge
  them, but schema validation, anti-cheat, and game-rule translation belong in
  external validators before commands are applied.
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
- UDP station transport instances represent one local station. They must reject
  source/target station mismatches instead of silently forwarding malformed
  station packets.
- Wire and transport abstractions must stay at frame/packet/batch boundaries.
  Avoid per-entity transport abstraction on hot paths.
- Transport implementations must be non-blocking or externally bounded at the
  station tick boundary. Do not introduce blocking receives, unbounded packet
  queues, or hidden per-entity network work in core transport adapters.
- The standard UDP adapter is a low-level packet adapter only. Reliability,
  encryption, authentication, reconnect, NAT traversal, and gateway/session
  semantics belong in outer integration layers unless explicitly scoped later.
- UDP examples/tests must stay localhost-only, use bounded retry loops, and
  avoid long sleeps or external network dependencies.
- Replication frame changes must preserve entity/component delta payload support
  and maintain binary encode/decode roundtrip tests.
- SDK-level changes should include or update an example/integration test when
  they affect the expected external usage flow.
- Split/migration changes should keep `cargo run -p sectorsync-bench --example
  split_migration` working as the executable usage example.
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
