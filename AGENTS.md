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
- Do not run `--profile=medium` or `--profile=large` as part of routine checks
  unless the user asks for heavier validation.

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
- Runtime barrier work must preserve the sequence: request, align to tick
  boundary, freeze, snapshot or migrate, resume.
- Command queues must remain bounded and barrier-aware. Do not add unbounded
  command buffers on hot paths.
- Custom component work should keep SectorSync as a low-level SDK. Do not turn
  it into a mandatory ECS framework; expose descriptors, storage, and hooks.
- Station-local APIs may be low-level and high-performance, but they must not
  bypass owner, dirty, replication-budget, barrier, or event-ordering invariants.
- Station internals should favor single-owner, lock-minimal execution.
- Multiple stations may run in parallel and communicate by bounded messages.
- Cross-station events should be tick-boundary ordered and idempotent where
  needed. Do not introduce distributed transactions in the core.
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
