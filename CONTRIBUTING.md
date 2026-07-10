# Contributing to SectorSync

SectorSync welcomes focused fixes, performance improvements, tests,
documentation, and low-level integration adapters.

## Scope

Keep changes within the embedded middleware boundary described in
[README.md](README.md#what-sectorsync-is-not) and [AGENTS.md](AGENTS.md). Game
business logic, production account systems, durable cluster state, process
orchestration, and mandatory GPU execution do not belong in the core workspace.

## Development Workflow

1. Create a focused branch and keep commits scoped to meaningful milestones.
2. Add tests for behavioral changes and an example when the external SDK flow
   changes.
3. Run the default quality gate:

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   cargo doc --workspace --no-deps
   cargo run -q -p sectorsync-bench -- --profile=smoke
   git diff --check
   ```

4. Update README or the relevant guide when public behavior, crate boundaries,
   benchmark fields, or invariants change.

Do not run medium, large, or manual heavy benchmarks without an explicit reason
and `--allow-heavy`. Include the profile, host context, repeated measurements,
and before/after results with performance changes.

## Pull Requests

Describe the problem, the chosen boundary, behavior changes, and verification
commands. Keep unrelated refactors out of the same pull request. Never commit
secrets, production keys, captured user traffic, or proprietary game data.

By contributing, you agree that your contributions are licensed under the MIT
License.
