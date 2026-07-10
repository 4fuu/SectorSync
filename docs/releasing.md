# Release Process

This checklist prepares matching SectorSync crate versions for publication. It
does not publish automatically or replace repository-host release controls.

## Prerequisites

- Replace any unreleased changelog section with a version and release date.
- Confirm the repository host and private security-reporting channel are final.
- After creating the GitHub repository, add its real URL to
  `[workspace.package].repository` and inherit it from every published crate;
  never publish the removed placeholder URL.
- Confirm all published crates use the same version and Rust version.
- Run from a clean worktree on the release commit.
- Enable GitHub private vulnerability reporting.

## Quality Gate

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
cargo run -q -p sectorsync-bench -- --profile=smoke
git diff --check
```

Run all four guarded smoke baselines when benchmark logic, report fields, or
acceptance thresholds changed.

## Package Audit

Inspect packaged files before publishing:

```bash
cargo package -p sectorsync-core --list
cargo package -p sectorsync-wire --list
cargo package -p sectorsync-transport --list
cargo package -p sectorsync-runtime --list
```

Published crates depend on matching `0.1` versions. Publish in dependency order:

1. `sectorsync-core`
2. `sectorsync-wire`
3. `sectorsync-transport`
4. `sectorsync-runtime`

Wait until each crate is available from the registry before publishing its
dependents. `sectorsync-bench` is intentionally not published.

## GitHub Release Workflow

Run the `Manual release` workflow from `main` and enter the version without a
`v` prefix. Keep `dry_run` enabled first. The workflow repeats the release gate,
builds Git source archives, generates SHA-256 checksums, and uploads the result
as a short-lived workflow artifact.

After reviewing those artifacts, run the workflow again with `dry_run` disabled
to create `v<version>` and attach the same source archives and checksums to a
GitHub Release. This workflow deliberately does not publish to crates.io or use
a registry token.

Validate workflow syntax locally after changing CI:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
```

## GitHub Upload Audit

Before the first push, inspect tracked and untracked files, ignored build
outputs, unusually large files, private absolute paths, credentials, private
keys, generated artifacts, and placeholder URLs. Repeat the scan across Git
history when importing an existing repository. Do not rewrite history merely to
remove harmless historical documentation or placeholders.

## Release Evidence

Record the commit, Rust toolchain, package checksums, quality-gate output, smoke
profile output, and release notes. Tag only the exact commit that passed the
release gate.
