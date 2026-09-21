# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

This file was introduced during a documentation/maturity pass on
2026-09-19. Versions and changes prior to that date are **not**
retroactively reconstructed here to avoid fabricating history — see
`git log` for the real commit history, and `docs/archive/` for retired
phase/completion reports. The current released version at the time this
file was created is **1.2.0** (published on PyPI and tagged up to
`v1.0.0` in git; `v1.1.x`/`v1.2.0` were version-bumped but not git-tagged
— see `ROADMAP_HONEST.md`).

## [Unreleased]

## [1.2.1] - 2026-09-22

### Added
- `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, this
  `CHANGELOG.md`, and `ROADMAP_HONEST.md`.
- `.github/dependabot.yml` (cargo, pip, and github-actions ecosystems).
- `.github/ISSUE_TEMPLATE/bug_report.yml` and `feature_request.yml`,
  `.github/pull_request_template.md`.
- A `security-audit` job in CI running `cargo audit` (informational,
  non-blocking — see `ROADMAP_HONEST.md`).

### Fixed
- `.github/workflows/release.yml`: referenced `BUILD_SUMMARY.md` at the
  repo root, which no longer exists there (moved to
  `docs/archive/BUILD_SUMMARY.md` in a prior commit) — the release's
  "Create Release" step would have failed to attach it.
- `.github/workflows/release.yml`: bumped `actions/checkout@v3` →
  `@v4` and `actions/upload-artifact@v3` → `@v4` (v3 of
  `upload-artifact` is deprecated/retired by GitHub); also gave each
  matrix OS's upload a unique artifact name (`${{ matrix.artifact }}`
  instead of a shared `binaries` name), since `upload-artifact@v4` no
  longer allows multiple uploads under the same name across a matrix.
- `.github/workflows/release.yml`: the `test`/`lint` jobs ran
  `cargo test --release` / `cargo fmt -- --check` / `cargo clippy -- -D
  warnings` without scoping to `-p pybeamguard-core`, unlike `ci.yml`
  (which was fixed for exactly this failure mode — see its inline
  comment and `README.md`'s Known Issues). Scoped them to match.
- `.github/workflows/release.yml`: the Windows branch of the "Prepare
  artifacts" step used `copy target\release\pybeamguard.exe ...`, a
  Windows `cmd.exe` builtin, inside a step explicitly declared
  `shell: bash` — `copy` does not exist in bash. Changed to
  `cp target/release/pybeamguard.exe ...`.
- `.github/workflows/release.yml`: `softprops/action-gh-release@v1` →
  `@v2` (actionlint flagged v1's runner as end-of-life).
- `README.md`: version badge said `1.1.2`; actual current version
  (`Cargo.toml`/`pyproject.toml`) is `1.2.0`.
- `.gitignore`: removed a `Cargo.lock` ignore rule that was already
  ineffective (the file is, correctly, tracked in git — this workspace
  produces binaries, so Cargo.lock should be committed) but misleading
  to read.
- `crates/core/src/analyzers/hotkey.rs:165`: replaced a
  `measured_cardinality.unwrap()` inside the `HOTKEY_HIGH_RISK` finding
  description with an explicit `if let Some(cardinality) =
  measured_cardinality.filter(|_| high_cardinality_measured)` match.
  Behavior is unchanged (still only formats the cardinality when the
  same condition that used to gate the unwrap holds), but it no longer
  relies on an invariant between two separate branches to avoid a
  panic — a future refactor of the surrounding code can't silently
  reintroduce one. Verified via `cargo test -p pybeamguard-core` (78
  unit + 13 integration tests, all passing) and `cargo clippy
  --workspace -- -D warnings` (0 warnings).

### Changed
- `.github/workflows/ci.yml`: the `security-audit` job's `cargo audit`
  step was run for real against the live RustSec advisory database in
  this pass (network access available this time) — 77 crate
  dependencies scanned, 0 vulnerabilities found. Promoted the job from
  `continue-on-error: true` to a hard gate now that it's confirmed to
  pass rather than being unverified (see `ROADMAP_HONEST.md`).
