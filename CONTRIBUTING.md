# Contributing to PyBeamGuard

Thanks for considering a contribution. This is a small, part-time-maintained
project (single maintainer), so please keep pull requests focused and
include real test evidence — see below.

## Project layout

- `crates/core/` — the Rust analysis engine (`pybeamguard-core`): parser,
  IR, analyzers, reporting, rule engine. This is where almost all real
  logic lives.
- `crates/core/src/bin/pybeamguard.rs` — the standalone Rust CLI binary
  (distributed via GitHub Releases, no Python involved).
- `bindings/python/` — the PyO3 `extension-module` crate (`pybeamguard`)
  that exposes `pybeamguard-core` to Python. Currently has no `#[test]`s
  of its own; it's a thin wrapper and is exercised end-to-end by the
  Python test suite instead.
- `src/pybeamguard/` — the Python package: `cli.py` (the `pybeamguard`
  console script installed by `pip install pybeamguard`) and `__init__.py`
  (re-exports the compiled native module).
- `tests/test_python_bindings.py` — Python-side tests (bindings + CLI,
  run with `pytest`).
- `crates/core/tests/integration.rs` — Rust integration tests.
- `examples/` — example pipeline source files used by tests and docs.
- `docs/archive/` — retired planning/phase-report documents, kept for
  history only. Do not treat anything in there as current status.

## Local setup

Requires Rust 1.70+ and Python 3.10+.

```bash
git clone https://github.com/Mullassery/PyBeamGuard.git
cd PyBeamGuard

# Rust core + CLI binary
cargo build --release --bin pybeamguard

# Python bindings (into your active venv)
pip install maturin pytest
maturin develop
```

**Important:** `cargo build --workspace` (or a bare `cargo test`/`cargo
build` at the workspace root without `-p`) fails to link on macOS, because
`bindings/python`'s `pyo3` dependency is declared with the
`extension-module` feature unconditionally, which needs maturin's
dynamic-lookup linker flags. Always scope Rust commands to the core crate:

```bash
cargo build -p pybeamguard-core
cargo test -p pybeamguard-core
```

`bindings/python` is only ever built via `maturin build`/`maturin develop`.
`cargo fmt --all` and `cargo clippy --workspace` are safe to run
workspace-wide (they don't need the final link step).

## Running the test suite

```bash
# Rust unit + integration tests
cargo test -p pybeamguard-core

# Formatting and lint (both enforced in CI, zero warnings allowed)
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings

# Python bindings + CLI tests (after `maturin develop`)
pytest tests/
```

All four of the above are what CI actually runs (`.github/workflows/ci.yml`)
— run them locally before opening a PR. Please report *actual* command
output in your PR description (see the PR template), not just "tests
pass."

## Adding a new analyzer

Each analyzer:
1. Lives in `crates/core/src/analyzers/<name>.rs`.
2. Is registered in `crates/core/src/analyzers/registry.rs` (or
   `frameworks/*.rs` for Flink/Spark-specific analyzers).
3. Needs at least one fixture-based test — either a `#[test]` in the same
   file or a case added to `crates/core/tests/integration.rs`.
4. If it introduces new tunable thresholds/patterns, wire them into
   `crates/core/src/rules.rs` (`RulesConfig`) rather than hardcoding
   `const`s, so they're YAML-overridable like the existing Hot Key/Cost/
   Spark Join/State analyzers.
5. Should be documented in the README's "Framework Support" section with
   the exact conditions it flags — this project's README is held to a
   no-overclaiming standard; describe what the analyzer actually checks,
   not what it's aspirationally meant to check.

## Reporting bugs / requesting features

Use the GitHub issue templates (`.github/ISSUE_TEMPLATE/`). For bugs,
please include a minimal pipeline source snippet that reproduces the
problem — the analyzers are regex/line-based, so exact source text often
matters.

## Honesty in docs and PRs

This project's README and `ROADMAP_HONEST.md` are deliberately blunt about
what is and isn't built/tested. When contributing:
- Don't describe untested code as working.
- Don't add "planned"/marketing language for things that don't exist yet —
  say plainly that they don't exist.
- If your PR changes behavior described in the README, update the README
  in the same PR.

## License

By contributing, you agree that your contributions will be licensed under
the [Apache License 2.0](LICENSE), the same license as the rest of the
project.
