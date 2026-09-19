# Security Policy

## Supported versions

PyBeamGuard is a single-maintainer, pre-1.0-maturity-in-spirit project
(currently at 1.2.0). There is no formal LTS or backport policy — only the
latest released version on PyPI / GitHub Releases receives fixes.

| Version | Supported |
|---------|-----------|
| 1.2.x (latest) | Yes |
| < 1.2.0 | No — please upgrade |

## Reporting a vulnerability

Please report security issues privately rather than opening a public
GitHub issue:

- Email: mullassery@gmail.com
- Or use [GitHub's private vulnerability reporting](https://github.com/Mullassery/PyBeamGuard/security/advisories/new)
  if enabled for this repository.

Include:
- A description of the issue and its potential impact.
- Steps to reproduce, or a minimal proof-of-concept pipeline/input.
- The PyBeamGuard version and installation method (PyPI wheel, standalone
  binary, or built from source).

There is no bug bounty program. As a single-maintainer project, response
time is best-effort — expect an initial acknowledgment within a week, not
guaranteed same-day.

## What counts as a security issue here

PyBeamGuard is a static analysis CLI that reads local pipeline source
files (and optional local JSON/YAML config files) and prints a report. It
does not execute the pipeline code it analyzes, make network calls, or run
with elevated privileges. Realistic security-relevant classes for this
project are:

- A crafted pipeline source file, `--data-profile` JSON file, or `--rules`
  YAML file that causes a crash, panic, hang, or resource exhaustion
  (denial of service) rather than a clean error. The Rust core has 114+
  `.unwrap()`/`.expect()` call sites (see `ROADMAP_HONEST.md`) that have
  not been systematically audited for panic-on-malformed-input; the
  integration test suite covers some malformed-input cases
  (`crates/core/tests/integration.rs`: empty source, non-Beam source,
  malformed data-profile JSON, pathological Unicode) but this is not a
  claim of full coverage.
- A memory-safety issue in the Rust core or the PyO3 bindings layer
  (`bindings/python/src/lib.rs`) reachable from untrusted input.
- Supply-chain issues in dependencies — see `cargo audit` results in CI
  (`.github/workflows/ci.yml`, `security-audit` job, informational/
  non-blocking as of this writing — see `ROADMAP_HONEST.md`).

This tool does not process secrets, credentials, or PII by design, and has
no telemetry or network calls of its own.

## Known gaps

See the "Technical Debt" section of `ROADMAP_HONEST.md` for the current,
honest list of unaudited panic surface and dependency-scanning gaps —
these are documented there rather than silently left undocumented.
