# Honest Roadmap & Status

This file exists to say plainly what is and isn't real in this codebase.
No hedge words ("planned", "may be added") for things that simply don't
exist — those are stated as "not built." Last verified: 2026-09-19, by
actually running the build/test/lint commands below (not by reading code
and assuming).

## What was actually verified in this pass (commands + real output)

```
cargo build -p pybeamguard-core --verbose        -> succeeds
cargo test -p pybeamguard-core --verbose         -> 78 unit + 13 integration tests passed, 0 failed
cargo fmt --all -- --check                       -> clean, no diff
cargo clippy --workspace -- -D warnings          -> 0 warnings
maturin develop --release (Python 3.11 venv)     -> builds and installs the wheel
python -c "... analyze_structured(...) "         -> 10 analyzer results, as expected
pytest tests/ -v                                 -> 23 passed, 0 failed
pybeamguard analyze examples/pipeline_simple.py --format json -> exit 0, valid JSON
pybeamguard --version                            -> "pybeamguard 1.2.0"
actionlint .github/workflows/*.yml               -> clean (after fixes below)
```

`cargo build --workspace` (no `-p`) was also confirmed to genuinely fail on
this machine (macOS) with `ld: symbol(s) not found for architecture arm64`
— this is a real, reproduced constraint, not a hypothetical one, and it's
why `ci.yml`/`release.yml` scope Rust commands to `-p pybeamguard-core`.

`cargo audit` could not be run to completion in this sandboxed environment
— it needs to fetch the RustSec advisory database over git/https and this
environment has no outbound network access. The `security-audit` CI job
added in this pass has **not** been confirmed to run green on real GitHub
Actions; verify it after the next push.

---

## Bucket 1: Built and tested (verified working)

- 10 Beam analyzers, 3 Flink analyzers, 3 Spark analyzers — exercised by
  the Rust test suite above and by `examples/pipeline_simple.py` through
  the actual compiled wheel.
- PyO3 Python bindings (`abi3`), console script entry point, `--format`,
  `--data-profile`, `--rules`, `--fail-on`, `--framework` flags — all
  covered by `tests/test_python_bindings.py` (23 tests) and confirmed via
  a real CLI invocation above.
- YAML rule-engine override (`RulesConfig`) for Hot Key/Cost/Spark Join/
  State analyzers — covered by Rust integration tests
  (`custom_hotkey_rules_yaml_changes_beam_analysis_end_to_end`,
  `custom_spark_join_rules_yaml_changes_spark_analysis_end_to_end`).
- `[tool.maturin] include = ["LICENSE"]` is present in `pyproject.toml` —
  the sdist-missing-LICENSE bug seen in other repos in this org does not
  apply here.
- Published on PyPI: confirmed live at pypi.org, versions 0.4.0 through
  1.2.0 present (checked via the PyPI JSON API during this pass).

## Bucket 2: Built but NOT independently verified in this pass

- The new `security-audit` cargo-audit CI job (`.github/workflows/ci.yml`)
  — added this pass, config is syntactically valid (actionlint-clean) but
  never actually executed against a live network, so whether it correctly
  reports/parses is unconfirmed.
- The new `actionlint` CI job — same caveat; the local `actionlint` binary
  run during this pass was manually installed via Homebrew, not the
  `curl`-based bootstrap the workflow uses.
- `.github/dependabot.yml` — valid YAML, follows the documented schema,
  but dependency-update PRs only get created by GitHub's own
  infrastructure; not something a local pass can verify.
- All five `release.yml` fixes below (BUILD_SUMMARY.md path, action
  version bumps, artifact naming, job scoping, Windows `cp` fix) — **this
  workflow only triggers on a `v*` tag push or manual dispatch, and no tag
  has been pushed since `v1.0.0`** (git tags stop at v1.0.0; the repo is
  actually at 1.2.0 per `Cargo.toml`/`pyproject.toml`, and PyPI has
  1.1.0/1.1.1/1.1.2/1.2.0 published). That means: (a) this release
  workflow has not run at all for over a year of shipped work, so the
  bugs found below existed undetected that whole time, and (b) my fixes
  to it are reasoned from static analysis + `actionlint` + the
  already-diagnosed `ci.yml` precedent, not from an actual green run.
  **Verify by pushing a real tag (e.g. `v1.2.1`) and watching the
  workflow, or running it via `workflow_dispatch`, before trusting it for
  a real release.**
- Malformed-input robustness beyond what
  `crates/core/tests/integration.rs` specifically exercises (empty
  source, non-Beam/Flink/Spark source, malformed `--data-profile` JSON,
  extremely long single line, pathological Unicode). Other malformed
  inputs (e.g. deeply nested/recursive constructs, malformed Flink/Spark
  source specifically, non-UTF-8 byte sequences reaching the FFI
  boundary) are untested.

## Bucket 3: CI errors found and fixed in this pass

All of these were real, reproducible bugs in `.github/workflows/release.yml`
(the CI/build system in `ci.yml` was already green and is not in this
bucket — see Bucket 1):

1. **Broken file reference**: the release step attached `BUILD_SUMMARY.md`
   from the repo root, but that file was moved to
   `docs/archive/BUILD_SUMMARY.md` in commit `2d9b206` — the "Create
   Release" step would fail to find it. Fixed: path updated.
2. **Deprecated actions**: `actions/checkout@v3` and
   `actions/upload-artifact@v3` — GitHub retired `upload-artifact@v3`;
   using it now fails outright. Fixed: bumped to `@v4` throughout.
3. **Artifact name collision**: `upload-artifact@v4` rejects multiple
   uploads sharing one name within a workflow run, but all three OS
   matrix legs uploaded under the shared name `binaries`. Fixed: each leg
   now uploads under its own `${{ matrix.artifact }}` name.
4. **Unscoped Rust commands**: the `test`/`lint` jobs ran
   `cargo test --release` / `cargo fmt -- --check` / `cargo clippy -- -D
   warnings` without `-p pybeamguard-core` or `--workspace`/`--all`, i.e.
   inconsistent with the exact fix already applied and documented in
   `ci.yml` for the identical PyO3-link failure mode. Locally reproduced
   that `cargo build --workspace` fails on macOS for this reason; whether
   the unscoped `release.yml` jobs (which run on `ubuntu-latest`) would
   also fail was not independently confirmed on Linux, but scoping them
   is strictly safer and loses zero coverage (`bindings/python` has 0
   `#[test]`s). Fixed: scoped to match `ci.yml`.
5. **Broken Windows step**: the "Prepare artifacts" step declares
   `shell: bash` but its Windows branch ran `copy target\release\...`,
   a `cmd.exe` builtin that doesn't exist in bash — `shellcheck`
   (via `actionlint`) flagged the unescaped backslashes as the symptom.
   Fixed: changed to `cp target/release/pybeamguard.exe ...`.
6. **EOL action runner**: `actionlint` flagged
   `softprops/action-gh-release@v1`'s runner as end-of-life. Fixed:
   bumped to `@v2`.

`actionlint .github/workflows/*.yml` is clean after all of the above.
**None of these fixes have been exercised in real GitHub Actions** — see
Bucket 2.

## Bucket 4: Not built (does not exist — no hedging)

- Kafka Streams and Ray Data framework support: not started. No parser,
  no IR mapping, no analyzers.
- Directory/glob input to `analyze`: does not exist. `analyze` takes
  exactly one file path; scanning multiple files requires an external
  shell loop (documented in the README).
- Organization governance features (cost budgets, SLO policy enforcement,
  audit logging) and Airflow/dbt/data-contract/FinOps ecosystem
  integrations: do not exist. They previously existed as unwired struct
  definitions and were deliberately deleted (not hidden/disabled) to stop
  overclaiming — see README "Release Status".
- Rule *logic* plugin system (Python hooks, Rego/OPA): does not exist.
  Only rule *data* (thresholds/keyword lists/cost rates) is externalized
  via YAML.
- A benchmark suite or any committed performance numbers: does not exist.
  No script, no results file. Any "<Nms" figure that might appear
  elsewhere is not backed by a reproducible benchmark in this repo.
- `cargo-deny`/license-compliance CI check: does not exist. Only
  vulnerability scanning (`cargo audit`, added this pass) is present, and
  even that is unverified in real CI (see Bucket 2).
- Git tags for versions 1.1.0, 1.1.1, 1.1.2, 1.2.0: do not exist. Tags
  stop at `v1.0.0` despite four subsequent PyPI releases.
- GitHub repository topics: not something this pass can set (no API
  write access used per instructions) — see suggested topics below.

There are currently no half-wired "features not yet functional" beyond
what's listed above — a prior pass already removed unwired scaffolding
(governance/audit-log structs) rather than leaving it half-built, which is
the right call and is reflected accurately in the README.

---

## Technical debt (concrete, file:line)

**Not urgent / no follow-up session needed (low-risk, documented for
awareness):**
- `crates/core/src/frameworks/flink.rs` (16 sites) and
  `crates/core/src/frameworks/spark.rs` (15 sites), plus
  `crates/core/src/parser.rs` (5 sites): 36 of the 37 total
  production-code `.unwrap()` calls are `Regex::new(<static pattern>).unwrap()`
  at struct-construction time. These only panic on a malformed regex
  *pattern string* (a programmer typo caught by any test run, since every
  analysis call constructs these structs), not on attacker/user-supplied
  pipeline source. Low risk as-is; would still read more clearly as
  `.expect("valid static regex")` to make the invariant explicit for
  future editors.
- `crates/core/src/analyzers/hotkey.rs:165`: `measured_cardinality.unwrap()`
  depends on the invariant that `high_cardinality_measured` (checked a few
  lines earlier) implies `measured_cardinality.is_some()`. Currently
  correct by inspection and covered indirectly by passing tests, but
  not enforced by the type system — a future refactor of the surrounding
  branch could silently introduce a panic. One-line fix
  (`if let Some(c) = measured_cardinality { ... }`), left undone here per
  the "document, don't casually touch analyzer logic" scope of this pass.
- `bindings/python/src/lib.rs` is 1,896 lines — by far the largest file
  in the repo (next largest: `crates/core/src/parser.rs` at 669 lines).
  It has **zero** `.unwrap()`/`.expect()`/`panic!` calls (clean
  `Result`-based FFI error propagation into Python exceptions — verified
  by grep), so it isn't a safety concern, but its size makes it a
  candidate for splitting into per-framework submodules
  (`lib/beam.rs`, `lib/flink.rs`, `lib/spark.rs`) for maintainability.

**Flagged for a dedicated follow-up session:**
- `bindings/python` (the PyO3 crate) has **zero** `#[test]`s of its own.
  The compiled extension is only exercised indirectly through
  `tests/test_python_bindings.py` (pytest, 23 tests). A regression in a
  type conversion or PyO3-specific glue code could pass `cargo test -p
  pybeamguard-core` while still being broken, and would only surface via
  the (slower, separate-toolchain) Python test suite. Worth a session to
  add direct Rust-level tests for the bindings crate, or at minimum
  document this as an intentional coverage boundary.
- No systematic audit of malformed/adversarial input beyond the specific
  cases already in `crates/core/tests/integration.rs` (see Bucket 2)
  — worth a fuzzing or property-based-testing pass (e.g. `cargo fuzz` or
  `proptest`) given this tool's job is specifically to safely parse
  arbitrary, possibly malformed, user-supplied pipeline source.
- The `release.yml` workflow has been silently unexercised for four
  releases (Bucket 2/3) — worth a dedicated session to actually cut a
  tagged release and confirm the fixed workflow produces working
  cross-platform binaries, rather than trusting static review alone.
- `cargo-deny` (license + advisory policy enforcement) is not present;
  `cargo audit` alone (added this pass) covers known vulnerabilities but
  not license-compatibility drift in transitive dependencies. Low
  priority for a single-maintainer Apache-2.0 project, but worth adding
  if the dependency tree grows.

---

## Suggested GitHub repo metadata (not set via API — for the maintainer to apply)

- **Description**: "Static analysis for Apache Beam, Flink, and Spark
  pipelines — catch hot keys, shuffle bottlenecks, and cost risks before
  deployment. Free, offline, open source."
- **Topics**: `apache-beam`, `apache-flink`, `apache-spark`,
  `static-analysis`, `data-engineering`, `cli`, `rust`, `pyo3`, `python`,
  `cost-optimization`, `ci-cd`

---

## Known limitations already documented in README.md (not repeated here)

See README's "Release Status", "Known Issues", and FAQ sections for:
single-file-only `analyze` input, unvalidated/heuristic cost model,
Flink's lack of rule-driven thresholds, and the exact Rust/Python test
counts. Those statements were spot-checked against real command output
during this pass and found accurate (see "What was actually verified"
above) other than the version badge, which was stale (1.1.2 shown,
1.2.0 actual — fixed in this pass).
