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

`cargo audit` could not be run to completion in the sandboxed environment
used for the 2026-09-19 pass — it needs to fetch the RustSec advisory
database over git/https and that environment had no outbound network
access. **Update (2026-09-21 quick-fix pass): re-run with real network
access.** `cargo audit` completed successfully — 77 crate dependencies
scanned, 0 vulnerabilities found, exit code 0. The `security-audit` CI
job's `cargo audit` step has been promoted from `continue-on-error: true`
to a hard gate in `.github/workflows/ci.yml` accordingly. It still has
not been confirmed to run green on real GitHub Actions (only locally);
verify that after the next push.

## Bugs found and fixed in a deep-review pass (2026-09-27)

This pass specifically hunted for real logic bugs that survive a linter/CI
pass -- silent false negatives, off-by-ones, stale/inconsistent values --
by reading analyzer/parser logic line-by-line and reproducing each
suspicion with a failing test before touching code. Three real,
independently-reproduced bugs were found and fixed; each has a regression
test that fails on the pre-fix code and passes after. `cargo test -p
pybeamguard-core` went from 78 unit + 13 integration to 82 unit + 15
integration (all passing); `cargo fmt --all -- --check` and `cargo clippy
--workspace -- -D warnings` stayed clean; `pytest tests/` stayed at 23
passed after rebuilding the wheel with `maturin develop --release`.

1. **`crates/core/src/parser.rs`: `GlobalWindows()` was never detected as a
   windowing operation at all.** `windowing_re` matched only
   `Windowing|FixedWindows|SlidingWindows|SessionWindows`. A real
   `beam.WindowInto(GlobalWindows())` line contains none of those literal
   substrings (`WindowInto` is not `Windowing`), so no `Windowing` IR node
   was ever created for it -- which silently disabled
   `WINDOWING_GLOBAL_NO_TRIGGER` (a High-severity finding whose entire
   purpose is to catch exactly this case: a global window with no trigger
   that will never emit in an unbounded stream) for every pipeline that
   actually uses `GlobalWindows()`. It only ever fired in hand-built-IR
   unit tests that bypassed the real parser. Verified with a failing
   end-to-end test (`beam_global_windows_without_trigger_flagged_end_to_end`
   in `crates/core/tests/integration.rs`) before the fix, passing after.
   Fixed by adding `WindowInto` to `windowing_re` so any `WindowInto(...)`
   call is captured regardless of which window class is used.
2. **`crates/core/src/analyzers/reliability.rs`: `RELIABILITY_NO_ERROR_HANDLING`
   was gated on a pipeline-wide flag instead of the specific node's own
   state.** The per-node check for "does this parsing/decoding ParDo lack
   error handling" used `!has_error_handling && !_has_side_outputs`, where
   both variables were "does *any* ParDo anywhere in the whole pipeline
   have side outputs" (the same predicate computed twice under different
   names) -- not whether *this* ParDo has one. A parsing ParDo with zero
   error handling of its own was silently never flagged as long as some
   unrelated ParDo elsewhere in the pipeline happened to have side outputs
   for its own purposes -- a real false negative in exactly the kind of
   "will malformed input silently fail" check this analyzer exists for.
   Verified with a failing test
   (`test_unrelated_pardo_side_output_does_not_mask_parse_finding`) before
   the fix, passing after. Fixed by matching the node's own
   `has_side_outputs` field in the per-node loop instead of the pipeline-
   wide aggregate (which is kept, under a clearer name, as the separate
   `has_error_handling`/`has_side_outputs` pipeline-level metrics feeding
   the overall reliability score -- that usage was already correct).
3. **`crates/core/src/analyzers/synthesis.rs` / `text_reporter.rs`: two
   differently-scaled metrics shared the literal display phrase "Overall
   Risk Score" in the same report.** `TextReporter` prints a top-line
   `Overall Risk Score: N/100` computed from summed finding severities
   (higher = riskier). `SynthesisEngine`'s own summary line printed
   `Overall Risk Score N/100 (EXCELLENT - Production Ready / ... / CRITICAL
   - Do Not Deploy)` computed from `overall_risk_score`, which is the
   *opposite* polarity (higher = healthier/more production-ready). Running
   `pybeamguard analyze` produces both lines in the same text report, so a
   reader could reasonably compare "Overall Risk Score: 0/100" at the top
   against "Overall Risk Score 82/100 (EXCELLENT)" further down as if they
   were the same number moving in the same direction, when they are
   opposite scales that happen to share a name -- confirmed concretely by
   running the CLI on `examples/pipeline_simple.py`, which prints exactly
   this pair. Fixed by renaming only the human-readable string in
   `SynthesisEngine`'s summary to "Pipeline Health Score" (the underlying
   `overall_risk_score` field/metric name is unchanged, since it's part of
   the JSON output and Python bindings' public surface and renaming it
   would be a breaking API change, not a "trivial and obviously correct"
   fix). Regression test:
   `text_report_overall_risk_score_label_is_not_ambiguous` in
   `crates/core/tests/integration.rs`, which asserts the phrase appears
   exactly once in a real end-to-end text report.

None of the above were previously listed anywhere in this file or
`CHANGELOG.md`.

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

- The `security-audit` cargo-audit CI job (`.github/workflows/ci.yml`) —
  **update (2026-09-21): now run locally against a live network** (77
  crates scanned, 0 vulnerabilities, exit 0), and the job promoted from
  `continue-on-error: true` to a hard gate. Still unconfirmed on real
  GitHub Actions specifically (installer/cache behavior on their runners
  could differ) — verify after the next push.
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
- ~~`crates/core/src/analyzers/hotkey.rs:165`: `measured_cardinality.unwrap()`~~
  **Fixed (2026-09-21 quick-fix pass).** Replaced with an explicit
  `if let Some(cardinality) = measured_cardinality.filter(|_|
  high_cardinality_measured)` match, so the panic no longer depends on
  an invariant between two separate branches. Verified: `cargo test -p
  pybeamguard-core` (78 unit + 13 integration, all passing), `cargo
  clippy --workspace -- -D warnings` (0 warnings), `cargo fmt --all --
  --check` (clean), `pytest tests/` (23 passed).
- `bindings/python/src/lib.rs` is 1,896 lines — by far the largest file
  in the repo (next largest: `crates/core/src/parser.rs` at 669 lines).
  It has **zero** `.unwrap()`/`.expect()`/`panic!` calls (clean
  `Result`-based FFI error propagation into Python exceptions — verified
  by grep), so it isn't a safety concern, but its size makes it a
  candidate for splitting into per-framework submodules
  (`lib/beam.rs`, `lib/flink.rs`, `lib/spark.rs`) for maintainability.

**Flagged for a dedicated follow-up session:**
- `crates/core/src/analyzers/deployment.rs` (`DeploymentAnalyzer`): 7 of its
  8 findings (`DEPLOY_SMALL_MACHINE`, `DEPLOY_EXCESSIVE_MAX_WORKERS`,
  `DEPLOY_HIGH_MIN_WORKERS`, `DEPLOY_NON_US_REGION`,
  `DEPLOY_NO_AUTOSCALING_ALG`, `DEPLOY_NO_STREAMING_ENGINE`, and the
  identically-gated `BESTPRAC_STREAMING_ENGINE` in `best_practices.rs`) all
  read `ir.metadata.deployment_config: Option<DeploymentConfig>`
  (`crates/core/src/ir.rs:35`), but **nothing in the shipped product ever
  sets it to `Some`.** `BeamPipelineParser::parse` (and the Flink/Spark
  parsers) never populate it, `PipelineIR::new` always sets it to `None`
  (`crates/core/src/ir.rs:123`), and neither the CLI (`--data-profile`
  loads a `DataProfile`, not a `DeploymentConfig`; there is no
  `--deployment-config` flag) nor any of the ~30 `#[pyfunction]`s in
  `bindings/python/src/lib.rs` accept one (confirmed by grep: the only
  `deployment`-related string in that file is a help-text analyzer-name
  listing, not a real parameter). The practical effect: through the CLI or
  Python bindings — the only two ways a real user runs this tool — a
  `deployment_config` for a would-be pipeline is *always* `None`, so
  `DeploymentAnalyzer` can only ever emit `DEPLOY_NO_CONFIG` and the other
  7 findings are dead code from a user's perspective, even though
  `ROADMAP_HONEST.md`'s "Built and tested" bucket and the README's "10
  analyzers" claim are both technically true (the Rust unit tests in
  `deployment.rs` do exercise those 7 findings, by constructing a
  `PipelineIR` with `deployment_config` set by hand — a path no shipped
  entry point can reach). Not fixed here: closing this gap means adding
  and documenting a real `--deployment-config <file.json>` CLI flag and a
  matching Python API parameter, a scope/design decision, not a one-line
  fix.
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
