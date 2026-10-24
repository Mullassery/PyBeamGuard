# Competitive intelligence in roadmap and upgrade work

**Policy date:** 2026-10-19

Competitive findings are an input to product decisions, not a one-off report.
Every pending roadmap item, new feature, and routine dependency upgrade should
check the repository's [`AUDIT_FINDINGS.md`](AUDIT_FINDINGS.md) before work
starts, and record the decision in the PR or changelog.

## Required check before building

1. **Read `AUDIT_FINDINGS.md`.** Find the relevant competitor row and the
   ranked next actions. If the finding is stale, update it with a dated source.
2. **Name the competitor and the user problem.** A feature is not justified by
   "competitor has it". State the workflow it unlocks, who needs it, and how
   often. Mark unverified demand as a hypothesis.
3. **Prefer reuse over new code.** Check whether an existing Mullassery repo
   already provides the capability (see `BUNDLES.md` in the profile repo and
   the bundle role file in this repo, when present). Typical reuse:
   - crawling/fetching/robots/SSRF → **MudFish**
   - architecture maps / PR diffs → **CodeAtlas**
   - data-quality contracts / validation → **StatGuardian**
   - token counting / cost tables → **PyTokenCalc**
   - dependency graph / advisories / SBOM → **PyDeepMesh**
   - segmentation / CLV / churn → **ClusterAudienceKit**
   - activation / lineage / PII → **PyReverseETL**
   - fleet state sync → **SyncFleet** (only after conflict resolution)
4. **Choose one disposition:** build, integrate, defer, or reject. Write it
   down. A rejection with a reason is a successful outcome.
5. **For routine upgrades**, check whether the upgrade closes a known
   competitor gap or breaks a differentiated capability. Run the tests that
   cover the affected path; do not merge a green build that silently changes a
   user-visible contract.

## PR / changelog checklist

- [ ] Competitor and source URL/date recorded (or "no direct competitor").
- [ ] User impact and frequency stated; unverified demand labelled.
- [ ] Reuse check done against sibling repos/bundles.
- [ ] Failure mode and rollback considered (especially auth, PII, cost).
- [ ] Docs/roadmap status updated honestly.
- [ ] Tests or a manual verification command included.

## Review cadence

- Refresh the competitor row when a release changes the capability.
- Re-check the full `AUDIT_FINDINGS.md` at least quarterly and before a major
  release.
- If a finding is wrong or stale, correct it in the same PR that relies on it.

## Non-goals

- Copying features for parity.
- Adding a dependency just because a competitor uses it.
- Treating star counts or download counts as proof of user need.

## OSS gap analysis — 2026-10-24

**What matters most here:** AST-grade analysis and validated cost. Regex/line matching and unvalidated rates limit it to linting; deepen the analyzers and back cost forecasts with real pipeline bills before selling pre-deployment assurance.

Scope: open-source pipeline runtimes and data validators that overlap
PyBeamGuard's reliability, shuffle, hot-key, and cost checks. No active OSS
project found with the same static, multi-engine scope. Dates/licenses verified 2026-10-24.

### Comparable OSS projects

- **Apache Beam** — `https://github.com/apache/beam`; Apache-2.0; v2.77.0
  (2026-10-07). Runtime transform/coder validation; no source lint or cost forecast.
- **Apache Flink** — `https://github.com/apache/flink`; Apache-2.0;
  release-2.3.0 (2026-06-14). Checkpoint, state-backend, and watermark behavior
  are validated at runtime/submission; no static source analysis.
- **Apache Spark** — `https://github.com/apache/spark`; Apache-2.0; active 4.x
  line with release candidates in October 2026. Query plans and Structured
  Streaming are validated at runtime; no static lint or cost forecast.
- **Great Expectations** — `https://github.com/fivetran/great_expectations`;
  Apache-2.0; v1.24.1 (2026-10-22). Runtime data-quality validation; it does not
  inspect pipeline code or estimate pipeline cost.

### Capability comparison

| Capability | PyBeamGuard | Apache Beam | Apache Flink | Apache Spark | Great Expectations |
| --- | --- | --- | --- | --- | --- |
| Static source analysis | Yes | No | No | No | No |
| Graph extraction | Yes | Yes | Partial | Partial | No |
| Hot key detection | Yes | No | Partial | Partial | No |
| Shuffle analysis | Yes | No | Partial | Partial | No |
| Windowing/watermark checks | Yes | Partial | Partial | Partial | No |
| State/checkpoint checks | Yes | Partial | Partial | Partial | No |
| Cost forecast | Partial | No | No | No | No |
| Configurable rules/severity gate | Yes | No | No | No | Partial |
| Runtime execution/observability | No | Yes | Yes | Yes | Partial |
| Multi-engine static coverage | Yes | No | No | No | No |
| SARIF/HTML output | No | No | No | No | Partial |

### Gaps that matter (ranked)

1. **Native analyzers are regex/line-based.** `--ast` adds an inventory and two
   AST-only findings, but the ten Beam analyzers and six Flink/Spark analyzers
   still pattern-match source text. False positives and missed constructs are
   expected. Effort: L. Build AST/IR-backed analyzers; the native rewrite is
   currently blocked.
2. **Cost estimates are unvalidated.** Figures come from simplified rates and
   optional profiles, with no comparison to real bills or committed accuracy
   study. Effort: M. Build a validation set before calling cost a planning input.
3. **No real pipeline corpus or false-positive benchmark.** Unit and integration
   tests exercise examples, not diverse production pipelines; performance
   claims are not benchmarked. Effort: M. Build a corpus and publish precision/
   recall and runtime results.
4. **Packaging and release hygiene.** PyPI ships only a macOS arm64 wheel and
   reports license metadata as `None`; git tags stop at v1.0.0 while the package
   is 1.5.0. Effort: S/M. Build a wheel matrix and release process.
5. **Output interoperability.** Only text and JSON are supported; there is no
   SARIF for code scanning or HTML for reviewers, despite `--fail-on` gating.
   Effort: S. Build SARIF first.
6. **Flink/Spark depth is shallow.** Three heuristics per engine, no YAML rule
   engine for Flink, and no AST path. Effort: M. Build selectively based on
   user demand rather than claiming parity.
7. **Docs drift.** `ROADMAP_HONEST.md` says directory scanning is not built, but
   `analyze <dir>` is implemented and tested; align the roadmap and README.
   Effort: S. Build.

### Where we are differentiated

- Offline pre-deployment analysis across Beam, Flink, and Spark with no cloud account or running job.
- Hot-key, shuffle, window, state, reliability, and cost checks in one pass.
- A Rust core with PyO3 bindings and a standalone binary.
- YAML threshold overrides, data-profile inputs, and severity-based CI gating.
- It complements runtime engines and data validators rather than replacing them.

### Disposition

**Build.** P1 is AST-backed analysis, cost/accuracy validation, and packaging;
P2 is SARIF/HTML and deeper Flink/Spark coverage. Reject Kafka Streams/Ray
expansion and do not describe regex heuristics as semantic analysis.

### Sources

- https://github.com/apache/beam/releases/tag/v2.77.0
- https://raw.githubusercontent.com/apache/beam/master/LICENSE
- https://github.com/apache/flink/releases/tag/release-2.3.0
- https://raw.githubusercontent.com/apache/flink/master/LICENSE
- https://github.com/apache/spark
- https://raw.githubusercontent.com/apache/spark/master/LICENSE
- https://github.com/fivetran/great_expectations/releases/tag/1.24.1
- https://pypi.org/project/pybeamguard/1.5.0/
- Repository evidence: `README.md`, `AUDIT_FINDINGS.md`, `ROADMAP_HONEST.md`, `analyzers`, `cli.py`.
