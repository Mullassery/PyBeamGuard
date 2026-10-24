# PyBeamGuard

Catch Apache Beam, Flink, and Spark pipeline failures before deployment — offline static analysis for reliability risks, hot keys, expensive shuffles, and rough cost forecasts.

[![CI](https://github.com/Mullassery/PyBeamGuard/actions/workflows/ci.yml/badge.svg)](https://github.com/Mullassery/PyBeamGuard/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pybeamguard)](https://pypi.org/project/pybeamguard/)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> **Not to be confused with [PyAirflowTester](https://github.com/Mullassery/PyAirflowTester):** PyBeamGuard analyzes Apache Beam/Dataflow pipelines; PyAirflowTester targets Airflow and dbt.

## Which project do I need?

Part of the Mullassery open-source family. These tools have similar names — here is the short version of who does what:

| If you need to… | Use |
| --- | --- |
| Build a repository's dependency graph and match it against vulnerability advisories (Python, Rust, Node, containers) | [PyDeepMesh](https://github.com/Mullassery/PyDeepMesh) |
| Understand *why* a Python dependency exists, who added it, and whether it is actually used | [PyDependencyCheck](https://github.com/Mullassery/PyDependencyCheck) |
| Statically analyze a **dbt** project (risky tests, test blast radius, cost) | [PyDBTGuard](https://github.com/Mullassery/PyDBTGuard) |
| Statically analyze **Apache Beam / Dataflow** pipelines (cost, hot keys, reliability) | [PyBeamGuard](https://github.com/Mullassery/PyBeamGuard) |
| Validate **Airflow** DAGs and dbt runs | [PyAirflowTester](https://github.com/Mullassery/PyAirflowTester) |
| Simulate cascading-failure **blast radius** across infrastructure, data pipelines, and AI systems | [PyBlastRadius](https://github.com/Mullassery/PyBlastRadius) |
| Check the **quality of the data itself** at runtime (schema, drift, anomalies) | [StatGuardian](https://github.com/Mullassery/StatGuardian) |
| Discover **devices, services, and vulnerabilities on a network** | [PyNetworkIntel](https://github.com/Mullassery/PyNetworkIntel) |
| Discover and score **API security** risk (real traffic, agent/MCP, Cedar policy) | [PyAPICheck](https://github.com/Mullassery/PyAPICheck) |
| Trace and root-cause **AI agent** failures | [PyAgentHound](https://github.com/Mullassery/PyAgentHound) |
| Diagnose **RAG retrieval** failures | [PyVectorHound](https://github.com/Mullassery/PyVectorHound) |
| Orchestrate **local, dependency-aware tasks** (DAG scheduler + content-addressed cache) | [PyTaskWireCheck](https://github.com/Mullassery/PyTaskWireCheck) |

[Browse all projects →](https://github.com/Mullassery?tab=repositories)

## What it is

PyBeamGuard is a static analysis platform for data pipelines. Point it at a
pipeline source file and it extracts the pipeline graph and runs a set of
analyzers over it, reporting findings with severities, a health score, and a
rough cost estimate. It is built as a Rust core with Python bindings (PyO3), and
ships both as a `pip install`-able package and as a standalone Rust CLI binary
with no Python runtime.

It works offline, needs no cloud account, and uses no vendor services. The
analysis is regex/line-based over the source — it is a fast heuristic pre-flight
check, not a full AST/type-level compiler front end.

## Features / Highlights

- **10 Apache Beam analyzers**: graph intelligence, hot key detection, shuffle
  analysis, windowing validation, state auditing, cost intelligence, reliability
  analysis, best-practices checks, deployment auditing, and architecture review.
- **First-class Flink analyzers** (`--framework flink`): checkpointing
  (`FlinkCheckpointAnalyzer`), state-backend choice (`FlinkStateAnalyzer`), and
  watermark/windowing correctness (`FlinkWatermarkAnalyzer`).
- **First-class Spark analyzers** (`--framework spark`): shuffle partitioning
  (`SparkShuffleAnalyzer`), join/broadcast strategy (`SparkJoinAnalyzer`), and
  Structured Streaming safety (`SparkStreamingAnalyzer`).
- **Configurable rule engine (YAML)**: override hot-key keyword patterns,
  cardinality/size thresholds, and cost rates via `--rules rules.yaml` (Beam and
  Spark only); any field you omit keeps its built-in default.
- **CI gating**: `--fail-on <severity>` exits non-zero when any finding meets or
  exceeds the given severity.
- **Data-profile-aware estimates**: `--data-profile profile.json` replaces some
  flat per-operation guesses with your throughput/state figures.
- **Two report formats**: human-readable text (default) and machine-readable
  JSON.
- **Dual distribution**: a PyPI package (`pip install pybeamguard`) and a
  standalone Rust binary from GitHub Releases.

## Who it's for / Use cases

- **Data engineers** validating a pipeline before it ships: "will this scale,
  and roughly what will it cost?"
- **Platform teams** adding a pre-deployment gate to CI/CD with
  `pybeamguard analyze pipeline.py --fail-on critical`.
- **FinOps teams** attributing spend to specific stages by inspecting the
  `CostAnalyzer` metrics in JSON output.
- **Anyone porting between engines** who wants a comparable set of reliability
  checks across Beam, Flink, and Spark.

## What it is not

- **Not a pipeline runner or deployment tool.** It reads source code; it never
  executes, deploys, or mutates your pipeline.
- **Not a full compiler/AST analyzer.** Analysis is regex/line-based over the
  pipeline source, so it is fast but can miss constructs a real parser would
  understand.
- **Not a validated billing forecast.** Cost figures are rough, pre-deployment
  heuristics from simplified pricing assumptions — treat them as an
  order-of-magnitude planning signal, not a quote.
- **Directory scanning** — `analyze <dir>` recursively analyzes every `.py`
  file (skipping caches/virtualenvs); JSON output is a list of per-file reports.
- **Optional AST inventory** — `analyze --ast` reports actual transforms,
  sources/sinks, and windowing, flags unbounded sources with no window, and
  flags `GroupByKey` without a combiner.
- **No Kafka Streams or Ray Data support** — neither has been started.
- **No LLM features.** All analysis and recommendations are deterministic rules.
- **Not organization governance tooling.** Budgets/SLOs/policy enforcement and
  audit logging were deliberately removed rather than left as untested stubs.

## Project status

**Beta / working proof-of-concept.** The Beam, Flink, and Spark analysis paths,
the CLI, the Python bindings, and the YAML rule engine are implemented and
covered by tests, and CI builds and tests the core crate. See
[docs/internal/ROADMAP_HONEST.md](docs/internal/ROADMAP_HONEST.md) for a blunt, file:line-level accounting of
what is tested, what is not built, and known technical debt.

## Installation

Requires **Python 3.10+** for the PyPI package.

```bash
pip install pybeamguard
# or, with uv:
uv pip install pybeamguard

pybeamguard --version
```

Standalone Rust binary (no Python required) — download the artifact for your
platform from the [releases page](https://github.com/Mullassery/PyBeamGuard/releases):

```bash
chmod +x pybeamguard-macos-universal   # or the linux/windows artifact
./pybeamguard-macos-universal --version
```

From source (Rust 1.70+ and Python 3.10+):

```bash
git clone https://github.com/Mullassery/PyBeamGuard.git
cd PyBeamGuard

# Python package via PyO3 bindings, installed into your active venv:
pip install maturin
maturin develop

# OR the standalone Rust CLI binary (no Python involved):
cargo build --release --bin pybeamguard
./target/release/pybeamguard --version
```

## Quick start

```bash
# Text report (default)
pybeamguard analyze pipeline.py

# JSON report
pybeamguard analyze pipeline.py --format json

# Fail CI on any critical finding
pybeamguard analyze pipeline.py --fail-on critical
```

Example output shape:

```
=== PyBeamGuard Analysis Report ===

Pipeline Health Score: 78/100
Total Findings: 5

HIGH PRIORITY ISSUES
• Large shuffle stage in join operation
• Missing dead-letter queue on parse failures

Estimated Cost: $2,300/month → Optimized: $1,350/month
```

## Usage

- **Analyze a Flink job**:
  ```bash
  pybeamguard analyze streaming_job.py --framework flink
  ```

- **Analyze a Spark job**:
  ```bash
  pybeamguard analyze etl.py --framework spark
  ```

- **Override analyzer thresholds/patterns**:
  ```yaml
  # rules.yaml — every section/field is optional; omitted fields keep defaults.
  hotkey:
    high_risk_patterns: ["customer", "tenant", "region"]
    low_cardinality_threshold: 500
  cost:
    worker_machine_cost_per_hour: 0.42
  spark_join:
    broadcast_threshold_high_risk_bytes: 536870912
  state:
    large_measured_state_size_gb: 250.0
  ```
  ```bash
  pybeamguard analyze pipeline.py --rules rules.yaml
  ```

- **Use a data profile**:
  ```bash
  pybeamguard analyze pipeline.py --data-profile profile.json --format json
  ```

- **Scan a directory of pipelines in CI** (single-file tool, so loop over files):
  ```bash
  for f in pipelines/*.py; do
    pybeamguard analyze "$f" --fail-on critical || exit 1
  done
  ```

For all subcommands and flags, run `pybeamguard --help`. The same `analyze`
surface is available from the Python bindings (`analyze`, `analyze_spark`,
`get_json_report`, `get_text_report`, `analyze_structured`, ...), which accept an
optional `rules_yaml` string.

## Documentation

- [docs/internal/ROADMAP_HONEST.md](docs/internal/ROADMAP_HONEST.md) — verified status: what's tested, what
  isn't built, known CI gaps, and technical debt with file:line specifics.
- [CONTRIBUTING.md](CONTRIBUTING.md) — project layout, local setup, and the exact
  commands CI runs.
- [SECURITY.md](SECURITY.md) — how to report a vulnerability.
- [CHANGELOG.md](CHANGELOG.md) — notable changes (Keep a Changelog format).
- [docs/archive/BUILD_SUMMARY.md](docs/archive/BUILD_SUMMARY.md) — historical
  phase implementation notes.

## Known issues and deferred work

**Blocked / failed:**
- Native Rust AST rewrite; non-mac wheels.

**Rejected / deferred:**
- Kafka Streams/Ray support.

Full shipped/blocked/rejected detail: [`docs/internal/QUEUE_STATUS.md`](docs/internal/QUEUE_STATUS.md).

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for layout,
setup, and the exact CI commands. Quick version:

```bash
cargo build --release --bin pybeamguard      # standalone CLI binary
cargo test -p pybeamguard-core               # Rust unit + integration tests
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings

pip install maturin pytest
maturin develop
pytest tests/                                # Python binding/CLI tests
```

Note: scope plain `cargo build`/`cargo test` to `-p pybeamguard-core`. The
`bindings/python` crate declares `pyo3`'s `extension-module` feature
unconditionally and only links correctly under `maturin`.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
