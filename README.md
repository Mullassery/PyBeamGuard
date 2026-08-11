# PyBeamGuard

**Catch Apache Beam failures before deployment. Forecast costs. Fix hot keys.**

Analyze Beam pipelines pre-deployment to identify bottlenecks, reliability risks, and cost drivers. FREE. No GCP account required. Works offline.

> A static analysis platform for Apache Beam pipelines, with basic pattern
> detection for Flink and Spark source files.

[![CI](https://github.com/Mullassery/PyBeamGuard/actions/workflows/ci.yml/badge.svg)](https://github.com/Mullassery/PyBeamGuard/actions/workflows/ci.yml)
[![Version](https://img.shields.io/badge/version-1.1.1-blue)](https://github.com/Mullassery/PyBeamGuard/releases)
[![License](https://img.shields.io/badge/license-Proprietary-blue)](LICENSE)
[![PyPI](https://img.shields.io/badge/PyPI-pybeamguard-blue)](https://pypi.org/project/pybeamguard/)

---

## Comparison with Similar Tools

### Why PyBeamGuard?

| Feature | PyBeamGuard | Cloud Profiler | Dataflow UI |
|---------|-------------|---|---|
| **Pre-deployment analysis** | ✅ | ❌ | ❌ |
| **Cost forecasting** | ⚠️ Heuristic estimate (e.g. $48-$2.5K/mo range) | ❌ | ⚠️ Post-deploy |
| **Hot key detection** | ✅ | ❌ | ❌ |
| **Shuffle analysis** | ✅ | ❌ | ⚠️ Post-deploy |
| **Windowing validation** | ✅ | ❌ | ❌ |
| **Cost** | 🎉 FREE | Included in GCP | Included in GCP |
| **Setup required** | None | GCP account | GCP account |
| **Offline capable** | ✅ | ❌ | ❌ |

**Bottom line:** Pre-deployment analysis you control, costs you forecast before running, no vendor lock-in.

---

## Quick Start

### Installation

**Requires Python 3.10 or later**

```bash
# Using pip
pip install pybeamguard

# Using uv (faster)
uv pip install pybeamguard

# Verify installation
pybeamguard --version
```

### Analyze a Pipeline

```bash
# Text output (default)
pybeamguard analyze pipeline.py

# JSON output  
pybeamguard analyze pipeline.py --format json

# With data profile
pybeamguard analyze pipeline.py --data-profile profile.json
```

### Example Output

```
=== PyBeamGuard Analysis Report ===

Overall Risk Score: 78/100
Total Findings: 5

🔴 CRITICAL ISSUES
• Hot key probability detected on customer_id aggregation

🟠 HIGH PRIORITY ISSUES
• Large shuffle stage in join operation
• Missing dead-letter queue on parse failures

🟡 MEDIUM PRIORITY ISSUES
• Unbounded state growth risk

Estimated Cost: $2,300/month → Optimized: $1,350/month (41% savings)
```

---

## Features

### All Features FREE - Proprietary Software

### 10 Intelligent Analyzers

| Analyzer | Purpose | Version |
|----------|---------|---------|
| **Graph Intelligence** | Extract pipeline topology, detect cycles | ✅ v0.1 |
| **Hot Key Detection** | Identify key skew & worker imbalance | ✅ v0.1 |
| **Shuffle Analysis** | Quantify expensive shuffle operations | ✅ v0.1 |
| **Windowing Validation** | Ensure streaming correctness | ✅ v0.1 |
| **State Auditor** | Prevent state-related failures | ✅ v0.1 |
| **Cost Intelligence** | Forecast Dataflow spend | ✅ v0.1 |
| **Reliability Analysis** | Detect operational weaknesses | ✅ v0.1 |
| **Best Practices Engine** | Rule-based Beam optimization checks | ✅ v0.1 |
| **Deployment Auditor** | Worker sizing & config validation | ✅ v0.1 |
| **Architecture Review** | Executive summary & synthesis | ✅ v0.1 |

### Framework Support (Free)

- ✅ Apache Beam — full pipeline graph extraction + all 10 analyzers
- ⚠️ Apache Flink — **pattern detection only**: regex-based occurrence
  counting of stateful operators (`KeyedProcessFunction`, `State`, `Timer`)
  and windowing calls, surfaced as metadata. This is not checkpoint
  strategy, state backend, or watermark analysis — that would require a
  real Flink IR/analyzer pass, which does not exist yet.
- ⚠️ Apache Spark — **pattern detection only**: regex-based occurrence
  counting of `readStream`/`groupBy`/`window`/`trigger`/`outputMode` calls,
  surfaced as metadata. This is not shuffle-partitioning, state-store, or
  join-strategy analysis.
- 🔜 Kafka Streams (not started)
- 🔜 Ray Data (not started)

---

## Use Cases

### 👨‍💻 For Data Engineers

Pre-deployment validation: "Will this scale? What will it cost?"

```bash
pybeamguard analyze my_pipeline.py
# ✓ Identifies 3 hot key risks
# ✓ Estimates $850/month cost
# ✓ Warns of unbounded state growth
```

### 🏢 For Platform Teams

CI/CD enforcement: fail the build if any finding is critical. `analyze`
takes a single pipeline file (there's no built-in directory/glob support
yet), so scanning a directory of pipelines means looping over the files:

```yaml
# .github/workflows/pipeline-validation.yml
- run: |
    for f in pipelines/*.py; do
      pybeamguard analyze "$f" --fail-on critical || exit 1
    done
```

### 💰 For FinOps Teams

Cost attribution: "Why is this pipeline $2,500/month?"

```bash
pybeamguard analyze pipeline.py --format json | jq '.[] | select(.analyzer_name=="CostAnalyzer")'
# "estimated_total_cost_per_month": 2500.00
# "estimated_shuffle_cost_per_month": 1500.00  ← Cost hotspot
```

Note: these dollar figures are a rough, heuristic pre-deployment estimate,
not a validated billing forecast — see the Cost Forecasting caveat below.

---

## Installation

### From PyPI (Recommended)

Python 3.10+ with pip or uv:

```bash
# Using pip
pip install pybeamguard

# Using uv
uv pip install pybeamguard

# Verify installation
pybeamguard --version
```

### From GitHub Releases (standalone Rust binary, no Python required)

GitHub Releases publishes the standalone `pybeamguard` Rust CLI binary for
macOS, Linux, and Windows — this is a *separate* build from the PyPI wheel
above and has no Python dependency at all:

```bash
# Download the binary for your platform from:
# https://github.com/Mullassery/PyBeamGuard/releases
chmod +x pybeamguard-macos-universal   # or the linux/windows artifact
./pybeamguard-macos-universal --version
```

### From Source

Requires Rust 1.70+ and Python 3.10+:

```bash
git clone https://github.com/Mullassery/PyBeamGuard.git
cd PyBeamGuard

# Python package (PyO3 bindings), installed into your active venv:
pip install maturin
maturin develop
pybeamguard --version

# OR the standalone Rust CLI binary (no Python involved):
cargo build --release --bin pybeamguard
./target/release/pybeamguard --version
```

---

## Documentation

- **[Build Summary](BUILD_SUMMARY.md)** - Phase implementation details and history

---

## Examples

### Example 1: Simple Batch Pipeline

```python
# pipeline.py
import apache_beam as beam

with beam.Pipeline() as p:
    result = (
        p
        | 'Read' >> beam.io.ReadFromText('input.txt')
        | 'Parse' >> beam.ParDo(ParseFn())
        | 'GroupByCustomer' >> beam.GroupByKey()
        | 'CountPerCustomer' >> beam.CombinePerKey(sum)
        | 'Write' >> beam.io.WriteToText('output.txt')
    )
```

```bash
$ pybeamguard analyze pipeline.py

🟠 HIGH PRIORITY
• High hot-key probability on customer_id
  Impact: 3-5x latency increase
  Mitigation: Apply key sharding strategy

📊 Cost Estimate
  Compute: $18/month
  Shuffle: $30/month
  Total: $48/month

✅ Recommendation: Implement key sharding before production
```

### Example 2: With Data Profile

```json
// profile.json
{
  "estimated_throughput_per_sec": 10000,
  "average_element_size_bytes": 500,
  "key_cardinality": 50000,
  "estimated_state_size_gb": 5.0
}
```

```bash
$ pybeamguard analyze pipeline.py --data-profile profile.json --format json

{
  "analyzer_name": "CostAnalyzer",
  "findings": [...],
  "metrics": {
    "estimated_total_cost_per_month": 2350.00,
    "estimated_compute_cost_per_month": 175.00,
    "estimated_shuffle_cost_per_month": 900.00,
    "estimated_state_cost_per_month": 1275.00
  }
}
```

---

## Performance

| Metric | Value |
|--------|-------|
| **Analysis Time** | <500ms (100+ node pipeline) |
| **Memory Usage** | <50MB |
| **Binary Size** | 15MB (release) |
| **Tests** | 29 Rust unit tests + 7 integration tests + Python binding/CLI tests (see CI badge) |

---

## Requirements

- **macOS** 10.13+ (Intel/Apple Silicon)
- **Linux** (glibc 2.31+, x86_64)
- **Windows** 10/11 (x86_64)

The standalone Rust binary (GitHub Releases) has no Python runtime,
dependencies, or environment variables required. The PyPI package
(`pip install pybeamguard`) requires Python 3.10+, same as any Python
package — it's a compiled PyO3 extension module with a thin Python shim,
not a pure-Python implementation.

---

## Contributing

Contributions welcome!

```bash
# Build the Rust workspace
cargo build --workspace --release

# Run the Rust test suite (unit + integration)
cargo test --workspace

# Lint
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings

# Build + install the Python bindings locally, then run the Python test suite
pip install maturin pytest
maturin develop
pytest tests/

# Analyze an example pipeline
./target/release/pybeamguard analyze examples/pipeline_simple.py
```

---

## Release Status

**Current state: working proof-of-concept for Apache Beam analysis**, with
packaging and CI around it. Concretely, what's implemented and tested today:
- ✅ 10 intelligent analyzers over Apache Beam pipelines (regex/heuristic-based, not full AST analysis)
- ✅ Python bindings via PyO3 abi3, real `pip install`-able package
- ✅ `--fail-on <severity>` CI gating and `--data-profile`-informed cost/hot-key estimates
- ⚠️ Flink/Spark: pattern detection only (see Framework Support above) — not full analysis
- ✅ Rust unit + integration tests, Python binding/CLI tests, all run in CI (see badge above)
- ✅ <500ms analysis per pipeline (small/medium pipelines; not independently benchmarked at scale)

**Explicitly not implemented** (removed from this codebase to stop
overclaiming rather than left as unused/untested scaffolding): organization
governance (cost budgets, SLOs, policy enforcement), audit logging, and
Airflow/dbt/data-contract/FinOps ecosystem integrations. These were
previously present as struct definitions with no wiring into the actual
analysis path and no way to test them without external systems this project
doesn't have access to.

**Future Roadmap (aspirational, not started):**
- Real Flink/Spark analyzers (checkpoint/state/shuffle-partitioning), not just pattern detection
- Kafka Streams, Ray Data framework support
- Directory/glob input to `analyze` (currently single-file only)
- Re-introduce org governance / audit logging as real, tested features if there's demand

---

## License

**Proprietary Software** — FREE forever, no licensing tiers, no paywalls.

See [LICENSE](LICENSE) file for complete terms. All features available to all users.

**Use Cases:**
- ✅ Commercial use
- ✅ Internal tools
- ✅ Research
- ✅ Education
- ✅ Open source projects

---

## Support & Contact

- **GitHub Issues**: https://github.com/Mullassery/PyBeamGuard/issues
- **Repository**: https://github.com/Mullassery/PyBeamGuard
- **PyPI**: https://pypi.org/project/pybeamguard/
- **Email**: mullassery@gmail.com
- **Author**: [@Mullassery](https://github.com/Mullassery)

---

## FAQ

**Q: How much does PyBeamGuard cost?**  
A: **FREE.** PyBeamGuard is proprietary software with no licensing fees, no tiers, no paywalls. All features available to everyone.

**Q: Does PyBeamGuard require Python?**  
A: Depends how you install it. The standalone Rust binary from GitHub
Releases has zero dependencies — download and run. The `pip install
pybeamguard` package is a compiled PyO3 extension with a thin Python shim,
so it requires Python 3.10+ like any Python package.

**Q: What pipeline sizes can it analyze?**  
A: The parser and analyzers are simple regex/line-based passes over the
source, so there's no architectural node-count ceiling, but this hasn't
been independently benchmarked at large scale (e.g. 1,000+ node pipelines).

**Q: How accurate are the cost estimates?**  
A: Treat them as an order-of-magnitude planning signal, not a validated
billing forecast — the underlying cost model uses simplified, only
partially-verified pricing assumptions (see `CostAnalyzer`'s doc comments
in `crates/core/src/analyzers/cost.rs`). Supplying `--data-profile` replaces
some flat per-operation guesses with your real throughput/state figures,
which narrows the estimate, but doesn't make it a guarantee. Always confirm
with the GCP Pricing Calculator or a real test run before committing to a
budget.

**Q: Can I use this in CI/CD?**  
A: Yes — `pybeamguard analyze pipeline.py --fail-on critical` exits
non-zero if any finding meets or exceeds the given severity, so it works in
any CI system with a shell (GitHub Actions, GitLab CI, Jenkins, Cloud
Build, ...). There's no bundled CI-specific plugin/action, just a
CLI with a meaningful exit code.

**Q: What about Spark, Flink, Kafka Streams?**  
A: Spark and Flink source files get basic pattern detection today (regex
occurrence counting of key operators, not real analysis — see Framework
Support above). Kafka Streams and Ray Data support hasn't been started.

---

## Detailed Comparison Matrix

### Analysis Capabilities

| Capability | PyBeamGuard | Beam Native Tools | GCP Dataflow | Monitoring Tools |
|---|---|---|---|---|
| Pipeline graph extraction | ✅ | ❌ | ❌ | ❌ |
| Complexity scoring | ✅ | ❌ | ❌ | ❌ |
| Hot key detection | ✅ Heuristic (keyword pattern + optional measured cardinality) | ❌ (disabled 2022) | ⚠️ Disabled for streaming | ❌ |
| Shuffle quantification | ✅ Per-stage (rule-based) | ❌ | ⚠️ Aggregate only | ⚠️ Post-deploy only |
| State growth prediction | ✅ Heuristic (flags stateful ops) | ❌ | ❌ | ❌ |
| Cost forecasting | ⚠️ Pre-deploy, rough heuristic | ❌ | ⚠️ Post-deploy estimate | ❌ |
| Best practices engine | ✅ Rule-based checks | ❌ | ❌ | ❌ |
| Deployment audit | ✅ | ❌ | ❌ | ❌ |
| Architecture review | ✅ Rule-based weighted scoring (not ML) | ❌ | ❌ | ⚠️ Manual only |

### Deployment & Integration

| Aspect | PyBeamGuard | Cloud Profiler | Dataflow UI |
|---|---|---|---|
| Installation | pip install / wheel | Built-in (GCP) | Built-in (GCP) |
| Setup time | <1 minute | Account required | Account required |
| Offline support | ✅ Full | ❌ No | ❌ No |
| CI/CD gating | ✅ `--fail-on <severity>` exit code (works with any CI system) | ❌ No | ❌ No |
| Python version | 3.10+ (via PyO3) | Any (GCP) | Any (GCP) |
| Platform support | macOS, Linux, Windows | GCP only | GCP only |

### Cost

| Feature | PyBeamGuard | Competitors |
|---|---|---|
| **Tool cost** | 🎉 FREE | Dataflow UI: Free (but runs expensive test jobs) |
| **Cost forecasting** | ⚠️ Rough, pre-deploy heuristic (see caveat below) | ❌ Requires running pipelines |
| **Test job cost** | ✅ Save money by not needing a real run for a first pass | ❌ Must run to estimate cost |

### Time to Insight

| Task | PyBeamGuard | Cloud Profiler | Dataflow UI |
|---|---|---|---|
| Analyze pipeline | <1 sec | N/A (need to run) | N/A (need to run) |
| Detect hot keys | <1 sec | 30+ min (with run) | 30+ min (with run) |
| Forecast cost | <1 sec | N/A | 24-48 hours (post-deploy) |
| Architecture review | <2 sec | N/A | N/A |

---

## Why PyBeamGuard Exists

PyBeamGuard fills a critical gap:

**The Problem**: Google disabled hot key detection for streaming Dataflow pipelines in March 2022. No other tool provides pre-deployment Beam analysis. Teams are left with:
1. Manual review (slow, inconsistent)
2. Running expensive test jobs (costly, time-consuming)
3. Production incidents (expensive, damaging)

**The Solution**: PyBeamGuard brings expert-level Beam analysis to every team, offline and for free.

---

**Made with ❤️ for data engineers everywhere.**
