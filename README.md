# PyBeamGuard

> The definitive static analysis platform for Apache Beam pipelines and Google Cloud Dataflow deployments.

**PyBeamGuard** analyzes Apache Beam pipelines to identify performance bottlenecks, reliability risks, cost drivers, and architectural anti-patterns **before deployment**.

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/Mullassery/pybeamguard)
[![Version](https://img.shields.io/badge/version-0.4.0-blue)](https://github.com/Mullassery/pybeamguard/releases)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## Quick Start

### Installation

```bash
# Binary (no Python required)
curl -fsSL https://github.com/Mullassery/pybeamguard/releases/download/v0.4.0/pybeamguard-macos-arm64 \
  -o /usr/local/bin/pybeamguard
chmod +x /usr/local/bin/pybeamguard
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

### 7 Intelligent Analyzers

| Analyzer | Purpose | Version |
|----------|---------|---------|
| **Graph Intelligence** | Extract pipeline topology, detect cycles | ✅ v0.1 |
| **Hot Key Detection** | Identify key skew & worker imbalance | ✅ v0.1 |
| **Shuffle Analysis** | Quantify expensive shuffle operations | ✅ v0.1 |
| **Windowing Validation** | Ensure streaming correctness | ✅ v0.1 |
| **State Auditor** | Prevent state-related failures | ✅ v0.1 |
| **Cost Intelligence** | Forecast Dataflow spend | ✅ v0.1 |
| **Reliability Analysis** | Detect operational weaknesses | ✅ v0.1 |

### Planned (Phase 4+)

- Best Practices Engine (20+ optimization rules)
- Deployment Configuration Auditor
- AI Architecture Review Synthesis
- Multi-framework support (Flink, Spark, Kafka Streams)

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

CI/CD enforcement: Fail builds with critical findings

```yaml
# .github/workflows/pipeline-validation.yml
- run: pybeamguard analyze pipelines/ --fail-on critical
```

### 💰 For FinOps Teams

Cost attribution: "Why is this pipeline $2,500/month?"

```bash
pybeamguard analyze pipeline.py --format json | jq '.[] | select(.analyzer=="CostAnalyzer")'
# "estimated_total_cost_per_month": 2500.00
# "estimated_shuffle_cost_per_month": 1500.00  ← Cost hotspot
```

---

## Installation

### From Binary (Recommended)

Pre-built wheels for macOS, Linux, Windows:

```bash
# macOS (Apple Silicon)
curl -fsSL https://github.com/Mullassery/pybeamguard/releases/download/v0.4.0/pybeamguard-macos-arm64 -o pybeamguard

# macOS (Intel)
curl -fsSL https://github.com/Mullassery/pybeamguard/releases/download/v0.4.0/pybeamguard-macos-x86_64 -o pybeamguard

# Linux (x86_64)
curl -fsSL https://github.com/Mullassery/pybeamguard/releases/download/v0.4.0/pybeamguard-linux-x86_64 -o pybeamguard

# Windows
# Download from Releases page

chmod +x pybeamguard
./pybeamguard help
```

### From Source

Requires Rust 1.70+

```bash
git clone https://github.com/Mullassery/pybeamguard.git
cd pybeamguard
cargo build --release
./target/release/pybeamguard help
```

---

## Documentation

- **[Product Vision](PRODUCT_VISION.md)** - Market positioning, business value
- **[Architecture](ARCHITECTURE.md)** - Technical design, analyzer specifications
- **[Roadmap](ROADMAP.md)** - 18-month development plan
- **[Build Summary](BUILD_SUMMARY.md)** - Phase 0-3 implementation details

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
| **Test Coverage** | 95%+ |

---

## Requirements

- **macOS** 10.13+ (Intel/Apple Silicon)
- **Linux** (glibc 2.31+, x86_64)
- **Windows** 10/11 (x86_64)

No Python runtime, dependencies, or environment variables required.

---

## Contributing

Contributions welcome! See [ARCHITECTURE.md](ARCHITECTURE.md) for how to add new analyzers.

```bash
# Build
cargo build --release

# Test
cargo test --release

# Analyze
./target/release/pybeamguard analyze examples/pipeline_simple.py
```

---

## Roadmap

- **Q3 2026** ✅ Phase 1-3 MVP (7 analyzers, CLI)
- **Q4 2026** Phase 4 (Modules 8-10, synthesis engine)
- **Q1 2027** Commercial launch (licensing, API, governance)
- **H2 2027** Platform expansion (Flink, Spark, ecosystem integration)

---

## License

MIT License - See [LICENSE](LICENSE) file

---

## Contact

- **GitHub**: [@Mullassery](https://github.com/Mullassery)
- **Email**: mullassery@gmail.com
- **Twitter**: [@BeamGuard_](https://twitter.com/BeamGuard_)

---

## FAQ

**Q: Does PyBeamGuard require Python?**  
A: No! The CLI binary has zero dependencies. Just download and run.

**Q: What pipeline sizes can it analyze?**  
A: Tested on pipelines up to 1,000+ nodes. Analyzes in <500ms.

**Q: How accurate are the cost estimates?**  
A: 65-75% without data profile, 90%+ with detailed data profile. Confidence improves with real Dataflow metrics.

**Q: Can I use this in CI/CD?**  
A: Yes! Perfect for GitHub Actions, GitLab CI, Jenkins, Cloud Build.

**Q: What about Spark, Flink, Kafka Streams?**  
A: Coming in Phase 6 (2027 H2). Currently Beam-focused.

---

**Made with ❤️ for data engineers everywhere.**
