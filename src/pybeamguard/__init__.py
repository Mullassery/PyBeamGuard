"""PyBeamGuard: Apache Beam & Dataflow Pipeline Analysis Platform

A free, proprietary software tool for analyzing Apache Beam pipelines and
Google Cloud Dataflow deployments to identify performance bottlenecks,
reliability risks, and cost optimization opportunities.

Features:
- 10 intelligent analyzers (Graph, HotKey, Shuffle, Windowing, State, Cost,
  Reliability, BestPractices, Deployment, Architecture Review)
- Pre-deployment cost forecasting
- Multi-framework support (Beam, Flink, Spark)
- CI/CD integration (GitHub, GitLab, Jenkins)
- Org governance (cost budgets, SLOs, audit logs)
- Completely free, no licensing tiers

Example:
    $ pybeamguard analyze pipeline.py
    $ pybeamguard analyze pipeline.py --format json
    $ pybeamguard analyze pipeline.py --data-profile profile.json

Visit: https://github.com/Mullassery/pybeamguard
"""

__version__ = "0.4.0"
__author__ = "Georgi Mammen Mullassery"
__email__ = "mullassery@gmail.com"
__license__ = "Proprietary"

try:
    from . import core
except ImportError:
    # PyO3 bindings not available (development mode)
    core = None

__all__ = [
    "__version__",
    "__author__",
    "core",
]
