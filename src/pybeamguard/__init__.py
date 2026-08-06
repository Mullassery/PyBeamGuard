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
- Structured Python bindings for programmatic access
- Completely free, no licensing tiers

Example:
    $ pybeamguard analyze pipeline.py
    $ pybeamguard analyze pipeline.py --format json

    # Python API (v0.5.0+)
    from pybeamguard import core
    results = core.analyze_structured(pipeline_code)
    for result in results:
        print(f"Analyzer: {result.get_analyzer_name()}")
        for finding in result.critical_findings():
            print(f"  {finding.get_title()}")

Visit: https://github.com/Mullassery/pybeamguard
"""

__version__ = "0.5.0"
__author__ = "Georgi Mammen Mullassery"
__email__ = "mullassery@gmail.com"
__license__ = "Proprietary"

try:
    # Import native PyO3 module
    from . import pybeamguard as core
    # Re-export structured classes from core module (with clean names)
    from .pybeamguard import (
        PyRiskSeverity as RiskSeverity,
        PyFinding as Finding,
        PyImpact as Impact,
        PyAnalysisResult as AnalysisResult,
        PyTransformNode as TransformNode,
        PyPipelineIR as PipelineIR,
        analyze,
        analyze_structured,
        parse_pipeline,
    )
except ImportError as e:
    # PyO3 bindings not available (development mode)
    import warnings
    warnings.warn(f"PyBeamGuard native bindings not available: {e}")
    core = None
    RiskSeverity = None
    Finding = None
    Impact = None
    AnalysisResult = None
    TransformNode = None
    PipelineIR = None
    analyze = None
    analyze_structured = None
    parse_pipeline = None

__all__ = [
    "__version__",
    "__author__",
    "core",
    "RiskSeverity",
    "Finding",
    "Impact",
    "AnalysisResult",
    "TransformNode",
    "PipelineIR",
    "analyze",
    "analyze_structured",
    "parse_pipeline",
]
