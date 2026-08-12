"""PyBeamGuard: Apache Beam & Dataflow Pipeline Analysis Platform

A free, proprietary software tool for analyzing Apache Beam pipelines and
Google Cloud Dataflow deployments to identify performance bottlenecks,
reliability risks, and cost optimization opportunities.

Features:
- 10 intelligent analyzers for Apache Beam (Graph, HotKey, Shuffle,
  Windowing, State, Cost, Reliability, BestPractices, Deployment,
  Architecture Review)
- 3 intelligent analyzers for Apache Flink (checkpoint strategy, state
  backend, watermark/windowing) and 3 for Apache Spark (shuffle
  partitioning, join/broadcast strategy, streaming checkpoint/trigger/
  output-mode) -- real static analysis over source, not pattern counting
- Pre-deployment cost forecasting (heuristic estimate; see README)
- CI/CD-friendly: `--fail-on <severity>` gates the process exit code so any
  CI system (GitHub Actions, GitLab CI, Jenkins, ...) can fail a build on it
- Structured Python bindings for programmatic access
- Completely free, no licensing tiers

Example:
    $ pybeamguard analyze pipeline.py
    $ pybeamguard analyze pipeline.py --format json
    $ pybeamguard analyze pipeline.py --fail-on critical
    $ pybeamguard analyze streaming_job.py --framework flink
    $ pybeamguard analyze etl.py --framework spark

    # Python API
    from pybeamguard import core
    results = core.analyze_structured(pipeline_code)
    for result in results:
        print(f"Analyzer: {result.get_analyzer_name()}")
        for finding in result.critical_findings():
            print(f"  {finding.get_title()}")

    # Flink / Spark
    flink_results = core.analyze_flink_structured(flink_pipeline_code)
    spark_results = core.analyze_spark_structured(spark_pipeline_code)

Visit: https://github.com/Mullassery/pybeamguard
"""

try:
    # Single source of truth: the version pip/maturin recorded at install
    # time (from pyproject.toml), so this never drifts from a hardcoded
    # literal the way a previous version of this file did (it was stuck at
    # "1.0.0" while the package had moved on to later releases).
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("pybeamguard")
except Exception:  # pragma: no cover - only hit on a broken/uninstalled package
    __version__ = "0.0.0+unknown"
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
        PyAnalyzerInfo as AnalyzerInfo,
        PyPipelineSummary as PipelineSummary,
        PyPipelineStats as PipelineStats,
        PySeveritySummary as SeveritySummary,
        PyResultComparison as ResultComparison,
        PyAnalyzerRanking as AnalyzerRanking,
        PyFindingRecommendation as FindingRecommendation,
        PyAnalyzerPerformance as AnalyzerPerformance,
        PyPrioritizedFinding as PrioritizedFinding,
        PyResultSnapshot as ResultSnapshot,
        analyze,
        analyze_structured,
        analyze_flink,
        analyze_flink_structured,
        analyze_spark,
        analyze_spark_structured,
        parse_pipeline,
        get_available_analyzers,
        analyze_with_analyzers,
        get_json_report,
        get_text_report,
        get_flink_json_report,
        get_flink_text_report,
        get_spark_json_report,
        get_spark_text_report,
        analyze_and_format,
        get_pipeline_complexity_score,
        get_pipeline_summary,
        filter_findings_by_severity,
        get_node_types_in_pipeline,
        validate_pipeline_syntax,
        get_pipeline_stats,
        get_severity_summary,
        deduplicate_findings,
        compare_results,
        rank_analyzers_by_impact,
        search_findings,
        merge_results,
        calculate_risk_score,
        get_fix_recommendations,
        get_analyzer_performance,
        calculate_quality_score,
        prioritize_findings,
        create_result_snapshot,
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
    AnalyzerInfo = None
    PipelineSummary = None
    PipelineStats = None
    SeveritySummary = None
    ResultComparison = None
    AnalyzerRanking = None
    FindingRecommendation = None
    AnalyzerPerformance = None
    PrioritizedFinding = None
    ResultSnapshot = None
    analyze = None
    analyze_structured = None
    analyze_flink = None
    analyze_flink_structured = None
    analyze_spark = None
    analyze_spark_structured = None
    parse_pipeline = None
    get_available_analyzers = None
    analyze_with_analyzers = None
    get_json_report = None
    get_text_report = None
    get_flink_json_report = None
    get_flink_text_report = None
    get_spark_json_report = None
    get_spark_text_report = None
    analyze_and_format = None
    get_pipeline_complexity_score = None
    get_pipeline_summary = None
    filter_findings_by_severity = None
    get_node_types_in_pipeline = None
    validate_pipeline_syntax = None
    get_pipeline_stats = None
    get_severity_summary = None
    deduplicate_findings = None
    compare_results = None
    rank_analyzers_by_impact = None
    search_findings = None
    merge_results = None
    calculate_risk_score = None
    get_fix_recommendations = None
    get_analyzer_performance = None
    calculate_quality_score = None
    prioritize_findings = None
    create_result_snapshot = None

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
    "AnalyzerInfo",
    "PipelineSummary",
    "PipelineStats",
    "SeveritySummary",
    "ResultComparison",
    "AnalyzerRanking",
    "FindingRecommendation",
    "AnalyzerPerformance",
    "PrioritizedFinding",
    "ResultSnapshot",
    "analyze",
    "analyze_structured",
    "analyze_flink",
    "analyze_flink_structured",
    "analyze_spark",
    "analyze_spark_structured",
    "parse_pipeline",
    "get_available_analyzers",
    "analyze_with_analyzers",
    "get_json_report",
    "get_text_report",
    "get_flink_json_report",
    "get_flink_text_report",
    "get_spark_json_report",
    "get_spark_text_report",
    "analyze_and_format",
    "get_pipeline_complexity_score",
    "get_pipeline_summary",
    "filter_findings_by_severity",
    "get_node_types_in_pipeline",
    "validate_pipeline_syntax",
    "get_pipeline_stats",
    "get_severity_summary",
    "deduplicate_findings",
    "compare_results",
    "rank_analyzers_by_impact",
    "search_findings",
    "merge_results",
    "calculate_risk_score",
    "get_fix_recommendations",
    "get_analyzer_performance",
    "calculate_quality_score",
    "prioritize_findings",
    "create_result_snapshot",
]
