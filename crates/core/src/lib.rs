pub mod analyzer;
pub mod analyzers;
pub mod frameworks;
pub mod ir;
pub mod parser;
pub mod reporting;
pub mod rules;

pub use analyzer::{
    AnalysisContext, AnalysisResult, Analyzer, DataProfile, Finding, FindingType, Impact,
    RiskSeverity,
};
pub use frameworks::{flink::FlinkFrameworkParser, spark::SparkFrameworkParser, FrameworkParser};
pub use ir::{
    DeploymentConfig, Edge, PipelineIR, PipelineMetadata, Runner, TransformNode, TransformType,
};
pub use parser::BeamPipelineParser;
pub use reporting::{JsonReporter, Reporter, TextReporter};
pub use rules::{CostRules, HotKeyRules, RulesConfig, SparkJoinRules, StateRules};

pub fn analyze_pipeline(
    python_code: &str,
    data_profile: Option<DataProfile>,
) -> anyhow::Result<Vec<AnalysisResult>> {
    analyze_pipeline_with_rules(python_code, data_profile, &RulesConfig::default())
}

/// Same as [`analyze_pipeline`], but with analyzer rule data (risk-pattern
/// keyword lists, cost rates, cardinality/size thresholds) overridden by
/// `rules` instead of each analyzer's hardcoded defaults -- see
/// [`rules::RulesConfig`].
pub fn analyze_pipeline_with_rules(
    python_code: &str,
    data_profile: Option<DataProfile>,
    rules: &RulesConfig,
) -> anyhow::Result<Vec<AnalysisResult>> {
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(python_code)?;

    let context = AnalysisContext {
        pipeline_ir: ir,
        data_profile,
    };

    let analyzers = analyzers::registry::create_analyzers(rules);
    run_analyzers(&context, analyzers)
}

/// Parses PyFlink DataStream/Table API source with `FlinkFrameworkParser`
/// and runs the Flink-specific analyzer suite (checkpointing, state
/// backend, watermark/windowing) against the result.
pub fn analyze_flink_pipeline(
    code: &str,
    data_profile: Option<DataProfile>,
) -> anyhow::Result<Vec<AnalysisResult>> {
    let parser = FlinkFrameworkParser::new();
    let ir = parser.parse(code)?;

    let context = AnalysisContext {
        pipeline_ir: ir,
        data_profile,
    };

    let analyzers = analyzers::registry::create_flink_analyzers();
    run_analyzers(&context, analyzers)
}

/// Parses PySpark (Structured Streaming / DataFrame) source with
/// `SparkFrameworkParser` and runs the Spark-specific analyzer suite
/// (shuffle partitioning, join/broadcast strategy, streaming
/// checkpoint/trigger/output-mode) against the result.
pub fn analyze_spark_pipeline(
    code: &str,
    data_profile: Option<DataProfile>,
) -> anyhow::Result<Vec<AnalysisResult>> {
    analyze_spark_pipeline_with_rules(code, data_profile, &RulesConfig::default())
}

/// Same as [`analyze_spark_pipeline`], but with `SparkJoinAnalyzer`'s rule
/// data overridden by `rules` -- see [`rules::RulesConfig`].
pub fn analyze_spark_pipeline_with_rules(
    code: &str,
    data_profile: Option<DataProfile>,
    rules: &RulesConfig,
) -> anyhow::Result<Vec<AnalysisResult>> {
    let parser = SparkFrameworkParser::new();
    let ir = parser.parse(code)?;

    let context = AnalysisContext {
        pipeline_ir: ir,
        data_profile,
    };

    let analyzers = analyzers::registry::create_spark_analyzers(rules);
    run_analyzers(&context, analyzers)
}

fn run_analyzers(
    context: &AnalysisContext,
    analyzers: Vec<Box<dyn Analyzer>>,
) -> anyhow::Result<Vec<AnalysisResult>> {
    let mut results = Vec::new();

    for analyzer in analyzers {
        match analyzer.analyze(context) {
            Ok(result) => results.push(result),
            Err(e) => {
                log::warn!("Analyzer {} failed: {}", analyzer.name(), e);
            }
        }
    }

    Ok(results)
}
