pub mod analyzer;
pub mod analyzers;
pub mod frameworks;
pub mod ir;
pub mod parser;
pub mod reporting;

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

pub fn analyze_pipeline(
    python_code: &str,
    data_profile: Option<DataProfile>,
) -> anyhow::Result<Vec<AnalysisResult>> {
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(python_code)?;

    let context = AnalysisContext {
        pipeline_ir: ir,
        data_profile,
    };

    let analyzers = analyzers::registry::create_analyzers();
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
    let parser = SparkFrameworkParser::new();
    let ir = parser.parse(code)?;

    let context = AnalysisContext {
        pipeline_ir: ir,
        data_profile,
    };

    let analyzers = analyzers::registry::create_spark_analyzers();
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
