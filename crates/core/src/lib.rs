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
    let mut results = Vec::new();

    for analyzer in analyzers {
        match analyzer.analyze(&context) {
            Ok(result) => results.push(result),
            Err(e) => {
                log::warn!("Analyzer {} failed: {}", analyzer.name(), e);
            }
        }
    }

    Ok(results)
}
