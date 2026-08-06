pub mod ir;
pub mod parser;
pub mod analyzer;
pub mod analyzers;
pub mod reporting;
pub mod commercial;
pub mod frameworks;
pub mod ecosystem;

pub use ir::{PipelineIR, TransformNode, TransformType, Edge, PipelineMetadata, Runner, DeploymentConfig};
pub use analyzer::{Analyzer, AnalysisResult, Finding, RiskSeverity, FindingType, Impact};
pub use parser::BeamPipelineParser;
pub use reporting::{Reporter, JsonReporter, TextReporter};

#[derive(Debug, Clone)]
pub struct AnalysisContext {
    pub pipeline_ir: PipelineIR,
    pub data_profile: Option<DataProfile>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataProfile {
    pub estimated_throughput_per_sec: Option<f64>,
    pub average_element_size_bytes: Option<usize>,
    pub key_cardinality: Option<usize>,
    pub estimated_state_size_gb: Option<f64>,
}

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
        match analyzer.analyze(&context.pipeline_ir) {
            Ok(result) => results.push(result),
            Err(e) => {
                log::warn!("Analyzer {} failed: {}", analyzer.name(), e);
            }
        }
    }

    Ok(results)
}
