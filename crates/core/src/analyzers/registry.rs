use super::{
    best_practices, cost, deployment, flink_checkpoint, flink_state, flink_watermark, graph,
    hotkey, reliability, shuffle, spark_join, spark_shuffle, spark_streaming, state, synthesis,
    windowing,
};
use crate::analyzer::Analyzer;

pub fn create_analyzers() -> Vec<Box<dyn Analyzer>> {
    vec![
        // Phase 1: MVP
        Box::new(graph::GraphAnalyzer),
        Box::new(hotkey::HotKeyAnalyzer),
        Box::new(shuffle::ShuffleAnalyzer),
        // Phase 2: Streaming
        Box::new(windowing::WindowingAnalyzer),
        Box::new(state::StateAnalyzer),
        // Phase 3: Cost & Reliability
        Box::new(cost::CostAnalyzer),
        Box::new(reliability::ReliabilityAnalyzer),
        // Phase 4: Advanced
        Box::new(best_practices::BestPracticesAnalyzer),
        Box::new(deployment::DeploymentAnalyzer),
        // Synthesis (must be last)
        Box::new(synthesis::SynthesisEngine),
    ]
}

pub fn create_analyzers_by_names(names: &[&str]) -> Vec<Box<dyn Analyzer>> {
    let all = create_analyzers();
    all.into_iter()
        .filter(|a| names.contains(&a.name()))
        .collect()
}

/// Analyzers for Apache Flink pipelines, run against the `PipelineIR`
/// produced by `FlinkFrameworkParser`. First-class, checkpoint/state/
/// watermark-aware analyzers -- not the pattern-occurrence counting this
/// framework used to be limited to.
pub fn create_flink_analyzers() -> Vec<Box<dyn Analyzer>> {
    vec![
        Box::new(flink_checkpoint::FlinkCheckpointAnalyzer),
        Box::new(flink_state::FlinkStateAnalyzer),
        Box::new(flink_watermark::FlinkWatermarkAnalyzer),
    ]
}

/// Analyzers for Apache Spark (Structured Streaming / DataFrame) pipelines,
/// run against the `PipelineIR` produced by `SparkFrameworkParser`.
pub fn create_spark_analyzers() -> Vec<Box<dyn Analyzer>> {
    vec![
        Box::new(spark_shuffle::SparkShuffleAnalyzer),
        Box::new(spark_join::SparkJoinAnalyzer),
        Box::new(spark_streaming::SparkStreamingAnalyzer),
    ]
}
