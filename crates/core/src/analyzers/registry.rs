use super::{
    best_practices, cost, deployment, flink_checkpoint, flink_state, flink_watermark, graph,
    hotkey, reliability, shuffle, spark_join, spark_shuffle, spark_streaming, state, synthesis,
    windowing,
};
use crate::analyzer::Analyzer;
use crate::rules::RulesConfig;

/// `HotKeyAnalyzer`/`StateAnalyzer`/`CostAnalyzer` are constructed with
/// `rules`'s corresponding section instead of their old hardcoded `const`
/// data -- see `crate::rules` for what's overridable and why.
pub fn create_analyzers(rules: &RulesConfig) -> Vec<Box<dyn Analyzer>> {
    vec![
        // Phase 1: MVP
        Box::new(graph::GraphAnalyzer),
        Box::new(hotkey::HotKeyAnalyzer::new(rules.hotkey.clone())),
        Box::new(shuffle::ShuffleAnalyzer),
        // Phase 2: Streaming
        Box::new(windowing::WindowingAnalyzer),
        Box::new(state::StateAnalyzer::new(rules.state)),
        // Phase 3: Cost & Reliability
        Box::new(cost::CostAnalyzer::new(rules.cost)),
        Box::new(reliability::ReliabilityAnalyzer),
        // Phase 4: Advanced
        Box::new(best_practices::BestPracticesAnalyzer),
        Box::new(deployment::DeploymentAnalyzer),
        // Synthesis (must be last)
        Box::new(synthesis::SynthesisEngine),
    ]
}

pub fn create_analyzers_by_names(names: &[&str], rules: &RulesConfig) -> Vec<Box<dyn Analyzer>> {
    let all = create_analyzers(rules);
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
pub fn create_spark_analyzers(rules: &RulesConfig) -> Vec<Box<dyn Analyzer>> {
    vec![
        Box::new(spark_shuffle::SparkShuffleAnalyzer),
        Box::new(spark_join::SparkJoinAnalyzer::new(rules.spark_join.clone())),
        Box::new(spark_streaming::SparkStreamingAnalyzer),
    ]
}
