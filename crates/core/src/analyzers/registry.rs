use super::{
    best_practices, cost, deployment, graph, hotkey, reliability, shuffle, state, synthesis,
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
