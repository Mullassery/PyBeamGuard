use crate::ir::PipelineIR;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RiskSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

impl RiskSeverity {
    pub fn score(&self) -> u32 {
        match self {
            RiskSeverity::Info => 1,
            RiskSeverity::Low => 2,
            RiskSeverity::Medium => 3,
            RiskSeverity::High => 4,
            RiskSeverity::Critical => 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FindingType {
    PerformanceRisk,
    ReliabilityRisk,
    CostRisk,
    AntiPattern,
    ConfigurationIssue,
    SecurityRisk,
    ScalabilityRisk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Impact {
    pub latency_multiplier: Option<f64>,
    pub cost_delta_monthly: Option<f64>,
    pub affected_records_percent: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub id: String,
    pub severity: RiskSeverity,
    pub finding_type: FindingType,
    pub title: String,
    pub description: String,
    pub affected_nodes: Vec<String>,
    pub recommendation: Option<String>,
    pub estimated_impact: Option<Impact>,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub analyzer_name: String,
    pub version: String,
    pub findings: Vec<Finding>,
    pub metrics: std::collections::HashMap<String, f64>,
    pub summary: String,
    pub confidence: f32,
}

/// Optional, caller-supplied statistics about the runtime characteristics of
/// a pipeline (throughput, element size, key cardinality, state size).
/// Analyzers may use this to replace their flat, heuristic assumptions with
/// figures closer to the pipeline's real behavior. All fields are optional:
/// an absent field means "fall back to the built-in heuristic".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataProfile {
    pub estimated_throughput_per_sec: Option<f64>,
    pub average_element_size_bytes: Option<usize>,
    pub key_cardinality: Option<usize>,
    pub estimated_state_size_gb: Option<f64>,
}

/// Everything an [`Analyzer`] needs to inspect a pipeline: the parsed IR,
/// plus an optional [`DataProfile`] with real-world runtime statistics.
#[derive(Debug, Clone)]
pub struct AnalysisContext {
    pub pipeline_ir: PipelineIR,
    pub data_profile: Option<DataProfile>,
}

pub trait Analyzer: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn priority(&self) -> u32;
    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult>;
}

impl AnalysisResult {
    pub fn critical_findings(&self) -> Vec<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.severity == RiskSeverity::Critical)
            .collect()
    }

    pub fn high_findings(&self) -> Vec<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.severity == RiskSeverity::High)
            .collect()
    }

    pub fn total_score(&self) -> u32 {
        self.findings.iter().map(|f| f.severity.score()).sum()
    }
}
