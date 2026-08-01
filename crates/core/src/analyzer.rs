use serde::{Deserialize, Serialize};
use crate::ir::PipelineIR;

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

pub trait Analyzer: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn priority(&self) -> u32;
    fn analyze(&self, ir: &PipelineIR) -> anyhow::Result<AnalysisResult>;
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
