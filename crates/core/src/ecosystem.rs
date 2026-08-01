use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Integration with PyAirflowTester (Orchestration + Execution)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AirflowPipelineContext {
    pub dag_id: String,
    pub task_id: String,
    pub operator_type: String,  // BashOperator, DataflowTemplateOperator, etc.
    pub dataflow_job_config: Option<DataflowJobConfig>,
    pub beam_pipeline_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataflowJobConfig {
    pub project_id: String,
    pub region: String,
    pub worker_machine_type: String,
    pub max_workers: u32,
    pub network: Option<String>,
    pub subnetwork: Option<String>,
    pub enable_streaming_engine: bool,
}

// ============================================================================
// dbt Artifact Integration (Transformation Cost Analysis)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtManifest {
    pub nodes: HashMap<String, DbtNode>,
    pub metadata: DbtMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtNode {
    pub name: String,
    pub resource_type: String,  // model, test, macro, etc.
    pub database: String,
    pub schema: String,
    pub materialization: Option<String>,  // table, view, ephemeral, incremental
    pub depends_on: Vec<String>,
    pub tags: Vec<String>,
    pub meta: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtMetadata {
    pub dbt_version: String,
    pub generated_at: String,
    pub invocation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtCostAnalysis {
    pub total_transformation_cost: f64,
    pub cost_per_model: HashMap<String, f64>,
    pub materialization_recommendations: Vec<MaterializationRecommendation>,
    pub incremental_optimization_opportunities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationRecommendation {
    pub model_name: String,
    pub current_materialization: String,
    pub recommended_materialization: String,
    pub estimated_cost_savings: f64,
    pub reasoning: String,
}

// ============================================================================
// Data Contracts & Schema Validation
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    pub id: String,
    pub name: String,
    pub owner: String,
    pub dataset: DatasetDescription,
    pub schema: SchemaDefinition,
    pub sla: Option<ServiceLevelAgreement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetDescription {
    pub name: String,
    pub description: String,
    pub owner_team: String,
    pub freshness_sla: Option<String>,  // "daily", "hourly", "real-time"
    pub availability_sla: Option<f32>,   // 99.9, 99.99, etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Option<Vec<String>>,
    pub partition_by: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub required: bool,
    pub description: Option<String>,
    pub valid_values: Option<Vec<String>>,
    pub null_percentage_warning: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceLevelAgreement {
    pub freshness_sla_minutes: Option<u32>,
    pub availability_target_percent: Option<f32>,
    pub max_acceptable_latency_seconds: Option<u32>,
    pub max_acceptable_error_rate_percent: Option<f32>,
}

// ============================================================================
// Anomaly Detection (ML-Powered)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyDetectionConfig {
    pub enabled: bool,
    pub sensitivity: f32,  // 0.1 (low) to 1.0 (high)
    pub lookback_days: u32,
    pub metrics_to_monitor: Vec<String>,  // "cost", "latency", "error_rate", etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedAnomaly {
    pub id: String,
    pub metric: String,
    pub severity: String,  // "low", "medium", "high"
    pub timestamp: String,
    pub baseline_value: f64,
    pub observed_value: f64,
    pub deviation_percent: f32,
    pub possible_causes: Vec<String>,
}

// ============================================================================
// FinOps Dashboard & Cost Management
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinOpsDashboard {
    pub organization_id: String,
    pub period: String,  // "month", "quarter", "year"
    pub total_cost: f64,
    pub cost_by_team: HashMap<String, f64>,
    pub cost_by_pipeline: HashMap<String, f64>,
    pub cost_by_framework: HashMap<String, f64>,
    pub cost_trends: Vec<CostTrend>,
    pub optimization_opportunities: Vec<OptimizationOpportunity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostTrend {
    pub date: String,
    pub cost: f64,
    pub trend_direction: String,  // "up", "down", "flat"
    pub month_over_month_percent_change: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationOpportunity {
    pub pipeline_id: String,
    pub opportunity_type: String,  // "hot_key_sharding", "reduce_shuffle", etc.
    pub potential_savings_monthly: f64,
    pub implementation_effort_hours: u32,
    pub roi_multiplier: f32,
}

// ============================================================================
// Community Features
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeamBestPracticeLibrary {
    pub practices: Vec<BeamBestPractice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeamBestPractice {
    pub id: String,
    pub title: String,
    pub description: String,
    pub category: String,  // "performance", "reliability", "cost", "scalability"
    pub code_example: String,
    pub anti_pattern: String,
    pub estimated_impact: String,
    pub difficulty_level: String,  // "easy", "medium", "hard"
    pub tags: Vec<String>,
    pub contributed_by: Option<String>,
    pub community_votes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertifiedBeamEngineer {
    pub user_id: String,
    pub name: String,
    pub certification_date: String,
    pub expertise_areas: Vec<String>,  // "streaming", "batch", "cost_optimization", etc.
    pub pipelines_reviewed: u32,
    pub community_contributions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityForum {
    pub posts: Vec<CommunityPost>,
    pub discussions: Vec<CommunityDiscussion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityPost {
    pub id: String,
    pub title: String,
    pub author: String,
    pub content: String,
    pub tags: Vec<String>,
    pub created_at: String,
    pub upvotes: u32,
    pub solved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityDiscussion {
    pub id: String,
    pub topic: String,
    pub initiated_by: String,
    pub participants: Vec<String>,
    pub messages: Vec<String>,
    pub resolution: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_contract_creation() {
        let contract = DataContract {
            id: "contract_1".to_string(),
            name: "Customer Events".to_string(),
            owner: "data_team".to_string(),
            dataset: DatasetDescription {
                name: "events".to_string(),
                description: "Customer activity events".to_string(),
                owner_team: "data".to_string(),
                freshness_sla: Some("hourly".to_string()),
                availability_sla: Some(99.9),
            },
            schema: SchemaDefinition {
                columns: vec![
                    ColumnDefinition {
                        name: "customer_id".to_string(),
                        data_type: "string".to_string(),
                        required: true,
                        description: Some("Unique customer ID".to_string()),
                        valid_values: None,
                        null_percentage_warning: None,
                    },
                ],
                primary_key: Some(vec!["customer_id".to_string()]),
                partition_by: Some(vec!["date".to_string()]),
            },
            sla: Some(ServiceLevelAgreement {
                freshness_sla_minutes: Some(60),
                availability_target_percent: Some(99.9),
                max_acceptable_latency_seconds: Some(300),
                max_acceptable_error_rate_percent: Some(0.1),
            }),
        };

        assert_eq!(contract.id, "contract_1");
        assert!(contract.sla.is_some());
    }

    #[test]
    fn test_finops_dashboard() {
        let dashboard = FinOpsDashboard {
            organization_id: "org_1".to_string(),
            period: "month".to_string(),
            total_cost: 50000.0,
            cost_by_team: {
                let mut map = HashMap::new();
                map.insert("data_eng".to_string(), 25000.0);
                map.insert("analytics".to_string(), 25000.0);
                map
            },
            cost_by_pipeline: HashMap::new(),
            cost_by_framework: {
                let mut map = HashMap::new();
                map.insert("Beam".to_string(), 30000.0);
                map.insert("Spark".to_string(), 20000.0);
                map
            },
            cost_trends: vec![],
            optimization_opportunities: vec![],
        };

        assert_eq!(dashboard.total_cost, 50000.0);
        assert_eq!(dashboard.cost_by_framework.len(), 2);
    }
}
