use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// PyBeamGuard: FREE Proprietary Software
//
// All features available to all users at no cost.
// Governance and organizational features for team coordination.
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationConfig {
    pub org_id: String,
    pub org_name: String,

    // Governance settings (available to all users)
    pub cost_budget_per_month: Option<f64>,
    pub reliability_slo_score: Option<f32>,  // Min acceptable reliability score (0-100)
    pub performance_slo_score: Option<f32>,  // Min acceptable performance score

    // Policy enforcement
    pub enforce_dlq_requirement: bool,
    pub enforce_error_handling: bool,
    pub enforce_cost_limits: bool,
    pub enforce_reliability_gates: bool,

    // Teams & access control
    pub teams: Vec<TeamConfig>,
    pub audit_log_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamConfig {
    pub team_id: String,
    pub team_name: String,
    pub cost_budget: Option<f64>,
    pub members: Vec<String>,
    pub pipelines: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernancePolicy {
    pub id: String,
    pub name: String,
    pub description: String,
    pub rules: Vec<PolicyRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    pub rule_type: String,  // "cost_limit", "reliability_gate", "deployment_standard"
    pub threshold: f32,
    pub action: PolicyAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyAction {
    Warn,
    Block,
    RequireApproval,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLog {
    pub id: String,
    pub timestamp: String,
    pub org_id: String,
    pub action: String,
    pub resource: String,
    pub user_id: String,
    pub details: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostAttribution {
    pub pipeline_id: String,
    pub team_id: String,
    pub estimated_monthly_cost: f64,
    pub cost_breakdown: HashMap<String, f64>,
    pub budget_remaining: Option<f64>,
    pub budget_utilization_percent: Option<f32>,
}

impl OrganizationConfig {
    /// All features available to all users - PyBeamGuard is FREE
    pub fn can_use_feature(&self, _feature: &str) -> bool {
        true
    }

    pub fn is_cost_within_budget(&self, estimated_cost: f64) -> bool {
        if let Some(budget) = self.cost_budget_per_month {
            estimated_cost <= budget
        } else {
            true
        }
    }

    pub fn is_reliability_acceptable(&self, score: f32) -> bool {
        if let Some(min_score) = self.reliability_slo_score {
            score >= min_score
        } else {
            true
        }
    }

    pub fn should_block_deployment(&self, estimated_cost: f64, reliability_score: f32) -> bool {
        if self.enforce_cost_limits && !self.is_cost_within_budget(estimated_cost) {
            return true;
        }

        if self.enforce_reliability_gates && !self.is_reliability_acceptable(reliability_score) {
            return true;
        }

        false
    }
}

// REST API endpoints (when SaaS platform is built)
/*
POST /api/v1/analyze
  - Analyze a Beam pipeline
  - Auth: None required (free)
  - Rate limit: Generous (1000+ requests/minute)

GET /api/v1/organizations/{org_id}
  - Get organization configuration
  - Auth: API key + owner

PUT /api/v1/organizations/{org_id}
  - Update organization settings
  - Auth: Admin token
  - Governance, SLOs, policies

GET /api/v1/organizations/{org_id}/audit-log
  - Retrieve audit log
  - Auth: Admin token

GET /api/v1/organizations/{org_id}/cost-attribution
  - Get team cost breakdown
  - Auth: Admin token

POST /api/v1/organizations/{org_id}/policies
  - Create governance policy
  - Auth: Admin token
  - Enforce cost limits, reliability gates, deployment standards

All endpoints are FREE - no licensing, no tiers, no paywalls
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_features_free() {
        let org = OrganizationConfig {
            org_id: "org_1".to_string(),
            org_name: "Test Org".to_string(),
            cost_budget_per_month: None,
            reliability_slo_score: None,
            performance_slo_score: None,
            enforce_dlq_requirement: false,
            enforce_error_handling: false,
            enforce_cost_limits: false,
            enforce_reliability_gates: false,
            teams: vec![],
            audit_log_enabled: false,
        };

        // All features available - it's FREE
        assert!(org.can_use_feature("analyze"));
        assert!(org.can_use_feature("export_json"));
        assert!(org.can_use_feature("export_html"));
        assert!(org.can_use_feature("cost_forecasting"));
        assert!(org.can_use_feature("ci_cd_integration"));
        assert!(org.can_use_feature("api_access"));
        assert!(org.can_use_feature("anything_else"));
    }

    #[test]
    fn test_cost_enforcement() {
        let org = OrganizationConfig {
            org_id: "org_123".to_string(),
            org_name: "Test Org".to_string(),
            cost_budget_per_month: Some(5000.0),
            reliability_slo_score: Some(70.0),
            performance_slo_score: None,
            enforce_dlq_requirement: false,
            enforce_error_handling: false,
            enforce_cost_limits: true,
            enforce_reliability_gates: true,
            teams: vec![],
            audit_log_enabled: true,
        };

        // Within budget
        assert!(!org.should_block_deployment(4000.0, 85.0));

        // Over budget
        assert!(org.should_block_deployment(6000.0, 85.0));

        // Below reliability SLA
        assert!(org.should_block_deployment(4000.0, 50.0));
    }
}
