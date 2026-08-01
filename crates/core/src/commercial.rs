use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct License {
    pub tier: LicenseTier,
    pub organization_id: String,
    pub license_key: String,
    pub expiration_date: String,
    pub max_analyses_per_month: Option<u32>,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LicenseTier {
    Community,      // Free, limited features
    Professional,   // $500/month, all core features
    Enterprise,     // $5K+/month, custom features
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationConfig {
    pub org_id: String,
    pub org_name: String,
    pub tier: LicenseTier,

    // Governance settings
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APIKey {
    pub key_id: String,
    pub org_id: String,
    pub secret_hash: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub scopes: Vec<String>,
    pub rate_limit_per_minute: u32,
}

impl License {
    pub fn is_valid(&self) -> bool {
        // Check expiration date
        // In production: parse ISO date and compare
        true
    }

    pub fn has_feature(&self, feature: &str) -> bool {
        self.features.contains(&feature.to_string())
    }
}

impl OrganizationConfig {
    pub fn can_use_feature(&self, feature: &str) -> bool {
        match self.tier {
            LicenseTier::Community => {
                matches!(feature, "analyze" | "export_json")
            }
            LicenseTier::Professional => {
                matches!(feature,
                    "analyze" | "export_json" | "export_html" |
                    "cost_forecasting" | "ci_cd_integration" |
                    "custom_reports"
                )
            }
            LicenseTier::Enterprise => {
                // All features
                true
            }
        }
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

// REST API endpoints documentation
/*
POST /api/v1/analyze
  - Analyze a Beam pipeline
  - Auth: API key required
  - Rate limit: 100 requests/minute

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

POST /api/v1/organizations/{org_id}/api-keys
  - Create API key
  - Auth: Admin token

GET /api/v1/organizations/{org_id}/cost-attribution
  - Get team cost breakdown
  - Auth: Admin token

POST /api/v1/organizations/{org_id}/policies
  - Create governance policy
  - Auth: Admin token
  - Enforce cost limits, reliability gates, deployment standards
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_license_validation() {
        let license = License {
            tier: LicenseTier::Professional,
            organization_id: "org_123".to_string(),
            license_key: "key_abc".to_string(),
            expiration_date: "2026-12-31".to_string(),
            max_analyses_per_month: Some(10000),
            features: vec!["analyze".to_string(), "export_json".to_string()],
        };

        assert!(license.is_valid());
        assert!(license.has_feature("analyze"));
        assert!(!license.has_feature("export_html"));
    }

    #[test]
    fn test_org_config_feature_access() {
        let community_org = OrganizationConfig {
            org_id: "org_1".to_string(),
            org_name: "Community Org".to_string(),
            tier: LicenseTier::Community,
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

        assert!(community_org.can_use_feature("analyze"));
        assert!(!community_org.can_use_feature("cost_forecasting"));

        let enterprise_org = OrganizationConfig {
            tier: LicenseTier::Enterprise,
            ..community_org
        };

        assert!(enterprise_org.can_use_feature("cost_forecasting"));
    }

    #[test]
    fn test_cost_enforcement() {
        let org = OrganizationConfig {
            org_id: "org_123".to_string(),
            org_name: "Test Org".to_string(),
            tier: LicenseTier::Professional,
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
