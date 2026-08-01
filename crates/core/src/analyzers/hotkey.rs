use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct HotKeyAnalyzer;

// Risk keywords that suggest high-cardinality key domains
const HIGH_RISK_PATTERNS: &[&str] = &[
    "customer", "tenant", "org", "account", "user", "organization",
    "client", "partner", "company",
];

// Medium-risk patterns
const MEDIUM_RISK_PATTERNS: &[&str] = &[
    "hour", "day", "date", "month", "timestamp",
];

impl Analyzer for HotKeyAnalyzer {
    fn name(&self) -> &str {
        "HotKeyAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        2
    }

    fn analyze(&self, ir: &PipelineIR) -> anyhow::Result<AnalysisResult> {
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        // Find all shuffle operations (GroupByKey, CoGroupByKey, CombinePerKey)
        for node in &ir.nodes {
            if let TransformType::GroupByKey { key_expr } = &node.node_type {
                let risk = self.assess_key_risk(key_expr);
                if risk.severity != RiskSeverity::Info {
                    findings.push(risk);
                }
            } else if let TransformType::CoGroupByKey { key_expr } = &node.node_type {
                let risk = self.assess_key_risk(key_expr);
                if risk.severity != RiskSeverity::Info {
                    findings.push(risk);
                }
            }
        }

        metrics.insert("groupbykey_count".to_string(),
            ir.nodes.iter().filter(|n| matches!(n.node_type, TransformType::GroupByKey { .. })).count() as f64);
        metrics.insert("cogroupbykey_count".to_string(),
            ir.nodes.iter().filter(|n| matches!(n.node_type, TransformType::CoGroupByKey { .. })).count() as f64);

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings: findings.clone(),
            metrics,
            summary: if findings.is_empty() {
                "No obvious hot key risks detected".to_string()
            } else {
                format!("Found {} potential hot key risk(s)", findings.len())
            },
            confidence: 0.75,
        })
    }
}

impl HotKeyAnalyzer {
    fn assess_key_risk(&self, key_expr: &Option<String>) -> Finding {
        let key_str = key_expr.as_deref().unwrap_or("unknown");

        // Check for high-risk patterns (customer_id, tenant_id, etc.)
        for pattern in HIGH_RISK_PATTERNS {
            if key_str.to_lowercase().contains(pattern) {
                return Finding {
                    id: "HOTKEY_HIGH_RISK".to_string(),
                    severity: RiskSeverity::High,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!("High hot-key probability on '{}'", key_str),
                    description: format!(
                        "Aggregating by '{}' likely creates key skew. This key domain has high cardinality variance (e.g., some customers generate 100x more data than others), creating worker imbalance.",
                        key_str
                    ),
                    affected_nodes: vec![],
                    recommendation: Some(
                        "Apply key sharding strategy: append a UUID or hash to distribute load. Example: sharded_key = (customer_id, hash(data) % num_shards)".to_string()
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(3.0),
                        cost_delta_monthly: Some(500.0),
                        affected_records_percent: Some(5.0),
                    }),
                    confidence: 0.82,
                };
            }
        }

        // Check for medium-risk patterns (timestamp-based keys)
        for pattern in MEDIUM_RISK_PATTERNS {
            if key_str.to_lowercase().contains(pattern) {
                return Finding {
                    id: "HOTKEY_MEDIUM_RISK".to_string(),
                    severity: RiskSeverity::Medium,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!("Moderate hot-key risk on '{}'", key_str),
                    description: format!(
                        "Aggregating by '{}' may create time-based skew, especially with time zones or peak hours.",
                        key_str
                    ),
                    affected_nodes: vec![],
                    recommendation: Some(
                        "Monitor key distribution at runtime. Consider combining with a dimension to spread load (e.g., customer_id + hour).".to_string()
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(1.5),
                        cost_delta_monthly: Some(200.0),
                        affected_records_percent: None,
                    }),
                    confidence: 0.65,
                };
            }
        }

        // Default: low risk for hash-based or unknown keys
        Finding {
            id: "HOTKEY_LOW_RISK".to_string(),
            severity: RiskSeverity::Info,
            finding_type: FindingType::PerformanceRisk,
            title: format!("Low hot-key risk on '{}'", key_str),
            description: "Key domain appears well-distributed".to_string(),
            affected_nodes: vec![],
            recommendation: None,
            estimated_impact: None,
            confidence: 0.60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_high_risk_customer_key() {
        let analyzer = HotKeyAnalyzer;
        let risk = analyzer.assess_key_risk(&Some("customer_id".to_string()));
        assert_eq!(risk.severity, RiskSeverity::High);
    }

    #[test]
    fn test_medium_risk_timestamp_key() {
        let analyzer = HotKeyAnalyzer;
        let risk = analyzer.assess_key_risk(&Some("hour".to_string()));
        assert_eq!(risk.severity, RiskSeverity::Medium);
    }
}
