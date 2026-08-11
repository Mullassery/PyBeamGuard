use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct HotKeyAnalyzer;

// Risk keywords that suggest high-cardinality key domains
const HIGH_RISK_PATTERNS: &[&str] = &[
    "customer",
    "tenant",
    "org",
    "account",
    "user",
    "organization",
    "client",
    "partner",
    "company",
];

// Medium-risk patterns
const MEDIUM_RISK_PATTERNS: &[&str] = &["hour", "day", "date", "month", "timestamp"];

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

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let profile = ctx.data_profile.as_ref();
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        // Find all shuffle operations (GroupByKey, CoGroupByKey, CombinePerKey)
        for node in &ir.nodes {
            if let TransformType::GroupByKey { key_expr } = &node.node_type {
                let risk = self.assess_key_risk(key_expr, profile);
                if risk.severity != RiskSeverity::Info {
                    findings.push(risk);
                }
            } else if let TransformType::CoGroupByKey { key_expr } = &node.node_type {
                let risk = self.assess_key_risk(key_expr, profile);
                if risk.severity != RiskSeverity::Info {
                    findings.push(risk);
                }
            }
        }

        metrics.insert(
            "groupbykey_count".to_string(),
            ir.nodes
                .iter()
                .filter(|n| matches!(n.node_type, TransformType::GroupByKey { .. }))
                .count() as f64,
        );
        metrics.insert(
            "cogroupbykey_count".to_string(),
            ir.nodes
                .iter()
                .filter(|n| matches!(n.node_type, TransformType::CoGroupByKey { .. }))
                .count() as f64,
        );
        if let Some(cardinality) = profile.and_then(|p| p.key_cardinality) {
            metrics.insert(
                "data_profile_key_cardinality".to_string(),
                cardinality as f64,
            );
        }

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
            confidence: if profile.and_then(|p| p.key_cardinality).is_some() {
                0.85
            } else {
                0.75
            },
        })
    }
}

impl HotKeyAnalyzer {
    fn assess_key_risk(&self, key_expr: &Option<String>, profile: Option<&DataProfile>) -> Finding {
        let key_str = key_expr.as_deref().unwrap_or("unknown");
        let measured_cardinality = profile.and_then(|p| p.key_cardinality);

        // A measured key cardinality from a data profile is a stronger signal
        // than the name-pattern heuristics below: very few distinct keys
        // sharing all traffic is close to a worst-case hot-key scenario
        // regardless of what the key expression looks like syntactically.
        if let Some(cardinality) = measured_cardinality {
            if cardinality > 0 && cardinality < 1_000 {
                return Finding {
                    id: "HOTKEY_LOW_CARDINALITY_MEASURED".to_string(),
                    severity: RiskSeverity::Critical,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!(
                        "Measured key cardinality ({}) on '{}' is dangerously low",
                        cardinality, key_str
                    ),
                    description: format!(
                        "The supplied data profile reports only {} distinct key(s) for '{}'. With \
                         this few partitions, traffic concentrates on a handful of workers regardless \
                         of what the key expression looks like.",
                        cardinality, key_str
                    ),
                    affected_nodes: vec![],
                    recommendation: Some(
                        "Add a synthetic sharding dimension (e.g., hash(payload) % N) to fan this \
                         key out across more partitions before aggregating."
                            .to_string(),
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(4.0),
                        cost_delta_monthly: Some(800.0),
                        affected_records_percent: Some(100.0),
                    }),
                    confidence: 0.90,
                };
            }
        }

        // Check for high-risk patterns (customer_id, tenant_id, etc.)
        for pattern in HIGH_RISK_PATTERNS {
            if key_str.to_lowercase().contains(pattern) {
                // A large measured cardinality means traffic is already
                // spread over many keys, which reduces (without eliminating)
                // the risk implied by the name pattern alone.
                let high_cardinality_measured = measured_cardinality.is_some_and(|c| c >= 100_000);
                let severity = if high_cardinality_measured {
                    RiskSeverity::Medium
                } else {
                    RiskSeverity::High
                };

                return Finding {
                    id: "HOTKEY_HIGH_RISK".to_string(),
                    severity,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!("High hot-key probability on '{}'", key_str),
                    description: if high_cardinality_measured {
                        format!(
                            "Aggregating by '{}' matches a high-risk key pattern, but the supplied \
                             data profile measured a large key cardinality ({}), which reduces the \
                             likelihood of a small number of keys dominating traffic.",
                            key_str,
                            measured_cardinality.unwrap()
                        )
                    } else {
                        format!(
                            "Aggregating by '{}' likely creates key skew. This key domain has high cardinality variance (e.g., some customers generate 100x more data than others), creating worker imbalance.",
                            key_str
                        )
                    },
                    affected_nodes: vec![],
                    recommendation: Some(
                        "Apply key sharding strategy: append a UUID or hash to distribute load. Example: sharded_key = (customer_id, hash(data) % num_shards)".to_string()
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(3.0),
                        cost_delta_monthly: Some(500.0),
                        affected_records_percent: Some(5.0),
                    }),
                    confidence: if measured_cardinality.is_some() { 0.88 } else { 0.82 },
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
        let risk = analyzer.assess_key_risk(&Some("customer_id".to_string()), None);
        assert_eq!(risk.severity, RiskSeverity::High);
    }

    #[test]
    fn test_medium_risk_timestamp_key() {
        let analyzer = HotKeyAnalyzer;
        let risk = analyzer.assess_key_risk(&Some("hour".to_string()), None);
        assert_eq!(risk.severity, RiskSeverity::Medium);
    }

    #[test]
    fn test_data_profile_low_cardinality_escalates_to_critical() {
        let analyzer = HotKeyAnalyzer;
        let profile = DataProfile {
            estimated_throughput_per_sec: None,
            average_element_size_bytes: None,
            key_cardinality: Some(12),
            estimated_state_size_gb: None,
        };
        // Even a syntactically "safe" looking key is critical if the
        // measured cardinality is dangerously low.
        let risk = analyzer.assess_key_risk(&Some("region".to_string()), Some(&profile));
        assert_eq!(risk.severity, RiskSeverity::Critical);
        assert_eq!(risk.id, "HOTKEY_LOW_CARDINALITY_MEASURED");
    }

    #[test]
    fn test_data_profile_high_cardinality_downgrades_high_risk_pattern() {
        let analyzer = HotKeyAnalyzer;
        let profile = DataProfile {
            estimated_throughput_per_sec: None,
            average_element_size_bytes: None,
            key_cardinality: Some(500_000),
            estimated_state_size_gb: None,
        };
        let risk = analyzer.assess_key_risk(&Some("customer_id".to_string()), Some(&profile));
        assert_eq!(risk.severity, RiskSeverity::Medium);
    }

    #[test]
    fn test_analyze_uses_context_data_profile() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(TransformNode {
            id: "node_1".to_string(),
            name: "GroupByKey".to_string(),
            node_type: TransformType::GroupByKey {
                key_expr: Some("shard_id".to_string()),
            },
            inputs: vec![],
            outputs: vec!["grouped".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        });

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: Some(DataProfile {
                estimated_throughput_per_sec: None,
                average_element_size_bytes: None,
                key_cardinality: Some(5),
                estimated_state_size_gb: None,
            }),
        };

        let result = analyzer_analyze_ok(&ctx);
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "HOTKEY_LOW_CARDINALITY_MEASURED"));
        assert_eq!(
            result.metrics.get("data_profile_key_cardinality"),
            Some(&5.0)
        );
    }

    fn analyzer_analyze_ok(ctx: &AnalysisContext) -> AnalysisResult {
        HotKeyAnalyzer.analyze(ctx).unwrap()
    }
}
