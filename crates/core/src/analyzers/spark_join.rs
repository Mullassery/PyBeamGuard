use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

// Same high-risk key-domain heuristic used by Beam's `HotKeyAnalyzer` and
// Flink's `FlinkStateAnalyzer`, applied to Spark join keys: these domains
// are the usual suspects for skewed shuffle joins (a handful of tenants/
// customers dominate the row count).
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

/// 1GB. Spark's default `autoBroadcastJoinThreshold` is 10MB; a threshold
/// raised well past this risks broadcasting a table large enough to exhaust
/// driver or executor memory when it's collected for the broadcast.
const BROADCAST_THRESHOLD_HIGH_RISK_BYTES: i64 = 1_073_741_824;

/// Analyzes Spark join strategy: `autoBroadcastJoinThreshold`
/// misconfiguration (disabled or dangerously high), and join keys that look
/// likely to be skewed (the Spark analog of Beam's hot-key detection,
/// applied to shuffle join keys instead of GroupByKey).
pub struct SparkJoinAnalyzer;

impl Analyzer for SparkJoinAnalyzer {
    fn name(&self) -> &str {
        "SparkJoinAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        31
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let profile = ctx.data_profile.as_ref();
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let joins: Vec<_> = ir
            .nodes
            .iter()
            .filter(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_join"))
            .collect();
        metrics.insert("join_count".to_string(), joins.len() as f64);

        let threshold_node = ir.nodes.iter().find(
            |n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_broadcast_threshold_config"),
        );

        if let Some(node) = threshold_node {
            if let Some(threshold) = node.config.get("threshold_bytes").and_then(|v| v.as_i64()) {
                metrics.insert("broadcast_threshold_bytes".to_string(), threshold as f64);

                if threshold < 0 {
                    findings.push(Finding {
                        id: "SPARK_BROADCAST_THRESHOLD_DISABLED".to_string(),
                        severity: RiskSeverity::Medium,
                        finding_type: FindingType::PerformanceRisk,
                        title: "autoBroadcastJoinThreshold is disabled".to_string(),
                        description: "Setting autoBroadcastJoinThreshold to -1 disables Spark's automatic broadcast-join optimization entirely. Every join falls back to a shuffle (sort-merge) join, even joins against a small dimension table that would otherwise avoid a shuffle.".to_string(),
                        affected_nodes: vec![node.id.clone()],
                        recommendation: Some("Re-enable automatic broadcast joins (remove the override, or set a sane threshold like the 10MB default) unless you've hit a specific broadcast-related failure. Use explicit broadcast() hints for the joins that need it instead of disabling the optimizer globally.".to_string()),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(1.5),
                            cost_delta_monthly: None,
                            affected_records_percent: None,
                        }),
                        confidence: 0.65,
                    });
                } else if threshold > BROADCAST_THRESHOLD_HIGH_RISK_BYTES {
                    findings.push(Finding {
                        id: "SPARK_BROADCAST_THRESHOLD_EXCESSIVE".to_string(),
                        severity: RiskSeverity::High,
                        finding_type: FindingType::ReliabilityRisk,
                        title: format!(
                            "autoBroadcastJoinThreshold is very high ({} MB)",
                            threshold / 1_048_576
                        ),
                        description: "A high broadcast threshold means Spark will automatically collect and broadcast any table under that size to every executor. If a table grows past what executors can hold in memory, this causes driver/executor OOM instead of a slower-but-safe shuffle join.".to_string(),
                        affected_nodes: vec![node.id.clone()],
                        recommendation: Some("Lower the threshold to a size you've verified is safe for your executor memory (typically tens to low hundreds of MB), and use explicit broadcast() hints for specific joins you've validated instead of a large blanket threshold.".to_string()),
                        estimated_impact: Some(Impact {
                            latency_multiplier: None,
                            cost_delta_monthly: None,
                            affected_records_percent: None,
                        }),
                        confidence: 0.68,
                    });
                }
            }
        }

        for node in &joins {
            let key_expr = node.config.get("key_expr").and_then(|v| v.as_str());
            let is_broadcast = node
                .config
                .get("is_broadcast")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if is_broadcast {
                findings.push(Finding {
                    id: "SPARK_EXPLICIT_BROADCAST_USED".to_string(),
                    severity: RiskSeverity::Info,
                    finding_type: FindingType::ConfigurationIssue,
                    title: "Explicit broadcast join hint in use".to_string(),
                    description: "This join uses an explicit broadcast() or .hint(\"broadcast\") call. Confirm the broadcasted side is verifiably small (well under executor memory) -- an explicit hint bypasses Spark's size-based safety check.".to_string(),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Verify the broadcasted DataFrame's size with .count()/storage metrics before relying on this hint in production, and revisit if the upstream data source can grow unbounded.".to_string()),
                    estimated_impact: None,
                    confidence: 0.55,
                });
                continue;
            }

            let Some(key) = key_expr else { continue };
            if let Some(finding) = self.assess_join_key_risk(node, key, profile) {
                findings.push(finding);
            }
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings: findings.clone(),
            metrics,
            summary: if findings.is_empty() {
                "No join strategy or skew risks detected".to_string()
            } else {
                format!("Found {} join-related risk(s)", findings.len())
            },
            confidence: 0.72,
        })
    }
}

impl SparkJoinAnalyzer {
    fn assess_join_key_risk(
        &self,
        node: &TransformNode,
        key_expr: &str,
        profile: Option<&DataProfile>,
    ) -> Option<Finding> {
        let measured_cardinality = profile.and_then(|p| p.key_cardinality);
        if let Some(cardinality) = measured_cardinality {
            if cardinality > 0 && cardinality < 1_000 {
                return Some(Finding {
                    id: "SPARK_JOIN_LOW_CARDINALITY_MEASURED".to_string(),
                    severity: RiskSeverity::Critical,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!(
                        "Measured key cardinality ({}) on join key '{}' is dangerously low",
                        cardinality, key_expr
                    ),
                    description: format!(
                        "The supplied data profile reports only {} distinct value(s) for the join key. A shuffle join partitions rows by this key, so this concentrates most of the join's data (and compute) onto a handful of tasks -- classic join skew.",
                        cardinality
                    ),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Salt the skewed key (append a random suffix and explode the smaller side), or enable Adaptive Query Execution's skew join optimization (`spark.sql.adaptive.skewJoin.enabled=true`).".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(3.5),
                        cost_delta_monthly: None,
                        affected_records_percent: Some(100.0),
                    }),
                    confidence: 0.85,
                });
            }
        }

        let key_lower = key_expr.to_lowercase();
        for pattern in HIGH_RISK_PATTERNS {
            if key_lower.contains(pattern) {
                let high_cardinality_measured = measured_cardinality.is_some_and(|c| c >= 100_000);
                let severity = if high_cardinality_measured {
                    RiskSeverity::Medium
                } else {
                    RiskSeverity::High
                };
                return Some(Finding {
                    id: "SPARK_JOIN_HIGH_RISK_KEY".to_string(),
                    severity,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!("Join on '{}' may be skewed", key_expr),
                    description: format!(
                        "Joining on '{}' matches a key domain that commonly has a long-tail distribution (a few values carry disproportionate row counts). A sort-merge/shuffle join on a skewed key produces straggler tasks that dominate the job's wall-clock time.",
                        key_expr
                    ),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Enable AQE skew join handling (`spark.sql.adaptive.skewJoin.enabled=true`), or manually salt the join key if AQE isn't available.".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(2.5),
                        cost_delta_monthly: None,
                        affected_records_percent: Some(5.0),
                    }),
                    confidence: if measured_cardinality.is_some() { 0.82 } else { 0.72 },
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join_node(key_expr: Option<&str>, is_broadcast: bool) -> TransformNode {
        TransformNode {
            id: "node_spark_join_1".to_string(),
            name: "Join".to_string(),
            node_type: TransformType::Custom("spark_join".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "key_expr": key_expr, "is_broadcast": is_broadcast }),
            annotations: vec![],
            line_number: Some(1),
        }
    }

    fn threshold_node(threshold_bytes: i64) -> TransformNode {
        TransformNode {
            id: "node_spark_broadcast_threshold_0".to_string(),
            name: "BroadcastThresholdConfig".to_string(),
            node_type: TransformType::Custom("spark_broadcast_threshold_config".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "threshold_bytes": threshold_bytes }),
            annotations: vec![],
            line_number: None,
        }
    }

    #[test]
    fn test_disabled_threshold_flagged() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(threshold_node(-1));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkJoinAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_BROADCAST_THRESHOLD_DISABLED"));
    }

    #[test]
    fn test_excessive_threshold_flagged_high() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(threshold_node(5_368_709_120)); // 5GB

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkJoinAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_BROADCAST_THRESHOLD_EXCESSIVE"
                && f.severity == RiskSeverity::High));
    }

    #[test]
    fn test_high_risk_join_key_flagged() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(join_node(Some("customer_id"), false));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkJoinAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_JOIN_HIGH_RISK_KEY" && f.severity == RiskSeverity::High));
    }

    #[test]
    fn test_broadcast_join_flagged_info() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(join_node(Some("customer_id"), true));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkJoinAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_EXPLICIT_BROADCAST_USED" && f.severity == RiskSeverity::Info));
        // Broadcast joins short-circuit the skew heuristic.
        assert!(!result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_JOIN_HIGH_RISK_KEY"));
    }

    #[test]
    fn test_data_profile_low_cardinality_escalates_to_critical() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(join_node(Some("region"), false));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: Some(DataProfile {
                estimated_throughput_per_sec: None,
                average_element_size_bytes: None,
                key_cardinality: Some(5),
                estimated_state_size_gb: None,
            }),
        };
        let result = SparkJoinAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_JOIN_LOW_CARDINALITY_MEASURED"
                && f.severity == RiskSeverity::Critical));
    }
}
