use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

// Same high-risk key-domain heuristic used by the Beam `HotKeyAnalyzer`,
// applied here to Flink's `key_by(...)` expressions -- keyed state in
// Flink concentrates on exactly these domains the same way a Beam
// GroupByKey does.
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

/// Analyzes Flink state management: which state backend is configured (and
/// whether it's appropriate for the workload), and whether `key_by(...)`
/// expressions look like they'll concentrate load on a small number of
/// keys (the Flink analog of Beam's hot-key detection, applied to keyed
/// state instead of GroupByKey).
pub struct FlinkStateAnalyzer;

impl Analyzer for FlinkStateAnalyzer {
    fn name(&self) -> &str {
        "FlinkStateAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        21
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let profile = ctx.data_profile.as_ref();
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let keyed_nodes: Vec<_> = ir
            .nodes
            .iter()
            .filter(
                |n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_keyed_state"),
            )
            .collect();
        metrics.insert("keyed_state_count".to_string(), keyed_nodes.len() as f64);

        let backend_node = ir.nodes.iter().find(
            |n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_state_backend"),
        );

        self.analyze_state_backend(
            backend_node,
            !keyed_nodes.is_empty(),
            &mut findings,
            &mut metrics,
        );

        for node in &keyed_nodes {
            let key_expr = node
                .config
                .get("key_expr")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if let Some(finding) = self.assess_key_risk(node, key_expr, profile) {
                findings.push(finding);
            }
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings: findings.clone(),
            metrics,
            summary: if findings.is_empty() {
                "No state backend or keyed-state risks detected".to_string()
            } else {
                format!("Found {} state-related risk(s)", findings.len())
            },
            confidence: 0.78,
        })
    }
}

impl FlinkStateAnalyzer {
    fn analyze_state_backend(
        &self,
        backend_node: Option<&TransformNode>,
        has_keyed_state: bool,
        findings: &mut Vec<Finding>,
        metrics: &mut HashMap<String, f64>,
    ) {
        metrics.insert(
            "state_backend_configured".to_string(),
            if backend_node.is_some() { 1.0 } else { 0.0 },
        );

        let Some(node) = backend_node else {
            if has_keyed_state {
                findings.push(Finding {
                    id: "FLINK_STATE_BACKEND_UNSPECIFIED".to_string(),
                    severity: RiskSeverity::Medium,
                    finding_type: FindingType::ConfigurationIssue,
                    title: "No explicit state backend configured".to_string(),
                    description: "This pipeline uses keyed state but no `set_state_backend(...)` call was found. The default varies by Flink version/cluster config, which makes state size and checkpoint behavior unpredictable across environments.".to_string(),
                    affected_nodes: vec![],
                    recommendation: Some("Explicitly configure a state backend: EmbeddedRocksDBStateBackend for state that may exceed heap, HashMapStateBackend for small, fast-access state.".to_string()),
                    estimated_impact: None,
                    confidence: 0.65,
                });
            }
            return;
        };

        let backend = node
            .config
            .get("backend")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let incremental = node.config.get("incremental").and_then(|v| v.as_bool());

        if backend == "HashMapStateBackend" || backend == "MemoryStateBackend" {
            findings.push(Finding {
                id: "FLINK_STATE_BACKEND_HEAP_RISK".to_string(),
                severity: RiskSeverity::High,
                finding_type: FindingType::ScalabilityRisk,
                title: format!("Heap-bound state backend in use: {}", backend),
                description: format!(
                    "{} keeps all state as Java objects on the JVM heap. If keyed state grows beyond available heap (many distinct keys, large per-key state, or long windows), the TaskManager will hit OutOfMemoryError.",
                    backend
                ),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Switch to EmbeddedRocksDBStateBackend for state that can grow large or is hard to bound in advance -- it spills to local disk instead of requiring everything in heap.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: None,
                    affected_records_percent: Some(100.0),
                }),
                confidence: 0.70,
            });
        }

        if backend == "FsStateBackend" {
            findings.push(Finding {
                id: "FLINK_STATE_BACKEND_DEPRECATED_FS".to_string(),
                severity: RiskSeverity::Low,
                finding_type: FindingType::ConfigurationIssue,
                title: "FsStateBackend is deprecated".to_string(),
                description: "FsStateBackend was deprecated in Flink 1.13+ in favor of the unified HashMapStateBackend (same semantics) plus a separate checkpoint storage configuration.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Migrate to HashMapStateBackend with `set_checkpoint_storage(...)` pointed at your durable filesystem.".to_string()),
                estimated_impact: None,
                confidence: 0.60,
            });
        }

        if backend.contains("RocksDB") && incremental != Some(true) {
            findings.push(Finding {
                id: "FLINK_STATE_BACKEND_ROCKSDB_NO_INCREMENTAL".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::PerformanceRisk,
                title: "RocksDB state backend without incremental checkpoints".to_string(),
                description: "Without incremental checkpointing, RocksDB uploads the full state snapshot on every checkpoint instead of just the delta. As state grows, checkpoint duration and I/O grow with it.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Enable incremental checkpoints, e.g. `EmbeddedRocksDBStateBackend(True)` / `env.get_checkpoint_config().enable_incremental_checkpointing(True)`.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: Some(200.0),
                    affected_records_percent: None,
                }),
                confidence: 0.65,
            });
        }
    }

    fn assess_key_risk(
        &self,
        node: &TransformNode,
        key_expr: &str,
        profile: Option<&DataProfile>,
    ) -> Option<Finding> {
        let measured_cardinality = profile.and_then(|p| p.key_cardinality);
        if let Some(cardinality) = measured_cardinality {
            if cardinality > 0 && cardinality < 1_000 {
                return Some(Finding {
                    id: "FLINK_KEYED_STATE_LOW_CARDINALITY_MEASURED".to_string(),
                    severity: RiskSeverity::Critical,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!(
                        "Measured key cardinality ({}) on key_by('{}') is dangerously low",
                        cardinality, key_expr
                    ),
                    description: format!(
                        "The supplied data profile reports only {} distinct key(s) for this keyed stream. Flink assigns each key to exactly one subtask's state, so this concentrates both processing and state on a handful of subtasks.",
                        cardinality
                    ),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Add a synthetic sharding dimension to the key (e.g. hash(payload) % N) to spread keyed state across more subtasks.".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(4.0),
                        cost_delta_monthly: None,
                        affected_records_percent: Some(100.0),
                    }),
                    confidence: 0.88,
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
                    id: "FLINK_KEYED_STATE_HOT_KEY_RISK".to_string(),
                    severity,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!("Keyed state on '{}' may concentrate load", key_expr),
                    description: format!(
                        "`key_by({})` partitions state by a domain that commonly has skewed cardinality (a few keys carry disproportionate traffic). Flink pins each key's state and processing to one subtask, so a hot key becomes a hot subtask.",
                        key_expr
                    ),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Consider a two-stage key: shard the hot key with a salted sub-key for local aggregation, then re-key on the original field for the final combine.".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(2.5),
                        cost_delta_monthly: None,
                        affected_records_percent: Some(5.0),
                    }),
                    confidence: if measured_cardinality.is_some() { 0.85 } else { 0.75 },
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyed_node(key_expr: &str) -> TransformNode {
        TransformNode {
            id: "node_flink_keyed_state_1".to_string(),
            name: "KeyBy".to_string(),
            node_type: TransformType::Custom("flink_keyed_state".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "key_expr": key_expr }),
            annotations: vec![],
            line_number: Some(1),
        }
    }

    fn backend_node(backend: &str, incremental: Option<bool>) -> TransformNode {
        TransformNode {
            id: "node_flink_state_backend_0".to_string(),
            name: "StateBackend".to_string(),
            node_type: TransformType::Custom("flink_state_backend".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "backend": backend, "incremental": incremental }),
            annotations: vec![],
            line_number: Some(0),
        }
    }

    #[test]
    fn test_no_backend_configured_with_keyed_state_flagged_medium() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(keyed_node("region"));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkStateAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_STATE_BACKEND_UNSPECIFIED"));
    }

    #[test]
    fn test_heap_backend_flagged_high() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(backend_node("HashMapStateBackend", None));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkStateAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_STATE_BACKEND_HEAP_RISK" && f.severity == RiskSeverity::High));
    }

    #[test]
    fn test_rocksdb_without_incremental_flagged() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes
            .push(backend_node("EmbeddedRocksDBStateBackend", Some(false)));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkStateAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_STATE_BACKEND_ROCKSDB_NO_INCREMENTAL"));
    }

    #[test]
    fn test_rocksdb_with_incremental_clean() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes
            .push(backend_node("EmbeddedRocksDBStateBackend", Some(true)));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkStateAnalyzer.analyze(&ctx).unwrap();
        assert!(result.findings.is_empty());
    }

    #[test]
    fn test_high_risk_key_by_customer_flagged() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes
            .push(backend_node("EmbeddedRocksDBStateBackend", Some(true)));
        ir.nodes.push(keyed_node("x.customer_id"));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkStateAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_KEYED_STATE_HOT_KEY_RISK" && f.severity == RiskSeverity::High));
    }

    #[test]
    fn test_data_profile_low_cardinality_escalates_to_critical() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes
            .push(backend_node("EmbeddedRocksDBStateBackend", Some(true)));
        ir.nodes.push(keyed_node("region"));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: Some(DataProfile {
                estimated_throughput_per_sec: None,
                average_element_size_bytes: None,
                key_cardinality: Some(8),
                estimated_state_size_gb: None,
            }),
        };
        let result = FlinkStateAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_KEYED_STATE_LOW_CARDINALITY_MEASURED"
                && f.severity == RiskSeverity::Critical));
    }
}
