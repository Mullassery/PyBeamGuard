use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

/// Below this interval, checkpoint barriers fire so often that the barrier
/// alignment / snapshot overhead can meaningfully compete with actual
/// record processing for CPU and I/O.
const AGGRESSIVE_INTERVAL_MS: u64 = 1_000;
/// Above this interval, a failure can lose/replay up to this much work,
/// which is a large blast radius for most SLAs.
const LONG_INTERVAL_MS: u64 = 600_000; // 10 minutes

/// Analyzes Flink checkpoint configuration: whether checkpointing is enabled
/// at all for a stateful pipeline, whether the configured interval is in a
/// safe range, and whether the checkpointing mode/timeout are sound. Mirrors
/// the depth of the Beam `StateAnalyzer`/`WindowingAnalyzer`, but keyed off
/// the `flink_checkpoint_config` node `FlinkFrameworkParser` extracts from
/// `env.enable_checkpointing(...)` and related calls.
pub struct FlinkCheckpointAnalyzer;

impl Analyzer for FlinkCheckpointAnalyzer {
    fn name(&self) -> &str {
        "FlinkCheckpointAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        20
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let is_stateful = ir
            .metadata
            .runner_hints
            .get("has_state")
            .map(|v| v == "true")
            .unwrap_or(false);

        let checkpoint_node = ir.nodes.iter().find(
            |n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_checkpoint_config"),
        );

        metrics.insert(
            "checkpointing_configured".to_string(),
            if checkpoint_node.is_some() { 1.0 } else { 0.0 },
        );

        let Some(node) = checkpoint_node else {
            if is_stateful {
                findings.push(Finding {
                    id: "FLINK_CHECKPOINT_DISABLED".to_string(),
                    severity: RiskSeverity::Critical,
                    finding_type: FindingType::ReliabilityRisk,
                    title: "Stateful Flink pipeline without checkpointing enabled".to_string(),
                    description: "This pipeline uses keyed/stateful operators (KeyedProcessFunction, ValueState, key_by, ...) but no `enable_checkpointing(...)` call was detected. Without checkpointing, a task failure loses all in-flight state and offsets -- there is no recovery point.".to_string(),
                    affected_nodes: vec![],
                    recommendation: Some(
                        "Call `env.enable_checkpointing(<interval_ms>, CheckpointingMode.EXACTLY_ONCE)` before executing the job. A 30-120s interval is a reasonable starting point for most workloads.".to_string(),
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: None,
                        cost_delta_monthly: None,
                        affected_records_percent: Some(100.0),
                    }),
                    confidence: 0.85,
                });
            }
            return Ok(AnalysisResult {
                analyzer_name: self.name().to_string(),
                version: self.version().to_string(),
                findings,
                metrics,
                summary: if is_stateful {
                    "Stateful pipeline with no checkpointing configured".to_string()
                } else {
                    "No checkpointing configuration detected (stateless pipeline)".to_string()
                },
                confidence: 0.80,
            });
        };

        let interval_ms = node.config.get("interval_ms").and_then(|v| v.as_u64());
        let mode = node.config.get("mode").and_then(|v| v.as_str());
        let timeout_ms = node.config.get("timeout_ms").and_then(|v| v.as_u64());

        if let Some(interval) = interval_ms {
            metrics.insert("checkpoint_interval_ms".to_string(), interval as f64);

            if interval < AGGRESSIVE_INTERVAL_MS {
                findings.push(Finding {
                    id: "FLINK_CHECKPOINT_INTERVAL_TOO_AGGRESSIVE".to_string(),
                    severity: RiskSeverity::High,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!("Checkpoint interval is very aggressive ({}ms)", interval),
                    description: "Checkpointing more often than once per second forces frequent barrier alignment and state snapshotting, which competes with real processing for CPU/I/O and can increase backpressure under load.".to_string(),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Increase the checkpoint interval to at least a few seconds (commonly 30-120s) unless you have measured that this workload tolerates frequent snapshots.".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(1.3),
                        cost_delta_monthly: None,
                        affected_records_percent: None,
                    }),
                    confidence: 0.75,
                });
            } else if interval > LONG_INTERVAL_MS {
                findings.push(Finding {
                    id: "FLINK_CHECKPOINT_INTERVAL_TOO_LONG".to_string(),
                    severity: RiskSeverity::Medium,
                    finding_type: FindingType::ReliabilityRisk,
                    title: format!(
                        "Checkpoint interval is very long ({} minutes)",
                        interval / 60_000
                    ),
                    description: "A long checkpoint interval means a failure can lose or force replay of a correspondingly large window of state and events, increasing recovery time and duplicate-processing risk.".to_string(),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Reduce the interval so a worst-case restart replays a bounded, acceptable amount of data for your SLA -- 30-120s is typical for most streaming workloads.".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: None,
                        cost_delta_monthly: None,
                        affected_records_percent: Some(5.0),
                    }),
                    confidence: 0.70,
                });
            }
        }

        match mode {
            Some("AT_LEAST_ONCE") => {
                findings.push(Finding {
                    id: "FLINK_CHECKPOINT_AT_LEAST_ONCE".to_string(),
                    severity: RiskSeverity::Medium,
                    finding_type: FindingType::ReliabilityRisk,
                    title: "Checkpointing mode is AT_LEAST_ONCE".to_string(),
                    description: "AT_LEAST_ONCE allows duplicate delivery of records on restore. This is only safe if every downstream sink is idempotent or deduplicates on a stable key.".to_string(),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Switch to CheckpointingMode.EXACTLY_ONCE unless throughput is checkpoint-bound and all sinks are verified idempotent.".to_string()),
                    estimated_impact: None,
                    confidence: 0.65,
                });
            }
            None => {
                findings.push(Finding {
                    id: "FLINK_CHECKPOINT_MODE_UNSPECIFIED".to_string(),
                    severity: RiskSeverity::Low,
                    finding_type: FindingType::ConfigurationIssue,
                    title: "Checkpointing mode not explicitly set".to_string(),
                    description: "`enable_checkpointing(interval)` was called without an explicit CheckpointingMode. Flink defaults to EXACTLY_ONCE, but relying on the implicit default makes the guarantee easy to lose track of during refactors.".to_string(),
                    affected_nodes: vec![node.id.clone()],
                    recommendation: Some("Pass the mode explicitly: `env.enable_checkpointing(interval_ms, CheckpointingMode.EXACTLY_ONCE)`.".to_string()),
                    estimated_impact: None,
                    confidence: 0.55,
                });
            }
            _ => {}
        }

        if timeout_ms.is_none() {
            findings.push(Finding {
                id: "FLINK_CHECKPOINT_NO_TIMEOUT_CONFIGURED".to_string(),
                severity: RiskSeverity::Low,
                finding_type: FindingType::ConfigurationIssue,
                title: "No explicit checkpoint timeout configured".to_string(),
                description: "Without `set_checkpoint_timeout(...)`, Flink's default (10 minutes) applies. For pipelines with large state, checkpoints that run long under load can be silently aborted right when they're most needed.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Set an explicit timeout via `env.get_checkpoint_config().set_checkpoint_timeout(<ms>)` sized to your largest expected state snapshot.".to_string()),
                estimated_impact: None,
                confidence: 0.55,
            });
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings: findings.clone(),
            metrics,
            summary: if findings.is_empty() {
                "Checkpointing is configured and looks reasonable".to_string()
            } else {
                format!("Found {} checkpoint configuration issue(s)", findings.len())
            },
            confidence: 0.80,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint_node(config: serde_json::Value) -> TransformNode {
        TransformNode {
            id: "node_flink_checkpoint_config_0".to_string(),
            name: "CheckpointConfig".to_string(),
            node_type: TransformType::Custom("flink_checkpoint_config".to_string()),
            inputs: vec![],
            outputs: vec![],
            config,
            annotations: vec![],
            line_number: Some(0),
        }
    }

    #[test]
    fn test_stateful_pipeline_without_checkpointing_is_critical() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.metadata
            .runner_hints
            .insert("has_state".to_string(), "true".to_string());

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkCheckpointAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_CHECKPOINT_DISABLED" && f.severity == RiskSeverity::Critical));
    }

    #[test]
    fn test_aggressive_interval_flagged_high() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(checkpoint_node(serde_json::json!({
            "interval_ms": 200,
            "mode": "EXACTLY_ONCE",
            "timeout_ms": 60000,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkCheckpointAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_CHECKPOINT_INTERVAL_TOO_AGGRESSIVE"
                && f.severity == RiskSeverity::High));
    }

    #[test]
    fn test_long_interval_flagged_medium() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(checkpoint_node(serde_json::json!({
            "interval_ms": 900_000,
            "mode": "EXACTLY_ONCE",
            "timeout_ms": 60000,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkCheckpointAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_CHECKPOINT_INTERVAL_TOO_LONG"));
    }

    #[test]
    fn test_at_least_once_mode_flagged() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(checkpoint_node(serde_json::json!({
            "interval_ms": 60000,
            "mode": "AT_LEAST_ONCE",
            "timeout_ms": 60000,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkCheckpointAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_CHECKPOINT_AT_LEAST_ONCE"));
    }

    #[test]
    fn test_well_configured_checkpointing_has_no_findings() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(checkpoint_node(serde_json::json!({
            "interval_ms": 60000,
            "mode": "EXACTLY_ONCE",
            "timeout_ms": 120000,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkCheckpointAnalyzer.analyze(&ctx).unwrap();
        assert!(result.findings.is_empty());
    }
}
