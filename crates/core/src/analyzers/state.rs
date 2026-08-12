use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

/// Above this measured state size, a single stateful pipeline is large
/// enough that unbounded growth (missing TTL/cleanup) becomes an operational
/// emergency rather than a slow-burn cost concern -- worth escalating past
/// the flat `STATE_UNBOUNDED_GROWTH` finding below.
const LARGE_MEASURED_STATE_SIZE_GB: f64 = 500.0;

pub struct StateAnalyzer;

impl Analyzer for StateAnalyzer {
    fn name(&self) -> &str {
        "StateAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        5
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let profile = ctx.data_profile.as_ref();
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let stateful_ops = self.find_stateful_operations(ir);
        let op_count = stateful_ops.len();
        metrics.insert("stateful_operation_count".to_string(), op_count as f64);

        if stateful_ops.is_empty() {
            return Ok(AnalysisResult {
                analyzer_name: self.name().to_string(),
                version: self.version().to_string(),
                findings: vec![],
                metrics,
                summary: "No stateful operations detected".to_string(),
                confidence: 0.95,
            });
        }

        // Analyze each stateful operation
        for stateful_op in &stateful_ops {
            if let TransformType::ParDo {
                dofn_name,
                is_stateful: true,
                ..
            } = &stateful_op.node_type
            {
                // Flag all stateful operations as needing review
                findings.push(Finding {
                    id: "STATE_UNBOUNDED_GROWTH".to_string(),
                    severity: RiskSeverity::High,
                    finding_type: FindingType::ReliabilityRisk,
                    title: format!("Stateful ParDo detected: '{}'", dofn_name),
                    description: "Stateful processing uses memory/disk. Without proper cleanup, state can grow unbounded and exhaust worker resources.".to_string(),
                    affected_nodes: vec![stateful_op.id.clone()],
                    recommendation: Some(
                        "Ensure state has explicit expiration logic:\n\
                         1. Set state TTL (e.g., 30 days)\n\
                         2. Implement timer-based cleanup\n\
                         3. Verify window expiration will clear state"
                            .to_string()
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: None,
                        cost_delta_monthly: Some(1000.0),
                        affected_records_percent: Some(100.0),
                    }),
                    confidence: 0.85,
                });

                // Additional warnings for common patterns
                findings.push(Finding {
                    id: "STATE_LIFECYCLE_CHECK".to_string(),
                    severity: RiskSeverity::Medium,
                    finding_type: FindingType::ReliabilityRisk,
                    title: "State lifecycle validation required".to_string(),
                    description: "Verify that state cleanup aligns with window expiration and allowed lateness.".to_string(),
                    affected_nodes: vec![stateful_op.id.clone()],
                    recommendation: Some(
                        "Review the state spec to confirm:\n\
                         - TimerSpec is used for cleanup\n\
                         - Window expiration (+ allowed lateness) > state TTL\n\
                         - Timers are actually fired (not dead code)"
                            .to_string()
                    ),
                    estimated_impact: None,
                    confidence: 0.75,
                });
            }
        }

        // Check for high-cardinality state (e.g., state keyed by user_id)
        for node in &ir.nodes {
            if let TransformType::ParDo {
                dofn_name,
                is_stateful: true,
                ..
            } = &node.node_type
            {
                if dofn_name.to_lowercase().contains("user")
                    || dofn_name.to_lowercase().contains("customer")
                    || dofn_name.to_lowercase().contains("session")
                {
                    findings.push(Finding {
                        id: "STATE_HIGH_CARDINALITY".to_string(),
                        severity: RiskSeverity::High,
                        finding_type: FindingType::ScalabilityRisk,
                        title: "High-cardinality state detected".to_string(),
                        description: "State is keyed by a high-cardinality dimension (users, customers, etc.). This may not scale to millions of keys.".to_string(),
                        affected_nodes: vec![node.id.clone()],
                        recommendation: Some(
                            "For millions of unique keys, consider:\n\
                             1. Sampling approach (process subset of data)\n\
                             2. Approximate algorithms (HyperLogLog for cardinality)\n\
                             3. Tiered storage (hot keys in memory, cold in Persistent Disk)"
                                .to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(2.0),
                            cost_delta_monthly: Some(2000.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.70,
                    });
                }
            }
        }

        // A measured state size from a data profile is a stronger signal
        // than the per-op flat heuristics above: real state that's already
        // this large means unbounded growth (missing TTL/cleanup) is not a
        // theoretical risk but an active operational concern.
        if let Some(state_size_gb) = profile.and_then(|p| p.estimated_state_size_gb) {
            metrics.insert("data_profile_state_size_gb".to_string(), state_size_gb);

            if state_size_gb > LARGE_MEASURED_STATE_SIZE_GB {
                findings.push(Finding {
                    id: "STATE_SIZE_MEASURED_CRITICAL".to_string(),
                    severity: RiskSeverity::Critical,
                    finding_type: FindingType::ScalabilityRisk,
                    title: format!(
                        "Measured state size ({:.0}GB) is dangerously large for {} stateful operation(s)",
                        state_size_gb, op_count
                    ),
                    description: format!(
                        "The supplied data profile reports {:.0}GB of state. At this size, missing TTL/cleanup logic or an unexpectedly long allowed-lateness window doesn't just add cost -- it risks checkpoint timeouts, slow restores, and worker disk pressure.",
                        state_size_gb
                    ),
                    affected_nodes: stateful_ops.iter().map(|n| n.id.clone()).collect(),
                    recommendation: Some(
                        "Audit every stateful operation for an explicit TTL/timer-based cleanup path, and confirm allowed lateness isn't retaining state far longer than the business logic requires. Consider partitioning state across more workers if a single key's state is the dominant contributor.".to_string()
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: None,
                        cost_delta_monthly: Some(state_size_gb * 0.04),
                        affected_records_percent: None,
                    }),
                    confidence: 0.80,
                });
            }
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: format!(
                "Found {} stateful operation(s); state management requires careful review",
                op_count
            ),
            confidence: 0.80,
        })
    }
}

impl StateAnalyzer {
    fn find_stateful_operations<'a>(&self, ir: &'a PipelineIR) -> Vec<&'a TransformNode> {
        ir.nodes
            .iter()
            .filter(|n| n.node_type.is_stateful())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stateful_operation_detection() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(TransformNode {
            id: "node_1".to_string(),
            name: "StatefulParDo".to_string(),
            node_type: TransformType::ParDo {
                dofn_name: "CountByUser".to_string(),
                is_stateful: true,
                has_side_inputs: false,
                has_side_outputs: false,
            },
            inputs: vec![],
            outputs: vec!["counted".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        });

        let analyzer = StateAnalyzer;
        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = analyzer.analyze(&ctx).unwrap();
        assert!(!result.findings.is_empty());
    }

    fn stateful_pardo_node(dofn_name: &str) -> TransformNode {
        TransformNode {
            id: "node_1".to_string(),
            name: "StatefulParDo".to_string(),
            node_type: TransformType::ParDo {
                dofn_name: dofn_name.to_string(),
                is_stateful: true,
                has_side_inputs: false,
                has_side_outputs: false,
            },
            inputs: vec![],
            outputs: vec!["counted".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        }
    }

    #[test]
    fn test_large_measured_state_size_escalates_to_critical() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(stateful_pardo_node("Aggregate"));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: Some(DataProfile {
                estimated_throughput_per_sec: None,
                average_element_size_bytes: None,
                key_cardinality: None,
                estimated_state_size_gb: Some(800.0),
            }),
        };
        let result = StateAnalyzer.analyze(&ctx).unwrap();
        assert!(result.findings.iter().any(
            |f| f.id == "STATE_SIZE_MEASURED_CRITICAL" && f.severity == RiskSeverity::Critical
        ));
        assert_eq!(
            result.metrics.get("data_profile_state_size_gb"),
            Some(&800.0)
        );
    }

    #[test]
    fn test_small_measured_state_size_does_not_escalate() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(stateful_pardo_node("Aggregate"));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: Some(DataProfile {
                estimated_throughput_per_sec: None,
                average_element_size_bytes: None,
                key_cardinality: None,
                estimated_state_size_gb: Some(5.0),
            }),
        };
        let result = StateAnalyzer.analyze(&ctx).unwrap();
        assert!(!result
            .findings
            .iter()
            .any(|f| f.id == "STATE_SIZE_MEASURED_CRITICAL"));
    }
}
