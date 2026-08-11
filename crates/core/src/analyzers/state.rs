use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

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
}
