use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct ShuffleAnalyzer;

impl Analyzer for ShuffleAnalyzer {
    fn name(&self) -> &str {
        "ShuffleAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        3
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let shuffle_ops = self.find_shuffle_operations(ir);
        metrics.insert(
            "shuffle_operation_count".to_string(),
            shuffle_ops.len() as f64,
        );

        if shuffle_ops.is_empty() {
            return Ok(AnalysisResult {
                analyzer_name: self.name().to_string(),
                version: self.version().to_string(),
                findings: vec![],
                metrics,
                summary: "No shuffle operations detected".to_string(),
                confidence: 0.95,
            });
        }

        // Analyze each shuffle operation
        for (idx, shuffle_op) in shuffle_ops.iter().enumerate() {
            let stage_num = idx + 1;

            match shuffle_op.node_type {
                TransformType::GroupByKey { .. } => {
                    findings.push(Finding {
                        id: format!("SHUFFLE_GROUPBYKEY_{}", stage_num),
                        severity: RiskSeverity::Medium,
                        finding_type: FindingType::PerformanceRisk,
                        title: format!("Stage {}: GroupByKey shuffle operation", stage_num),
                        description: format!(
                            "Transform '{}' uses GroupByKey, which triggers a full shuffle. This is expensive and should be used carefully.",
                            shuffle_op.name
                        ),
                        affected_nodes: vec![shuffle_op.id.clone()],
                        recommendation: Some(
                            "Consider using CombinePerKey instead if you're computing an associative, commutative operation (sum, count, max, etc.)".to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(1.8),
                            cost_delta_monthly: Some(300.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.85,
                    });
                }
                TransformType::CoGroupByKey { .. } => {
                    findings.push(Finding {
                        id: format!("SHUFFLE_COGROUPBYKEY_{}", stage_num),
                        severity: RiskSeverity::High,
                        finding_type: FindingType::PerformanceRisk,
                        title: format!("Stage {}: CoGroupByKey (join) shuffle operation", stage_num),
                        description: format!(
                            "Transform '{}' uses CoGroupByKey for a join operation. This requires full shuffle of both inputs.",
                            shuffle_op.name
                        ),
                        affected_nodes: vec![shuffle_op.id.clone()],
                        recommendation: Some(
                            "Consider pre-aggregating the larger input before the join to reduce shuffle volume.".to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(2.5),
                            cost_delta_monthly: Some(800.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.88,
                    });
                }
                TransformType::CombinePerKey { .. } => {
                    findings.push(Finding {
                        id: format!("SHUFFLE_COMBINEPERKEY_{}", stage_num),
                        severity: RiskSeverity::Low,
                        finding_type: FindingType::PerformanceRisk,
                        title: format!("Stage {}: CombinePerKey operation (optimized)", stage_num),
                        description: format!(
                            "Transform '{}' uses CombinePerKey, which is more efficient than GroupByKey for associative operations.",
                            shuffle_op.name
                        ),
                        affected_nodes: vec![shuffle_op.id.clone()],
                        recommendation: None,
                        estimated_impact: None,
                        confidence: 0.90,
                    });
                }
                _ => {}
            }
        }

        // Check for multiple consecutive shuffles (expensive)
        let consecutive = self.find_consecutive_shuffles(ir, &shuffle_ops);
        if consecutive.len() > 1 {
            findings.push(Finding {
                id: "SHUFFLE_CONSECUTIVE".to_string(),
                severity: RiskSeverity::High,
                finding_type: FindingType::CostRisk,
                title: format!(
                    "Multiple consecutive shuffle operations detected ({} stages)",
                    consecutive.len()
                ),
                description: "Multiple shuffles in sequence dramatically increase cost and latency.".to_string(),
                affected_nodes: consecutive.iter().map(|n| n.id.clone()).collect(),
                recommendation: Some(
                    "Consider reorganizing the pipeline to minimize reshuffles. If possible, push aggregations before joins.".to_string()
                ),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(3.5),
                    cost_delta_monthly: Some(1200.0),
                    affected_records_percent: None,
                }),
                confidence: 0.80,
            });
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: format!(
                "Detected {} shuffle operation(s) across {} stage(s)",
                shuffle_ops.len(),
                shuffle_ops.len()
            ),
            confidence: 0.85,
        })
    }
}

impl ShuffleAnalyzer {
    fn find_shuffle_operations<'a>(&self, ir: &'a PipelineIR) -> Vec<&'a TransformNode> {
        ir.nodes
            .iter()
            .filter(|n| n.node_type.is_shuffle_operation())
            .collect()
    }

    fn find_consecutive_shuffles<'a>(
        &self,
        ir: &'a PipelineIR,
        shuffle_ops: &[&'a TransformNode],
    ) -> Vec<&'a TransformNode> {
        let mut consecutive: Vec<&'a TransformNode> = Vec::new();

        for shuffle_op in shuffle_ops {
            let downstream = ir.get_downstream_nodes(&shuffle_op.id);
            for down in downstream {
                if down.node_type.is_shuffle_operation() {
                    // Two shuffles in a row
                    consecutive.push(shuffle_op);
                    consecutive.push(down);
                }
            }
        }

        consecutive.sort_by_key(|n| ir.edges.iter().filter(|e| e.to == n.id).count());

        consecutive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shuffle_detection() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(TransformNode {
            id: "node_1".to_string(),
            name: "GroupByKey".to_string(),
            node_type: TransformType::GroupByKey { key_expr: None },
            inputs: vec![],
            outputs: vec!["grouped".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        });

        let analyzer = ShuffleAnalyzer;
        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = analyzer.analyze(&ctx).unwrap();
        assert!(!result.findings.is_empty());
    }
}
