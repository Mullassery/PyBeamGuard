use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct BestPracticesAnalyzer;

// Encoded Apache Beam best practice rules (documentation)
// Rules are implemented inline in analyze() method

impl Analyzer for BestPracticesAnalyzer {
    fn name(&self) -> &str {
        "BestPracticesAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        8
    }

    fn analyze(&self, ir: &PipelineIR) -> anyhow::Result<AnalysisResult> {
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();
        let mut rule_violations = 0;

        // Rule 1: Prefer CombinePerKey over GroupByKey
        let groupbykey_count = ir.nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::GroupByKey { .. }))
            .count();
        let combineperkey_count = ir.nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::CombinePerKey { .. }))
            .count();

        if groupbykey_count > combineperkey_count && groupbykey_count > 0 {
            findings.push(Finding {
                id: "BESTPRAC_PREFER_COMBINEPERKEY".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::PerformanceRisk,
                title: "Use CombinePerKey instead of GroupByKey".to_string(),
                description: format!(
                    "Found {} GroupByKey operations vs {} CombinePerKey. \
                     For associative, commutative operations (sum, count, max, min), \
                     CombinePerKey is 20-30% faster and parallelizable.",
                    groupbykey_count, combineperkey_count
                ),
                affected_nodes: ir.nodes
                    .iter()
                    .filter(|n| matches!(n.node_type, TransformType::GroupByKey { .. }))
                    .map(|n| n.id.clone())
                    .collect(),
                recommendation: Some(
                    "Analyze GroupByKey operations. Replace with CombinePerKey where applicable: \
                     beam.CombinePerKey(SumFn()) instead of GroupByKey().CombineValues(sum)"
                        .to_string()
                ),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(0.75),
                    cost_delta_monthly: Some(-300.0),
                    affected_records_percent: None,
                }),
                confidence: 0.85,
            });
            rule_violations += 1;
        }

        // Rule 2: Detect redundant transforms
        let redundant = self.find_redundant_transforms(ir);
        if !redundant.is_empty() {
            findings.push(Finding {
                id: "BESTPRAC_REDUNDANT_TRANSFORMS".to_string(),
                severity: RiskSeverity::Low,
                finding_type: FindingType::PerformanceRisk,
                title: format!("Found {} redundant transform(s)", redundant.len()),
                description: "Some ParDo operations appear to be no-ops or redundant.".to_string(),
                affected_nodes: redundant,
                recommendation: Some("Review and remove unnecessary transforms.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(0.95),
                    cost_delta_monthly: Some(-50.0),
                    affected_records_percent: None,
                }),
                confidence: 0.70,
            });
            rule_violations += 1;
        }

        // Rule 3: Large object side inputs
        let large_side_inputs = ir.nodes
            .iter()
            .filter(|n| {
                if let TransformType::ParDo { has_side_inputs: true, .. } = n.node_type {
                    true
                } else {
                    false
                }
            })
            .count();

        if large_side_inputs > 0 {
            findings.push(Finding {
                id: "BESTPRAC_SIDE_INPUT_SIZE".to_string(),
                severity: RiskSeverity::Low,
                finding_type: FindingType::PerformanceRisk,
                title: format!("Found {} side input(s)", large_side_inputs),
                description: "Side inputs are broadcast to all workers. Large objects increase network overhead.".to_string(),
                affected_nodes: vec![],
                recommendation: Some(
                    "If side inputs >100MB, consider using BigTable, BigQuery, or external lookup instead of broadcasting."
                        .to_string()
                ),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(1.1),
                    cost_delta_monthly: Some(100.0),
                    affected_records_percent: None,
                }),
                confidence: 0.65,
            });
            rule_violations += 1;
        }

        // Rule 4: Multiple shuffles (reshuffling)
        let shuffle_ops: Vec<_> = ir.nodes
            .iter()
            .filter(|n| n.node_type.is_shuffle_operation())
            .collect();

        if shuffle_ops.len() > 2 {
            findings.push(Finding {
                id: "BESTPRAC_EXCESSIVE_RESHUFFLING".to_string(),
                severity: RiskSeverity::High,
                finding_type: FindingType::CostRisk,
                title: format!("Excessive reshuffling ({} shuffle operations)", shuffle_ops.len()),
                description: "Multiple shuffles dramatically increase cost. Each shuffle writes data to shuffle storage.".to_string(),
                affected_nodes: shuffle_ops.iter().map(|n| n.id.clone()).collect(),
                recommendation: Some(
                    "Reorganize pipeline to minimize shuffles. Group related aggregations together. \
                     Use pre-aggregation before joins."
                        .to_string()
                ),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(2.5),
                    cost_delta_monthly: Some(1500.0),
                    affected_records_percent: None,
                }),
                confidence: 0.80,
            });
            rule_violations += 1;
        }

        // Rule 5: Streaming Engine configuration
        if let Some(streaming) = ir.metadata.deployment_config.as_ref() {
            if streaming.streaming_engine != Some(true) {
                findings.push(Finding {
                    id: "BESTPRAC_STREAMING_ENGINE".to_string(),
                    severity: RiskSeverity::Medium,
                    finding_type: FindingType::PerformanceRisk,
                    title: "Streaming Engine not enabled".to_string(),
                    description: "Streaming Engine offloads state/shuffle to managed backend, improving performance and reducing worker memory pressure.".to_string(),
                    affected_nodes: vec![],
                    recommendation: Some(
                        "Enable Streaming Engine in Dataflow job options for all streaming pipelines."
                            .to_string()
                    ),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(0.7),
                        cost_delta_monthly: Some(-200.0),
                        affected_records_percent: None,
                    }),
                    confidence: 0.90,
                });
                rule_violations += 1;
            }
        }

        metrics.insert("groupbykey_count".to_string(), groupbykey_count as f64);
        metrics.insert("combineperkey_count".to_string(), combineperkey_count as f64);
        metrics.insert("side_input_count".to_string(), large_side_inputs as f64);
        metrics.insert("shuffle_operation_count".to_string(), shuffle_ops.len() as f64);
        metrics.insert("rule_violations".to_string(), rule_violations as f64);

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: format!(
                "Found {} best practice violation(s). Potential savings: $500-$2,000/month",
                rule_violations
            ),
            confidence: 0.80,
        })
    }
}

impl BestPracticesAnalyzer {
    fn find_redundant_transforms(&self, ir: &PipelineIR) -> Vec<String> {
        let mut redundant = Vec::new();

        for (i, node) in ir.nodes.iter().enumerate() {
            if let TransformType::ParDo { dofn_name, .. } = &node.node_type {
                // Check for identity ParDo (common mistake)
                if dofn_name.to_lowercase().contains("identity") ||
                   dofn_name.to_lowercase().contains("passthrough") ||
                   dofn_name.to_lowercase().contains("noop") {
                    redundant.push(node.id.clone());
                }

                // Check for consecutive identical ParDo
                if i > 0 {
                    if let TransformType::ParDo { dofn_name: prev_name, .. } = &ir.nodes[i - 1].node_type {
                        if dofn_name == prev_name {
                            redundant.push(node.id.clone());
                        }
                    }
                }
            }
        }

        redundant
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combineperkey_recommendation() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(TransformNode {
            id: "node_1".to_string(),
            name: "GroupByKey".to_string(),
            node_type: TransformType::GroupByKey { key_expr: None },
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        });

        let analyzer = BestPracticesAnalyzer;
        let result = analyzer.analyze(&ir).unwrap();
        assert!(result.findings.iter().any(|f| f.id == "BESTPRAC_PREFER_COMBINEPERKEY"));
    }
}
