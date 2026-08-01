use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct ReliabilityAnalyzer;

impl Analyzer for ReliabilityAnalyzer {
    fn name(&self) -> &str {
        "ReliabilityAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        7
    }

    fn analyze(&self, ir: &PipelineIR) -> anyhow::Result<AnalysisResult> {
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        // Check for error handling patterns
        let has_error_handling = self.check_error_handling(ir);
        metrics.insert("has_error_handling".to_string(), if has_error_handling { 1.0 } else { 0.0 });

        // Check for dead-letter queue
        let has_dlq = self.check_dead_letter_queue(ir);
        metrics.insert("has_dead_letter_queue".to_string(), if has_dlq { 1.0 } else { 0.0 });

        // Check for side outputs (alternate failure path)
        let _has_side_outputs = ir.nodes
            .iter()
            .any(|n| {
                if let TransformType::ParDo { has_side_outputs: true, .. } = n.node_type {
                    true
                } else {
                    false
                }
            });
        metrics.insert("has_side_outputs".to_string(), if _has_side_outputs { 1.0 } else { 0.0 });

        // Check for transformations that might silently fail
        for node in &ir.nodes {
            if let TransformType::ParDo { dofn_name, .. } = &node.node_type {
                if dofn_name.to_lowercase().contains("parse") ||
                   dofn_name.to_lowercase().contains("decode") ||
                   dofn_name.to_lowercase().contains("extract") {
                    if !has_error_handling && !_has_side_outputs {
                        findings.push(Finding {
                            id: "RELIABILITY_NO_ERROR_HANDLING".to_string(),
                            severity: RiskSeverity::High,
                            finding_type: FindingType::ReliabilityRisk,
                            title: format!("'{}' lacks error handling", dofn_name),
                            description: format!(
                                "Transform '{}' performs parsing/decoding but has no visible error handling. Malformed input will cause silent failures or exceptions.",
                                dofn_name
                            ),
                            affected_nodes: vec![node.id.clone()],
                            recommendation: Some(
                                "Implement one of:\n\
                                 1. Try-catch with side output for failed records\n\
                                 2. Dead-letter queue pattern\n\
                                 3. Graceful fallback value"
                                    .to_string()
                            ),
                            estimated_impact: Some(Impact {
                                latency_multiplier: None,
                                cost_delta_monthly: None,
                                affected_records_percent: Some(0.1),
                            }),
                            confidence: 0.85,
                        });
                    }
                }
            }
        }

        // Check for potential silent data loss
        let sink_count = ir.get_sink_nodes().len();
        if sink_count == 0 {
            findings.push(Finding {
                id: "RELIABILITY_NO_SINK".to_string(),
                severity: RiskSeverity::Critical,
                finding_type: FindingType::ReliabilityRisk,
                title: "Pipeline has no sink/output".to_string(),
                description: "The pipeline processes data but has no output. All data is discarded.".to_string(),
                affected_nodes: vec![],
                recommendation: Some("Add a sink operation (WriteToBigQuery, WriteToPubSub, etc.)".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: None,
                    affected_records_percent: Some(100.0),
                }),
                confidence: 0.99,
            });
        }

        // Check for multiple sinks (fan-out pattern)
        if sink_count > 1 {
            findings.push(Finding {
                id: "RELIABILITY_MULTIPLE_SINKS".to_string(),
                severity: RiskSeverity::Low,
                finding_type: FindingType::ConfigurationIssue,
                title: format!("Pipeline has {} sinks (fan-out)", sink_count),
                description: "Multiple sinks increase complexity. Ensure all outputs are coordinated.".to_string(),
                affected_nodes: ir.get_sink_nodes()
                    .iter()
                    .map(|n| n.id.clone())
                    .collect(),
                recommendation: None,
                estimated_impact: None,
                confidence: 0.80,
            });
        }

        // Calculate reliability score
        let mut reliability_issues = findings.len();
        if !has_error_handling {
            reliability_issues += 1;
        }
        if !has_dlq {
            reliability_issues += 1;
        }

        let reliability_score = if reliability_issues == 0 {
            100.0
        } else {
            (100.0 - (reliability_issues as f64 * 15.0)).max(0.0)
        };

        metrics.insert("reliability_score".to_string(), reliability_score);

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: format!(
                "Reliability score: {:.0}/100 | Error handling: {} | DLQ: {} | Sinks: {}",
                reliability_score,
                if has_error_handling { "✓" } else { "✗" },
                if has_dlq { "✓" } else { "✗" },
                sink_count
            ),
            confidence: 0.75,
        })
    }
}

impl ReliabilityAnalyzer {
    fn check_error_handling(&self, ir: &PipelineIR) -> bool {
        // Simple heuristic: if ParDo has side outputs or has error patterns in name
        ir.nodes.iter().any(|n| {
            if let TransformType::ParDo {
                has_side_outputs: true,
                ..
            } = n.node_type
            {
                true
            } else {
                false
            }
        })
    }

    fn check_dead_letter_queue(&self, ir: &PipelineIR) -> bool {
        // Check for sinks that might be dead-letter queues
        ir.get_sink_nodes()
            .iter()
            .any(|sink| {
                if let TransformType::Sink { .. } = &sink.node_type {
                    sink.name.to_lowercase().contains("dead") ||
                    sink.name.to_lowercase().contains("error") ||
                    sink.name.to_lowercase().contains("dlq")
                } else {
                    false
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reliability_no_sink() {
        let ir = PipelineIR::new("test".to_string());
        let analyzer = ReliabilityAnalyzer;
        let result = analyzer.analyze(&ir).unwrap();
        // Should flag missing sink
        assert!(result.findings.iter().any(|f| f.id == "RELIABILITY_NO_SINK"));
    }
}
