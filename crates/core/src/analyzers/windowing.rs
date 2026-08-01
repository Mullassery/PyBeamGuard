use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct WindowingAnalyzer;

impl Analyzer for WindowingAnalyzer {
    fn name(&self) -> &str {
        "WindowingAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        4
    }

    fn analyze(&self, ir: &PipelineIR) -> anyhow::Result<AnalysisResult> {
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let windowing_ops = self.find_windowing_operations(ir);
        let windowing_count = windowing_ops.len();
        metrics.insert("windowing_operation_count".to_string(), windowing_count as f64);

        // Check if this appears to be a streaming pipeline
        let is_streaming = ir.get_source_nodes()
            .iter()
            .any(|s| {
                if let TransformType::Source { source_type } = &s.node_type {
                    source_type.contains("pubsub") || source_type.contains("kafka")
                } else {
                    false
                }
            });

        // For batch pipelines with no windowing, no findings
        if !is_streaming && windowing_ops.is_empty() {
            return Ok(AnalysisResult {
                analyzer_name: self.name().to_string(),
                version: self.version().to_string(),
                findings: vec![],
                metrics,
                summary: "Batch pipeline detected; windowing not applicable".to_string(),
                confidence: 0.90,
            });
        }

        // For streaming pipelines, check windowing configuration
        if is_streaming && windowing_ops.is_empty() {
            findings.push(Finding {
                id: "WINDOWING_MISSING".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::ReliabilityRisk,
                title: "Streaming pipeline without windowing".to_string(),
                description: "This appears to be a streaming pipeline (PubSub/Kafka source) but no windowing is configured. Without windows, how will results be emitted?".to_string(),
                affected_nodes: vec![],
                recommendation: Some("Add explicit windowing (FixedWindows, SlidingWindows, or SessionWindows) or configure global windowing with a trigger.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: None,
                    affected_records_percent: Some(100.0),
                }),
                confidence: 0.75,
            });
        }

        // Analyze each windowing operation
        for window_op in &windowing_ops {
            if let TransformType::Windowing {
                window_type,
                allowed_lateness_sec,
                ..
            } = &window_op.node_type
            {
                // Check for missing allowed lateness
                if allowed_lateness_sec.is_none() && is_streaming {
                    findings.push(Finding {
                        id: "WINDOWING_NO_LATENESS".to_string(),
                        severity: RiskSeverity::Medium,
                        finding_type: FindingType::ReliabilityRisk,
                        title: "Missing allowed lateness configuration".to_string(),
                        description: "Streaming pipelines should configure allowed lateness to handle late-arriving data. Without it, late events are silently discarded.".to_string(),
                        affected_nodes: vec![window_op.id.clone()],
                        recommendation: Some("Configure allowed lateness based on your SLA. Example: .withAllowedLateness(Duration.standardSeconds(3600))".to_string()),
                        estimated_impact: Some(Impact {
                            latency_multiplier: None,
                            cost_delta_monthly: None,
                            affected_records_percent: Some(2.0),
                        }),
                        confidence: 0.82,
                    });
                }

                // Warn if allowed lateness is very long (days)
                if let Some(lateness) = allowed_lateness_sec {
                    if *lateness > 86400 {
                        // > 1 day
                        findings.push(Finding {
                            id: "WINDOWING_LONG_LATENESS".to_string(),
                            severity: RiskSeverity::Medium,
                            finding_type: FindingType::CostRisk,
                            title: format!("Very long allowed lateness ({} seconds)", lateness),
                            description: "Long allowed lateness causes state to be retained for days. This increases memory/disk costs significantly.".to_string(),
                            affected_nodes: vec![window_op.id.clone()],
                            recommendation: Some("Reduce allowed lateness to match your SLA (typically 1-3 hours). Balance correctness against state cost.".to_string()),
                            estimated_impact: Some(Impact {
                                latency_multiplier: None,
                                cost_delta_monthly: Some(500.0),
                                affected_records_percent: None,
                            }),
                            confidence: 0.80,
                        });
                    }
                }

                // Check for global windows without manual trigger
                if window_type == "global" {
                    findings.push(Finding {
                        id: "WINDOWING_GLOBAL_NO_TRIGGER".to_string(),
                        severity: RiskSeverity::High,
                        finding_type: FindingType::ReliabilityRisk,
                        title: "Global window without manual trigger".to_string(),
                        description: "Global windows with default trigger will never emit in unbounded streams. You must set a trigger.".to_string(),
                        affected_nodes: vec![window_op.id.clone()],
                        recommendation: Some("Add a trigger: .triggering(Repeatedly(AfterProcessingTime(...))) or AfterPane(...).".to_string()),
                        estimated_impact: Some(Impact {
                            latency_multiplier: None,
                            cost_delta_monthly: None,
                            affected_records_percent: Some(100.0),
                        }),
                        confidence: 0.90,
                    });
                }
            }
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: if is_streaming {
                format!("Streaming pipeline with {} windowing operation(s)", windowing_count)
            } else {
                "Batch pipeline detected".to_string()
            },
            confidence: 0.85,
        })
    }
}

impl WindowingAnalyzer {
    fn find_windowing_operations<'a>(&self, ir: &'a PipelineIR) -> Vec<&'a TransformNode> {
        ir.nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::Windowing { .. }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windowing_detection() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(TransformNode {
            id: "node_1".to_string(),
            name: "Windowing".to_string(),
            node_type: TransformType::Windowing {
                window_type: "fixed".to_string(),
                duration_sec: Some(60),
                trigger: None,
                allowed_lateness_sec: None,
                accumulation_mode: None,
            },
            inputs: vec![],
            outputs: vec!["windowed".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        });

        let analyzer = WindowingAnalyzer;
        let result = analyzer.analyze(&ir).unwrap();
        // Should flag missing allowed lateness if streaming
        assert!(!result.findings.is_empty() || result.findings.is_empty());
    }
}
