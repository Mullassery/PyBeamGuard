use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

/// Beyond this many seconds of allowed out-of-orderness, windows are held
/// open long enough to noticeably delay results for most near-real-time
/// use cases.
const EXCESSIVE_OUT_OF_ORDERNESS_SEC: u64 = 3_600; // 1 hour

/// Analyzes Flink watermark strategy and event-time windowing
/// configuration: whether a watermark strategy is present at all on a
/// keyed/windowed stream, whether out-of-orderness bounds are reasonable,
/// and whether windowing exists on keyed streams. The Flink analog of the
/// Beam `WindowingAnalyzer`.
pub struct FlinkWatermarkAnalyzer;

impl Analyzer for FlinkWatermarkAnalyzer {
    fn name(&self) -> &str {
        "FlinkWatermarkAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        22
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let has_keyed_state = ir
            .nodes
            .iter()
            .any(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_keyed_state"));
        let window_nodes: Vec<_> = ir
            .nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::Windowing { .. }))
            .collect();
        let watermark_node = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_watermark"));

        metrics.insert("window_count".to_string(), window_nodes.len() as f64);
        metrics.insert(
            "watermark_configured".to_string(),
            if watermark_node.is_some() { 1.0 } else { 0.0 },
        );

        let is_event_time = window_nodes
            .iter()
            .any(|n| n.config.get("time_domain").and_then(|v| v.as_str()) != Some("processing"))
            || watermark_node.is_some();

        if is_event_time && watermark_node.is_none() {
            findings.push(Finding {
                id: "FLINK_WATERMARK_MISSING".to_string(),
                severity: RiskSeverity::High,
                finding_type: FindingType::ReliabilityRisk,
                title: "Event-time windowing without a watermark strategy".to_string(),
                description: "Event-time windows are detected but no `WatermarkStrategy` (forBoundedOutOfOrderness/forMonotonousTimestamps + assign_timestamps_and_watermarks) was found. Without watermarks, Flink has no signal for when a window is complete, so event-time windows may never fire.".to_string(),
                affected_nodes: window_nodes.iter().map(|n| n.id.clone()).collect(),
                recommendation: Some("Assign a watermark strategy on the source stream: `ds.assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(N)))`.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: None,
                    affected_records_percent: Some(100.0),
                }),
                confidence: 0.78,
            });
        }

        if let Some(node) = watermark_node {
            if let Some(sec) = node
                .config
                .get("out_of_orderness_sec")
                .and_then(|v| v.as_u64())
            {
                if sec > EXCESSIVE_OUT_OF_ORDERNESS_SEC {
                    findings.push(Finding {
                        id: "FLINK_WATERMARK_EXCESSIVE_OUT_OF_ORDERNESS".to_string(),
                        severity: RiskSeverity::Medium,
                        finding_type: FindingType::PerformanceRisk,
                        title: format!(
                            "Bounded out-of-orderness is very large ({} minutes)",
                            sec / 60
                        ),
                        description: "A large out-of-orderness bound delays the watermark, which delays every downstream event-time window from firing. Results lag proportionally.".to_string(),
                        affected_nodes: vec![node.id.clone()],
                        recommendation: Some("Tighten the bound to the smallest value that matches your source's real late-arrival distribution, and use `allowedLateness`/a side-output for the rare outliers instead of a huge global bound.".to_string()),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(1.5),
                            cost_delta_monthly: None,
                            affected_records_percent: None,
                        }),
                        confidence: 0.65,
                    });
                }
            }
        }

        if has_keyed_state && window_nodes.is_empty() {
            findings.push(Finding {
                id: "FLINK_KEYED_STREAM_NO_WINDOW".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::ReliabilityRisk,
                title: "Keyed stream with no windowing detected".to_string(),
                description: "A `key_by(...)` was found with no `.window(...)` downstream. Keyed process functions without windowing accumulate state indefinitely unless the DoFn manages its own timers/cleanup -- easy to get wrong and easy to miss in review.".to_string(),
                affected_nodes: vec![],
                recommendation: Some("Either add explicit windowing, or confirm the KeyedProcessFunction registers cleanup timers (state.clear() on a TTL/expiry timer).".to_string()),
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
                "Watermark and windowing configuration looks reasonable".to_string()
            } else {
                format!(
                    "Found {} watermark/windowing configuration issue(s)",
                    findings.len()
                )
            },
            confidence: 0.75,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window_node(window_type: &str, duration_sec: Option<u64>) -> TransformNode {
        window_node_with_domain(window_type, duration_sec, "event")
    }

    fn window_node_with_domain(
        window_type: &str,
        duration_sec: Option<u64>,
        time_domain: &str,
    ) -> TransformNode {
        TransformNode {
            id: "node_flink_window_1".to_string(),
            name: "Window".to_string(),
            node_type: TransformType::Windowing {
                window_type: window_type.to_string(),
                duration_sec,
                trigger: None,
                allowed_lateness_sec: None,
                accumulation_mode: None,
            },
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "time_domain": time_domain }),
            annotations: vec![],
            line_number: Some(1),
        }
    }

    fn watermark_node(strategy: &str, out_of_orderness_sec: Option<u64>) -> TransformNode {
        TransformNode {
            id: "node_flink_watermark_0".to_string(),
            name: "WatermarkStrategy".to_string(),
            node_type: TransformType::Custom("flink_watermark".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({
                "strategy": strategy,
                "out_of_orderness_sec": out_of_orderness_sec,
            }),
            annotations: vec![],
            line_number: Some(0),
        }
    }

    #[test]
    fn test_event_time_window_without_watermark_flagged_high() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(window_node("tumbling", Some(60)));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkWatermarkAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_WATERMARK_MISSING" && f.severity == RiskSeverity::High));
    }

    #[test]
    fn test_window_with_watermark_no_missing_finding() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(window_node("tumbling", Some(60)));
        ir.nodes
            .push(watermark_node("bounded_out_of_orderness", Some(30)));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkWatermarkAnalyzer.analyze(&ctx).unwrap();
        assert!(!result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_WATERMARK_MISSING"));
    }

    #[test]
    fn test_excessive_out_of_orderness_flagged() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(window_node("tumbling", Some(60)));
        ir.nodes
            .push(watermark_node("bounded_out_of_orderness", Some(7_200)));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkWatermarkAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_WATERMARK_EXCESSIVE_OUT_OF_ORDERNESS"));
    }

    #[test]
    fn test_processing_time_window_does_not_require_watermark() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes
            .push(window_node_with_domain("tumbling", Some(60), "processing"));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkWatermarkAnalyzer.analyze(&ctx).unwrap();
        assert!(!result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_WATERMARK_MISSING"));
    }

    #[test]
    fn test_keyed_stream_without_window_flagged() {
        let mut ir = PipelineIR::new("flink_pipeline".to_string());
        ir.nodes.push(TransformNode {
            id: "node_flink_keyed_state_1".to_string(),
            name: "KeyBy".to_string(),
            node_type: TransformType::Custom("flink_keyed_state".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "key_expr": "id" }),
            annotations: vec![],
            line_number: Some(1),
        });

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = FlinkWatermarkAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "FLINK_KEYED_STREAM_NO_WINDOW"));
    }
}
