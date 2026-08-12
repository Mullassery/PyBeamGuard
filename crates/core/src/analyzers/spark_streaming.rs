use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

/// Analyzes Spark Structured Streaming checkpoint/trigger/output-mode setup
/// and DataFrame cache/persist lifecycle. The Spark analog of Beam's
/// `WindowingAnalyzer`/`StateAnalyzer`: it looks for the same class of
/// "will this actually run correctly / recover cleanly" issues, specific to
/// Spark's execution model.
pub struct SparkStreamingAnalyzer;

impl Analyzer for SparkStreamingAnalyzer {
    fn name(&self) -> &str {
        "SparkStreamingAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        32
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let streaming_query = ir.nodes.iter().find(
            |n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_streaming_query"),
        );

        if let Some(node) = streaming_query {
            self.analyze_streaming_query(node, &mut findings, &mut metrics);
        } else {
            metrics.insert("is_streaming".to_string(), 0.0);
        }

        let cache_usage = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_cache_usage"));
        if let Some(node) = cache_usage {
            self.analyze_cache_usage(node, &mut findings, &mut metrics);
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings: findings.clone(),
            metrics,
            summary: if findings.is_empty() {
                "Streaming/caching configuration looks reasonable".to_string()
            } else {
                format!(
                    "Found {} streaming/caching configuration issue(s)",
                    findings.len()
                )
            },
            confidence: 0.78,
        })
    }
}

impl SparkStreamingAnalyzer {
    fn analyze_streaming_query(
        &self,
        node: &TransformNode,
        findings: &mut Vec<Finding>,
        metrics: &mut HashMap<String, f64>,
    ) {
        metrics.insert("is_streaming".to_string(), 1.0);

        let has_checkpoint_location = node
            .config
            .get("has_checkpoint_location")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let trigger_configured = node
            .config
            .get("trigger_configured")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let output_mode = node.config.get("output_mode").and_then(|v| v.as_str());
        let has_aggregation = node
            .config
            .get("has_aggregation")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let has_watermark = node
            .config
            .get("has_watermark")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        metrics.insert(
            "checkpoint_location_configured".to_string(),
            if has_checkpoint_location { 1.0 } else { 0.0 },
        );

        if !has_checkpoint_location {
            findings.push(Finding {
                id: "SPARK_STREAMING_NO_CHECKPOINT_LOCATION".to_string(),
                severity: RiskSeverity::Critical,
                finding_type: FindingType::ReliabilityRisk,
                title: "Streaming query has no checkpointLocation".to_string(),
                description: "This writeStream query has no `.option(\"checkpointLocation\", ...)`. Without a checkpoint location, Spark cannot persist source offsets or state store data, so a restart re-reads from the source's default starting point and can duplicate or drop data.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Add `.option(\"checkpointLocation\", \"<durable-path>\")` to every writeStream query -- this is required for exactly-once/at-least-once recovery guarantees.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: None,
                    affected_records_percent: Some(100.0),
                }),
                confidence: 0.85,
            });
        }

        if !trigger_configured {
            findings.push(Finding {
                id: "SPARK_STREAMING_NO_TRIGGER".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::PerformanceRisk,
                title: "No explicit trigger configured".to_string(),
                description: "Without `.trigger(...)`, Spark uses the default micro-batch trigger, which starts a new batch as soon as the previous one finishes. For high-throughput sources this can starve cluster resources with back-to-back micro-batches and makes batch cadence unpredictable.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Set an explicit trigger, e.g. `.trigger(processingTime=\"30 seconds\")`, sized to your latency/throughput tradeoff.".to_string()),
                estimated_impact: None,
                confidence: 0.55,
            });
        }

        // Spark rejects `outputMode("append")` for aggregation queries
        // without a watermark at *run time* (AnalysisException); this is a
        // real, well-known Structured Streaming pitfall, not a stylistic
        // preference.
        let effective_mode = output_mode.unwrap_or("append");
        if has_aggregation && effective_mode == "append" && !has_watermark {
            findings.push(Finding {
                id: "SPARK_AGGREGATION_APPEND_MODE_INCOMPATIBLE".to_string(),
                severity: RiskSeverity::High,
                finding_type: FindingType::ReliabilityRisk,
                title: "Streaming aggregation with append output mode and no watermark".to_string(),
                description: "This query aggregates (groupBy) over a stream, uses (or defaults to) outputMode(\"append\"), and has no `.withWatermark(...)`. Spark Structured Streaming does not support append mode for aggregations unless a watermark bounds when a group is considered final -- this combination fails at query start with an AnalysisException.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Either add `.withWatermark(\"event_time\", \"<threshold>\")` before the aggregation and keep append mode, or switch to outputMode(\"update\")/outputMode(\"complete\") if you don't need append semantics.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: None,
                    affected_records_percent: Some(100.0),
                }),
                confidence: 0.80,
            });
        } else if output_mode.is_none() {
            findings.push(Finding {
                id: "SPARK_STREAMING_NO_OUTPUT_MODE".to_string(),
                severity: RiskSeverity::Low,
                finding_type: FindingType::ConfigurationIssue,
                title: "No explicit output mode configured".to_string(),
                description: "Without `.outputMode(...)`, Spark defaults to \"append\". Relying on the implicit default makes it easy to end up with the wrong semantics after the query is later changed to include an aggregation.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Set the output mode explicitly: append, update, or complete, based on the query's semantics.".to_string()),
                estimated_impact: None,
                confidence: 0.50,
            });
        }
    }

    fn analyze_cache_usage(
        &self,
        node: &TransformNode,
        findings: &mut Vec<Finding>,
        metrics: &mut HashMap<String, f64>,
    ) {
        let cache_call_count = node
            .config
            .get("cache_call_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let has_unpersist = node
            .config
            .get("has_unpersist")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        metrics.insert("cache_call_count".to_string(), cache_call_count as f64);

        if cache_call_count > 0 && !has_unpersist {
            findings.push(Finding {
                id: "SPARK_CACHE_WITHOUT_UNPERSIST".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::ReliabilityRisk,
                title: format!(
                    "{} cache()/persist() call(s) with no matching unpersist()",
                    cache_call_count
                ),
                description: "Cached/persisted DataFrames stay in executor memory/disk until explicitly unpersisted or evicted by LRU. In a long-running or streaming job, repeatedly caching new DataFrames without releasing old ones accumulates storage pressure over time.".to_string(),
                affected_nodes: vec![node.id.clone()],
                recommendation: Some("Call `.unpersist()` once a cached DataFrame is no longer needed, or use a `with`-style scope/context to tie its lifetime to the code that needs it.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: Some(150.0),
                    affected_records_percent: None,
                }),
                confidence: 0.55,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn streaming_node(config: serde_json::Value) -> TransformNode {
        TransformNode {
            id: "node_spark_streaming_query_0".to_string(),
            name: "StreamingQuery".to_string(),
            node_type: TransformType::Custom("spark_streaming_query".to_string()),
            inputs: vec![],
            outputs: vec![],
            config,
            annotations: vec![],
            line_number: None,
        }
    }

    fn cache_node(count: u64, has_unpersist: bool) -> TransformNode {
        TransformNode {
            id: "node_spark_cache_usage_0".to_string(),
            name: "CacheUsage".to_string(),
            node_type: TransformType::Custom("spark_cache_usage".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "cache_call_count": count, "has_unpersist": has_unpersist }),
            annotations: vec![],
            line_number: None,
        }
    }

    #[test]
    fn test_missing_checkpoint_location_is_critical() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(streaming_node(serde_json::json!({
            "has_checkpoint_location": false,
            "trigger_configured": true,
            "output_mode": "update",
            "has_aggregation": false,
            "has_watermark": false,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkStreamingAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_STREAMING_NO_CHECKPOINT_LOCATION"
                && f.severity == RiskSeverity::Critical));
    }

    #[test]
    fn test_aggregation_append_mode_without_watermark_flagged_high() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(streaming_node(serde_json::json!({
            "has_checkpoint_location": true,
            "trigger_configured": true,
            "output_mode": "append",
            "has_aggregation": true,
            "has_watermark": false,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkStreamingAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_AGGREGATION_APPEND_MODE_INCOMPATIBLE"
                && f.severity == RiskSeverity::High));
    }

    #[test]
    fn test_aggregation_with_watermark_is_clean() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(streaming_node(serde_json::json!({
            "has_checkpoint_location": true,
            "trigger_configured": true,
            "output_mode": "append",
            "has_aggregation": true,
            "has_watermark": true,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkStreamingAnalyzer.analyze(&ctx).unwrap();
        assert!(!result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_AGGREGATION_APPEND_MODE_INCOMPATIBLE"));
    }

    #[test]
    fn test_no_trigger_flagged_medium() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(streaming_node(serde_json::json!({
            "has_checkpoint_location": true,
            "trigger_configured": false,
            "output_mode": "update",
            "has_aggregation": false,
            "has_watermark": false,
        })));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkStreamingAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_STREAMING_NO_TRIGGER"));
    }

    #[test]
    fn test_cache_without_unpersist_flagged() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(cache_node(2, false));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkStreamingAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_CACHE_WITHOUT_UNPERSIST"));
    }

    #[test]
    fn test_cache_with_unpersist_is_clean() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(cache_node(2, true));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkStreamingAnalyzer.analyze(&ctx).unwrap();
        assert!(!result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_CACHE_WITHOUT_UNPERSIST"));
    }

    #[test]
    fn test_batch_pipeline_without_streaming_query_has_no_findings() {
        let ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkStreamingAnalyzer.analyze(&ctx).unwrap();
        assert!(result.findings.is_empty());
    }
}
