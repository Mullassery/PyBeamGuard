use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

/// Spark's built-in default for `spark.sql.shuffle.partitions`. Sized for a
/// generic cluster and rarely right for either very small or very large
/// workloads.
const DEFAULT_SHUFFLE_PARTITIONS: u64 = 200;
/// Below this, each shuffle partition after a wide transform on
/// non-trivial data tends to be large enough to risk executor OOM/spill and
/// leaves most of the cluster idle.
const TOO_FEW_PARTITIONS: u64 = 10;
/// Above this, per-task overhead (scheduling, small output files) starts to
/// dominate actual compute for most workloads.
const EXCESSIVE_PARTITIONS: u64 = 10_000;
/// Beyond this many wide (shuffle-triggering) transforms in one job, cost
/// and latency from repeated shuffles compound significantly -- the same
/// threshold `CostAnalyzer` uses for Beam's `COST_MULTIPLE_SHUFFLES`.
const MANY_WIDE_TRANSFORMS: usize = 3;

/// Analyzes Spark shuffle partition configuration and wide-transformation
/// usage (`groupBy`, `join`, `distinct`, `repartition`, `coalesce`,
/// `orderBy`/`sort`). The Spark analog of Beam's `ShuffleAnalyzer`.
pub struct SparkShuffleAnalyzer;

impl Analyzer for SparkShuffleAnalyzer {
    fn name(&self) -> &str {
        "SparkShuffleAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        30
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        let wide_transforms: Vec<_> = ir
            .nodes
            .iter()
            .filter(
                |n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_wide_transform"),
            )
            .collect();
        metrics.insert(
            "wide_transform_count".to_string(),
            wide_transforms.len() as f64,
        );

        let shuffle_config = ir.nodes.iter().find(
            |n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_shuffle_config"),
        );
        let shuffle_partitions = shuffle_config
            .and_then(|n| n.config.get("shuffle_partitions"))
            .and_then(|v| v.as_u64());

        if let Some(partitions) = shuffle_partitions {
            metrics.insert("shuffle_partitions".to_string(), partitions as f64);

            if !wide_transforms.is_empty() && partitions < TOO_FEW_PARTITIONS {
                findings.push(Finding {
                    id: "SPARK_SHUFFLE_PARTITIONS_TOO_LOW".to_string(),
                    severity: RiskSeverity::High,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!(
                        "spark.sql.shuffle.partitions is very low ({})",
                        partitions
                    ),
                    description: "With wide transformations (groupBy/join/...) present, this few shuffle partitions means each partition after the shuffle holds a large slice of data, risking executor spill/OOM and leaving most of the cluster's parallelism unused.".to_string(),
                    affected_nodes: wide_transforms.iter().map(|n| n.id.clone()).collect(),
                    recommendation: Some("Raise shuffle.partitions so post-shuffle partition size targets roughly 128-200MB each, or enable Adaptive Query Execution (`spark.sql.adaptive.enabled=true`) to let Spark coalesce partitions automatically.".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(2.0),
                        cost_delta_monthly: None,
                        affected_records_percent: None,
                    }),
                    confidence: 0.72,
                });
            } else if partitions > EXCESSIVE_PARTITIONS {
                findings.push(Finding {
                    id: "SPARK_SHUFFLE_PARTITIONS_EXCESSIVE".to_string(),
                    severity: RiskSeverity::Medium,
                    finding_type: FindingType::PerformanceRisk,
                    title: format!(
                        "spark.sql.shuffle.partitions is very high ({})",
                        partitions
                    ),
                    description: "An excessive partition count means many small tasks, each with fixed scheduling overhead, and can produce large numbers of small output files downstream.".to_string(),
                    affected_nodes: vec![],
                    recommendation: Some("Right-size shuffle.partitions to your data volume and cluster core count, or use AQE's coalescing instead of a large fixed value.".to_string()),
                    estimated_impact: Some(Impact {
                        latency_multiplier: Some(1.3),
                        cost_delta_monthly: None,
                        affected_records_percent: None,
                    }),
                    confidence: 0.65,
                });
            }
        } else if wide_transforms.len() >= 2 {
            findings.push(Finding {
                id: "SPARK_SHUFFLE_PARTITIONS_UNCONFIGURED".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::ConfigurationIssue,
                title: format!(
                    "spark.sql.shuffle.partitions left at the default ({})",
                    DEFAULT_SHUFFLE_PARTITIONS
                ),
                description: "Multiple wide transformations are present but `spark.sql.shuffle.partitions` is never set, so Spark uses its fixed default of 200 regardless of actual data volume or cluster size.".to_string(),
                affected_nodes: wide_transforms.iter().map(|n| n.id.clone()).collect(),
                recommendation: Some("Set spark.sql.shuffle.partitions explicitly based on data volume (target ~128-200MB per partition after shuffle), or enable Adaptive Query Execution to size partitions automatically.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(1.4),
                    cost_delta_monthly: None,
                    affected_records_percent: None,
                }),
                confidence: 0.60,
            });
        }

        if wide_transforms.len() > MANY_WIDE_TRANSFORMS {
            findings.push(Finding {
                id: "SPARK_MULTIPLE_WIDE_TRANSFORMS".to_string(),
                severity: RiskSeverity::High,
                finding_type: FindingType::CostRisk,
                title: format!(
                    "{} wide (shuffle-triggering) transformations detected",
                    wide_transforms.len()
                ),
                description: "Each groupBy/join/distinct/repartition triggers a full shuffle. Several of them in one job compound cost and latency, especially if they operate on the same large DataFrame repeatedly.".to_string(),
                affected_nodes: wide_transforms.iter().map(|n| n.id.clone()).collect(),
                recommendation: Some("Look for opportunities to combine transformations (e.g. a single groupBy with multiple aggregations instead of several passes), or cache an intermediate DataFrame that's reused across shuffles.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(1.8),
                    cost_delta_monthly: Some(400.0),
                    affected_records_percent: None,
                }),
                confidence: 0.70,
            });
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings: findings.clone(),
            metrics,
            summary: if wide_transforms.is_empty() {
                "No shuffle-triggering transformations detected".to_string()
            } else if findings.is_empty() {
                format!(
                    "{} wide transformation(s) detected; partitioning looks reasonable",
                    wide_transforms.len()
                )
            } else {
                format!("Found {} shuffle configuration issue(s)", findings.len())
            },
            confidence: 0.75,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wide_node(op: &str, seq: usize) -> TransformNode {
        TransformNode {
            id: format!("node_spark_wide_transform_{}", seq),
            name: format!("WideTransform_{}", op),
            node_type: TransformType::Custom("spark_wide_transform".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "op": op }),
            annotations: vec![],
            line_number: Some(seq),
        }
    }

    fn shuffle_config_node(partitions: Option<u64>) -> TransformNode {
        TransformNode {
            id: "node_spark_shuffle_config_0".to_string(),
            name: "ShuffleConfig".to_string(),
            node_type: TransformType::Custom("spark_shuffle_config".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "shuffle_partitions": partitions }),
            annotations: vec![],
            line_number: None,
        }
    }

    #[test]
    fn test_too_few_partitions_with_wide_transforms_flagged_high() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(shuffle_config_node(Some(2)));
        ir.nodes.push(wide_node("groupby", 1));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkShuffleAnalyzer.analyze(&ctx).unwrap();
        assert!(result.findings.iter().any(
            |f| f.id == "SPARK_SHUFFLE_PARTITIONS_TOO_LOW" && f.severity == RiskSeverity::High
        ));
    }

    #[test]
    fn test_excessive_partitions_flagged_medium() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(shuffle_config_node(Some(50_000)));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkShuffleAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_SHUFFLE_PARTITIONS_EXCESSIVE"));
    }

    #[test]
    fn test_unconfigured_partitions_with_multiple_wide_transforms_flagged() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(wide_node("groupby", 1));
        ir.nodes.push(wide_node("join", 2));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkShuffleAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_SHUFFLE_PARTITIONS_UNCONFIGURED"));
    }

    #[test]
    fn test_many_wide_transforms_flagged() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(shuffle_config_node(Some(200)));
        for i in 0..5 {
            ir.nodes.push(wide_node("join", i));
        }

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkShuffleAnalyzer.analyze(&ctx).unwrap();
        assert!(result
            .findings
            .iter()
            .any(|f| f.id == "SPARK_MULTIPLE_WIDE_TRANSFORMS"));
    }

    #[test]
    fn test_reasonable_config_has_no_findings() {
        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());
        ir.nodes.push(shuffle_config_node(Some(200)));
        ir.nodes.push(wide_node("groupby", 1));

        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = SparkShuffleAnalyzer.analyze(&ctx).unwrap();
        assert!(result.findings.is_empty());
    }
}
