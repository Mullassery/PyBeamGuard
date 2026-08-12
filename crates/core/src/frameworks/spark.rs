use super::{Framework, FrameworkParser};
use crate::ir::{Edge, PipelineIR, TransformNode, TransformType};
use anyhow::{anyhow, Result};
use regex::Regex;

/// Regex-based parser for PySpark (Structured Streaming and batch DataFrame)
/// source. Mirrors `BeamPipelineParser`'s line/whole-file regex approach but
/// extracts Spark-specific configuration -- shuffle partitioning, join
/// strategy/broadcast config, and streaming checkpoint/trigger/output-mode
/// setup -- into structured `TransformNode`s so the Spark analyzers
/// (`SparkShuffleAnalyzer`, `SparkJoinAnalyzer`, `SparkStreamingAnalyzer`)
/// can reason about real configuration instead of raw occurrence counts.
pub struct SparkFrameworkParser;

struct SparkPatterns {
    spark_import_re: Regex,
    shuffle_partitions_re: Regex,
    wide_transform_re: Regex,
    broadcast_call_re: Regex,
    broadcast_threshold_re: Regex,
    join_re: Regex,
    join_on_kw_re: Regex,
    write_stream_re: Regex,
    checkpoint_location_re: Regex,
    cache_re: Regex,
    unpersist_re: Regex,
    trigger_re: Regex,
    output_mode_re: Regex,
    groupby_re: Regex,
    watermark_re: Regex,
}

impl SparkPatterns {
    fn new() -> Self {
        Self {
            spark_import_re: Regex::new(r"pyspark|SparkSession|SparkContext").unwrap(),
            shuffle_partitions_re: Regex::new(
                r#"shuffle\.partitions["']?\s*,\s*["']?(\d+)"#,
            )
            .unwrap(),
            wide_transform_re: Regex::new(
                r"\.groupBy\(|\.join\(|\.distinct\(|\.repartition\(|\.coalesce\(|\.orderBy\(|\.sort\(",
            )
            .unwrap(),
            broadcast_call_re: Regex::new(r#"broadcast\(|\.hint\(\s*["']broadcast["']"#).unwrap(),
            broadcast_threshold_re: Regex::new(
                r#"autoBroadcastJoinThreshold["']?\s*,\s*["']?(-?\d+)"#,
            )
            .unwrap(),
            join_re: Regex::new(r"\.join\(").unwrap(),
            join_on_kw_re: Regex::new(r#"on\s*=\s*["']([^"']+)["']"#).unwrap(),
            write_stream_re: Regex::new(r"\.writeStream").unwrap(),
            checkpoint_location_re: Regex::new(r#"checkpointLocation"#).unwrap(),
            cache_re: Regex::new(r"\.cache\(\)|\.persist\(").unwrap(),
            unpersist_re: Regex::new(r"\.unpersist\(").unwrap(),
            trigger_re: Regex::new(r"\.trigger\(").unwrap(),
            output_mode_re: Regex::new(
                r#"outputMode\(\s*["'](append|update|complete)["']"#,
            )
            .unwrap(),
            groupby_re: Regex::new(r"\.groupBy\(").unwrap(),
            watermark_re: Regex::new(r"\.withWatermark\(").unwrap(),
        }
    }
}

impl SparkFrameworkParser {
    pub fn new() -> Self {
        Self
    }

    fn generate_id(&self, name: &str, seq: usize) -> String {
        format!("node_{}_{}", name.to_lowercase().replace(' ', "_"), seq)
    }

    fn extract_shuffle_config(&self, code: &str, patterns: &SparkPatterns, ir: &mut PipelineIR) {
        let shuffle_partitions: Option<u64> = patterns
            .shuffle_partitions_re
            .captures(code)
            .and_then(|c| c.get(1))
            .and_then(|m| m.as_str().parse().ok());

        ir.nodes.push(TransformNode {
            id: self.generate_id("spark_shuffle_config", 0),
            name: "ShuffleConfig".to_string(),
            node_type: TransformType::Custom("spark_shuffle_config".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "shuffle_partitions": shuffle_partitions }),
            annotations: vec![],
            line_number: None,
        });
    }

    fn extract_wide_transforms(&self, code: &str, patterns: &SparkPatterns, ir: &mut PipelineIR) {
        for (line_num, line) in code.lines().enumerate() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            let Some(m) = patterns.wide_transform_re.find(line) else {
                continue;
            };
            let op = match m.as_str() {
                s if s.contains("groupBy") => "groupby",
                s if s.contains("join") => "join",
                s if s.contains("distinct") => "distinct",
                s if s.contains("repartition") => "repartition",
                s if s.contains("coalesce") => "coalesce",
                s if s.contains("orderBy") => "orderby",
                s if s.contains("sort") => "sort",
                _ => "unknown",
            };

            ir.nodes.push(TransformNode {
                id: self.generate_id("spark_wide_transform", line_num),
                name: format!("WideTransform_{}", op),
                node_type: TransformType::Custom("spark_wide_transform".to_string()),
                inputs: vec![],
                outputs: vec![],
                config: serde_json::json!({ "op": op }),
                annotations: vec![],
                line_number: Some(line_num),
            });
        }
    }

    fn extract_joins(&self, code: &str, patterns: &SparkPatterns, ir: &mut PipelineIR) {
        for (line_num, line) in code.lines().enumerate() {
            if !patterns.join_re.is_match(line) {
                continue;
            }
            let key_expr = patterns
                .join_on_kw_re
                .captures(line)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_string());
            let is_broadcast = patterns.broadcast_call_re.is_match(line);

            ir.nodes.push(TransformNode {
                id: self.generate_id("spark_join", line_num),
                name: "Join".to_string(),
                node_type: TransformType::Custom("spark_join".to_string()),
                inputs: vec![],
                outputs: vec![],
                config: serde_json::json!({ "key_expr": key_expr, "is_broadcast": is_broadcast }),
                annotations: vec![],
                line_number: Some(line_num),
            });
        }
    }

    fn extract_broadcast_threshold(
        &self,
        code: &str,
        patterns: &SparkPatterns,
        ir: &mut PipelineIR,
    ) {
        let Some(caps) = patterns.broadcast_threshold_re.captures(code) else {
            return;
        };
        let threshold_bytes: Option<i64> = caps.get(1).and_then(|m| m.as_str().parse().ok());

        ir.nodes.push(TransformNode {
            id: self.generate_id("spark_broadcast_threshold", 0),
            name: "BroadcastThresholdConfig".to_string(),
            node_type: TransformType::Custom("spark_broadcast_threshold_config".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({ "threshold_bytes": threshold_bytes }),
            annotations: vec![],
            line_number: None,
        });
    }

    fn extract_streaming_query(&self, code: &str, patterns: &SparkPatterns, ir: &mut PipelineIR) {
        if !patterns.write_stream_re.is_match(code) {
            return;
        }

        let has_checkpoint_location = patterns.checkpoint_location_re.is_match(code);
        let trigger_configured = patterns.trigger_re.is_match(code);
        let output_mode = patterns
            .output_mode_re
            .captures(code)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string());
        let has_aggregation = patterns.groupby_re.is_match(code);
        let has_watermark = patterns.watermark_re.is_match(code);

        ir.nodes.push(TransformNode {
            id: self.generate_id("spark_streaming_query", 0),
            name: "StreamingQuery".to_string(),
            node_type: TransformType::Custom("spark_streaming_query".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({
                "has_checkpoint_location": has_checkpoint_location,
                "trigger_configured": trigger_configured,
                "output_mode": output_mode,
                "has_aggregation": has_aggregation,
                "has_watermark": has_watermark,
            }),
            annotations: vec![],
            line_number: None,
        });
    }

    fn extract_cache_calls(&self, code: &str, patterns: &SparkPatterns, ir: &mut PipelineIR) {
        let cache_count = patterns.cache_re.find_iter(code).count();
        if cache_count == 0 {
            return;
        }
        let has_unpersist = patterns.unpersist_re.is_match(code);

        ir.nodes.push(TransformNode {
            id: self.generate_id("spark_cache_usage", 0),
            name: "CacheUsage".to_string(),
            node_type: TransformType::Custom("spark_cache_usage".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({
                "cache_call_count": cache_count,
                "has_unpersist": has_unpersist,
            }),
            annotations: vec![],
            line_number: None,
        });
    }
}

impl FrameworkParser for SparkFrameworkParser {
    fn parse(&self, code: &str) -> Result<PipelineIR> {
        let patterns = SparkPatterns::new();

        if !patterns.spark_import_re.is_match(code) {
            return Err(anyhow!(
                "No PySpark constructs (pyspark import, SparkSession, or SparkContext) detected in code"
            ));
        }

        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());

        self.extract_shuffle_config(code, &patterns, &mut ir);
        self.extract_wide_transforms(code, &patterns, &mut ir);
        self.extract_joins(code, &patterns, &mut ir);
        self.extract_broadcast_threshold(code, &patterns, &mut ir);
        self.extract_streaming_query(code, &patterns, &mut ir);
        self.extract_cache_calls(code, &patterns, &mut ir);

        let node_ids: Vec<String> = ir.nodes.iter().map(|n| n.id.clone()).collect();
        for i in 0..node_ids.len().saturating_sub(1) {
            ir.edges.push(Edge {
                from: node_ids[i].clone(),
                to: node_ids[i + 1].clone(),
                pcollection_name: format!("pcol_{}", i),
            });
        }

        let source_count = Regex::new(r"readStream|kafka|socket|rate")
            .ok()
            .map(|re| re.find_iter(code).count())
            .unwrap_or(0);

        ir.metadata
            .runner_hints
            .insert("framework".to_string(), "spark".to_string());
        ir.metadata
            .runner_hints
            .insert("sources".to_string(), source_count.to_string());
        ir.metadata.runner_hints.insert(
            "is_streaming".to_string(),
            patterns.write_stream_re.is_match(code).to_string(),
        );

        Ok(ir)
    }

    fn framework(&self) -> Framework {
        Framework::ApacheSpark
    }
}

impl Default for SparkFrameworkParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REALISTIC_SPARK_PIPELINE: &str = r#"
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("orders").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

orders = spark.readStream.format("kafka").option("subscribe", "orders").load()
customers = spark.read.format("jdbc").option("dbtable", "customers").load()

joined = orders.join(customers, on="customer_id")
grouped = joined.groupBy("customer_id").count()

query = (
    grouped.writeStream
    .format("console")
    .outputMode("append")
    .start()
)
"#;

    #[test]
    fn test_spark_parser_rejects_non_spark_code() {
        let parser = SparkFrameworkParser;
        let result = parser.parse("print('hello world')");
        assert!(result.is_err());
    }

    #[test]
    fn test_spark_parser_extracts_shuffle_partitions() {
        let parser = SparkFrameworkParser;
        let ir = parser.parse(REALISTIC_SPARK_PIPELINE).unwrap();

        let node = ir
            .nodes
            .iter()
            .find(
                |n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_shuffle_config"),
            )
            .expect("shuffle config node");
        assert_eq!(node.config["shuffle_partitions"], 4);
    }

    #[test]
    fn test_spark_parser_extracts_broadcast_threshold() {
        let parser = SparkFrameworkParser;
        let ir = parser.parse(REALISTIC_SPARK_PIPELINE).unwrap();

        let node = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_broadcast_threshold_config"))
            .expect("broadcast threshold node");
        assert_eq!(node.config["threshold_bytes"], -1);
    }

    #[test]
    fn test_spark_parser_extracts_join_key() {
        let parser = SparkFrameworkParser;
        let ir = parser.parse(REALISTIC_SPARK_PIPELINE).unwrap();

        let node = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_join"))
            .expect("join node");
        assert_eq!(node.config["key_expr"], "customer_id");
    }

    #[test]
    fn test_spark_parser_extracts_streaming_query_without_checkpoint() {
        let parser = SparkFrameworkParser;
        let ir = parser.parse(REALISTIC_SPARK_PIPELINE).unwrap();

        let node = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "spark_streaming_query"))
            .expect("streaming query node");
        assert_eq!(node.config["has_checkpoint_location"], false);
        assert_eq!(node.config["output_mode"], "append");
        assert_eq!(node.config["has_aggregation"], true);
    }

    #[test]
    fn test_spark_parser_extracts_wide_transforms() {
        let parser = SparkFrameworkParser;
        let ir = parser.parse(REALISTIC_SPARK_PIPELINE).unwrap();

        let ops: Vec<&str> = ir
            .nodes
            .iter()
            .filter_map(|n| match &n.node_type {
                TransformType::Custom(t) if t == "spark_wide_transform" => n.config["op"].as_str(),
                _ => None,
            })
            .collect();
        assert!(ops.contains(&"join"));
        assert!(ops.contains(&"groupby"));
    }
}
