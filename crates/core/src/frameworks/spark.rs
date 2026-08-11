use super::{Framework, FrameworkParser};
use crate::ir::PipelineIR;
use anyhow::Result;
use regex::Regex;

pub struct SparkFrameworkParser;

impl FrameworkParser for SparkFrameworkParser {
    fn parse(&self, code: &str) -> Result<PipelineIR> {
        // Spark Structured Streaming parser (simplified regex-based)
        // Full implementation would parse Spark SQL and Python DataFrame API

        let mut ir = PipelineIR::new("spark_streaming_pipeline".to_string());

        // Detect stream sources
        let source_pattern = Regex::new(r"readStream|kafka|socket|rate")?;
        let source_count = source_pattern.find_iter(code).count();

        // Detect stateful operations (groupBy, window, etc.)
        let stateful_pattern = Regex::new(r"groupBy|window|stateStore")?;
        let stateful_count = stateful_pattern.find_iter(code).count();

        // Detect micro-batch intervals
        let trigger_pattern = Regex::new(r"trigger\s*\(\s*(?:Trigger|processingTime|once)")?;
        let has_trigger = trigger_pattern.is_match(code);

        // Detect output modes
        let output_mode_pattern = Regex::new(r"outputMode\s*\(\s*(?:append|complete|update)")?;
        let has_output_mode = output_mode_pattern.is_match(code);

        // Set metadata
        ir.metadata
            .runner_hints
            .insert("framework".to_string(), "spark".to_string());
        ir.metadata
            .runner_hints
            .insert("sources".to_string(), source_count.to_string());
        ir.metadata.runner_hints.insert(
            "has_stateful_ops".to_string(),
            (stateful_count > 0).to_string(),
        );

        if has_trigger {
            ir.metadata
                .runner_hints
                .insert("trigger_configured".to_string(), "true".to_string());
        }

        if has_output_mode {
            ir.metadata
                .runner_hints
                .insert("output_mode_configured".to_string(), "true".to_string());
        }

        Ok(ir)
    }

    fn framework(&self) -> Framework {
        Framework::ApacheSpark
    }
}

// Spark-specific analyzers would focus on:
// 1. Micro-batch interval optimization
// 2. Shuffle partitioning strategy
// 3. State store size and TTL
// 4. Output mode correctness (append vs. complete vs. update)
// 5. Checkpoint location and recovery
// 6. DataFrame join optimization (broadcast vs. sort-merge)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spark_parser() {
        let code = r#"
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.readStream.format("kafka").load()
result = df.groupBy("key").count()
result.writeStream.format("console").trigger(processingTime="10 seconds").start()
        "#;

        let parser = SparkFrameworkParser;
        let result = parser.parse(code);
        assert!(result.is_ok());
    }
}
