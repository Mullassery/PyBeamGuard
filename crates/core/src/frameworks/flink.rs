use super::{Framework, FrameworkParser};
use crate::ir::{Edge, PipelineIR, TransformNode, TransformType};
use anyhow::{anyhow, Result};
use regex::Regex;

/// Regex-based parser for PyFlink DataStream/Table API source. Mirrors
/// `BeamPipelineParser`'s approach (line/whole-file regex scanning, no AST)
/// but extracts Flink-specific configuration -- checkpointing, state
/// backend, watermark strategy, keyed state, and event-time windowing --
/// into structured `TransformNode`s so the Flink analyzers
/// (`FlinkCheckpointAnalyzer`, `FlinkStateAnalyzer`, `FlinkWatermarkAnalyzer`)
/// can reason about real configuration instead of raw occurrence counts.
pub struct FlinkFrameworkParser;

struct FlinkPatterns {
    flink_import_re: Regex,
    stateful_re: Regex,
    keyby_re: Regex,
    checkpoint_enable_re: Regex,
    checkpoint_mode_re: Regex,
    checkpoint_timeout_re: Regex,
    checkpoint_min_pause_re: Regex,
    state_backend_re: Regex,
    incremental_single_arg_re: Regex,
    incremental_two_arg_re: Regex,
    watermark_bounded_snake_re: Regex,
    watermark_bounded_camel_re: Regex,
    watermark_monotonic_re: Regex,
    window_type_re: Regex,
    window_duration_seconds_re: Regex,
    window_duration_minutes_re: Regex,
}

impl FlinkPatterns {
    fn new() -> Self {
        Self {
            flink_import_re: Regex::new(r"pyflink|StreamExecutionEnvironment").unwrap(),
            stateful_re: Regex::new(
                r"KeyedProcessFunction|ProcessWindowFunction|ValueState|ListState|MapState|ReducingState|AggregatingState|TimerService|Timer\b",
            )
            .unwrap(),
            keyby_re: Regex::new(r"\.key_by\(([^)]*)\)").unwrap(),
            checkpoint_enable_re: Regex::new(r"enable_checkpointing\(\s*(\d+)").unwrap(),
            checkpoint_mode_re: Regex::new(r"CheckpointingMode\.(EXACTLY_ONCE|AT_LEAST_ONCE)")
                .unwrap(),
            checkpoint_timeout_re: Regex::new(r"set_checkpoint_timeout\(\s*(\d+)").unwrap(),
            checkpoint_min_pause_re: Regex::new(r"set_min_pause_between_checkpoints\(\s*(\d+)")
                .unwrap(),
            state_backend_re: Regex::new(
                r"EmbeddedRocksDBStateBackend|RocksDBStateBackend|HashMapStateBackend|MemoryStateBackend|FsStateBackend",
            )
            .unwrap(),
            // Two forms in the wild: `EmbeddedRocksDBStateBackend(True)`
            // (single incremental-checkpoints bool arg) and the older
            // `RocksDBStateBackend(checkpoint_dir, True)` (path + bool).
            // Matched as two separate patterns instead of one with an
            // optional leading argument, since a greedy `[^,]*` there would
            // happily swallow the boolean itself before the capture group
            // gets a chance to match it.
            incremental_single_arg_re: Regex::new(
                r"RocksDBStateBackend\(\s*(True|False|true|false)\s*\)",
            )
            .unwrap(),
            incremental_two_arg_re: Regex::new(
                r"RocksDBStateBackend\([^,)]*,\s*(True|False|true|false)\s*\)",
            )
            .unwrap(),
            watermark_bounded_snake_re: Regex::new(
                r"for_bounded_out_of_orderness\(\s*Duration\.of_seconds\(\s*(\d+)",
            )
            .unwrap(),
            watermark_bounded_camel_re: Regex::new(
                r"forBoundedOutOfOrderness\(\s*Duration\.ofSeconds\(\s*(\d+)",
            )
            .unwrap(),
            watermark_monotonic_re: Regex::new(
                r"for_monotonous_timestamps\(|forMonotonousTimestamps\(",
            )
            .unwrap(),
            window_type_re: Regex::new(
                r"TumblingEventTimeWindows|SlidingEventTimeWindows|EventTimeSessionWindows|TumblingProcessingTimeWindows|SlidingProcessingTimeWindows|ProcessingTimeSessionWindows|SessionWindows",
            )
            .unwrap(),
            window_duration_seconds_re: Regex::new(r"Time\.seconds\(\s*(\d+)").unwrap(),
            window_duration_minutes_re: Regex::new(r"Time\.minutes\(\s*(\d+)").unwrap(),
        }
    }
}

impl FlinkFrameworkParser {
    pub fn new() -> Self {
        Self
    }

    fn generate_id(&self, name: &str, seq: usize) -> String {
        format!("node_{}_{}", name.to_lowercase().replace(' ', "_"), seq)
    }

    fn extract_checkpoint_config(&self, code: &str, patterns: &FlinkPatterns, ir: &mut PipelineIR) {
        let Some(enable_caps) = patterns.checkpoint_enable_re.captures(code) else {
            return;
        };
        let interval_ms: Option<u64> = enable_caps.get(1).and_then(|m| m.as_str().parse().ok());
        let mode = patterns
            .checkpoint_mode_re
            .captures(code)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string());
        let timeout_ms: Option<u64> = patterns
            .checkpoint_timeout_re
            .captures(code)
            .and_then(|c| c.get(1))
            .and_then(|m| m.as_str().parse().ok());
        let min_pause_ms: Option<u64> = patterns
            .checkpoint_min_pause_re
            .captures(code)
            .and_then(|c| c.get(1))
            .and_then(|m| m.as_str().parse().ok());

        let line_number = code
            .lines()
            .position(|l| patterns.checkpoint_enable_re.is_match(l));

        ir.nodes.push(TransformNode {
            id: self.generate_id("flink_checkpoint_config", 0),
            name: "CheckpointConfig".to_string(),
            node_type: TransformType::Custom("flink_checkpoint_config".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({
                "interval_ms": interval_ms,
                "mode": mode,
                "timeout_ms": timeout_ms,
                "min_pause_ms": min_pause_ms,
            }),
            annotations: vec![],
            line_number,
        });
    }

    fn extract_state_backend(&self, code: &str, patterns: &FlinkPatterns, ir: &mut PipelineIR) {
        let Some(backend_match) = patterns.state_backend_re.find(code) else {
            return;
        };
        let backend = backend_match.as_str().to_string();
        let incremental = if backend.contains("RocksDB") {
            patterns
                .incremental_single_arg_re
                .captures(code)
                .or_else(|| patterns.incremental_two_arg_re.captures(code))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_lowercase() == "true")
        } else {
            None
        };

        let line_number = code
            .lines()
            .position(|l| patterns.state_backend_re.is_match(l));

        ir.nodes.push(TransformNode {
            id: self.generate_id("flink_state_backend", 0),
            name: "StateBackend".to_string(),
            node_type: TransformType::Custom("flink_state_backend".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({
                "backend": backend,
                "incremental": incremental,
            }),
            annotations: vec![],
            line_number,
        });
    }

    fn extract_keyed_state(&self, code: &str, patterns: &FlinkPatterns, ir: &mut PipelineIR) {
        for (line_num, line) in code.lines().enumerate() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            if let Some(caps) = patterns.keyby_re.captures(line) {
                let key_expr = caps
                    .get(1)
                    .map(|m| m.as_str().trim().to_string())
                    .filter(|s| !s.is_empty());

                ir.nodes.push(TransformNode {
                    id: self.generate_id("flink_keyed_state", line_num),
                    name: "KeyBy".to_string(),
                    node_type: TransformType::Custom("flink_keyed_state".to_string()),
                    inputs: vec![],
                    outputs: vec!["keyed_stream".to_string()],
                    config: serde_json::json!({ "key_expr": key_expr }),
                    annotations: vec![],
                    line_number: Some(line_num),
                });
            }
        }
    }

    fn extract_watermark_strategy(
        &self,
        code: &str,
        patterns: &FlinkPatterns,
        ir: &mut PipelineIR,
    ) {
        let (strategy, out_of_orderness_sec) =
            if let Some(caps) = patterns.watermark_bounded_snake_re.captures(code) {
                (
                    "bounded_out_of_orderness",
                    caps.get(1).and_then(|m| m.as_str().parse::<u64>().ok()),
                )
            } else if let Some(caps) = patterns.watermark_bounded_camel_re.captures(code) {
                (
                    "bounded_out_of_orderness",
                    caps.get(1).and_then(|m| m.as_str().parse::<u64>().ok()),
                )
            } else if patterns.watermark_monotonic_re.is_match(code) {
                ("monotonous", None)
            } else {
                return;
            };

        ir.nodes.push(TransformNode {
            id: self.generate_id("flink_watermark", 0),
            name: "WatermarkStrategy".to_string(),
            node_type: TransformType::Custom("flink_watermark".to_string()),
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({
                "strategy": strategy,
                "out_of_orderness_sec": out_of_orderness_sec,
            }),
            annotations: vec![],
            line_number: None,
        });
    }

    fn extract_windowing(&self, code: &str, patterns: &FlinkPatterns, ir: &mut PipelineIR) {
        for (line_num, line) in code.lines().enumerate() {
            let Some(window_match) = patterns.window_type_re.find(line) else {
                continue;
            };
            let matched = window_match.as_str();
            let window_type = if matched.contains("Sliding") {
                "sliding"
            } else if matched.contains("Session") {
                "session"
            } else {
                "tumbling"
            };
            // Event-time windows require a watermark strategy to fire;
            // processing-time windows don't. Recorded separately from
            // `window_type` (which mirrors Beam's shape vocabulary) so
            // `FlinkWatermarkAnalyzer` can tell the two apart.
            let time_domain = if matched.contains("ProcessingTime") {
                "processing"
            } else {
                "event"
            };

            let duration_sec = patterns
                .window_duration_seconds_re
                .captures(line)
                .and_then(|c| c.get(1))
                .and_then(|m| m.as_str().parse::<u64>().ok())
                .or_else(|| {
                    patterns
                        .window_duration_minutes_re
                        .captures(line)
                        .and_then(|c| c.get(1))
                        .and_then(|m| m.as_str().parse::<u64>().ok())
                        .map(|min| min * 60)
                });

            ir.nodes.push(TransformNode {
                id: self.generate_id("flink_window", line_num),
                name: "Window".to_string(),
                node_type: TransformType::Windowing {
                    window_type: window_type.to_string(),
                    duration_sec,
                    trigger: None,
                    allowed_lateness_sec: None,
                    accumulation_mode: None,
                },
                inputs: vec![],
                outputs: vec!["windowed_stream".to_string()],
                config: serde_json::json!({ "time_domain": time_domain }),
                annotations: vec![],
                line_number: Some(line_num),
            });
        }
    }
}

impl FrameworkParser for FlinkFrameworkParser {
    fn parse(&self, code: &str) -> Result<PipelineIR> {
        let patterns = FlinkPatterns::new();

        if !patterns.flink_import_re.is_match(code) {
            return Err(anyhow!(
                "No Flink DataStream/Table API constructs (pyflink import or \
                 StreamExecutionEnvironment) detected in code"
            ));
        }

        let mut ir = PipelineIR::new("flink_pipeline".to_string());

        self.extract_checkpoint_config(code, &patterns, &mut ir);
        self.extract_state_backend(code, &patterns, &mut ir);
        self.extract_keyed_state(code, &patterns, &mut ir);
        self.extract_watermark_strategy(code, &patterns, &mut ir);
        self.extract_windowing(code, &patterns, &mut ir);

        // Sequential edges (best-effort ordering signal, same heuristic as
        // BeamPipelineParser) so graph-shaped consumers still get something
        // sensible; the Flink analyzers themselves key off node kind/config,
        // not edge topology.
        let node_ids: Vec<String> = ir.nodes.iter().map(|n| n.id.clone()).collect();
        for i in 0..node_ids.len().saturating_sub(1) {
            ir.edges.push(Edge {
                from: node_ids[i].clone(),
                to: node_ids[i + 1].clone(),
                pcollection_name: format!("pcol_{}", i),
            });
        }

        let stateful_count = patterns.stateful_re.find_iter(code).count();
        let keyed_count = ir
            .nodes
            .iter()
            .filter(
                |n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_keyed_state"),
            )
            .count();

        ir.metadata
            .runner_hints
            .insert("framework".to_string(), "flink".to_string());
        ir.metadata.runner_hints.insert(
            "has_state".to_string(),
            (stateful_count > 0 || keyed_count > 0).to_string(),
        );
        ir.metadata.runner_hints.insert(
            "stateful_operator_count".to_string(),
            stateful_count.to_string(),
        );

        Ok(ir)
    }

    fn framework(&self) -> Framework {
        Framework::ApacheFlink
    }
}

impl Default for FlinkFrameworkParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REALISTIC_FLINK_PIPELINE: &str = r#"
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.common import Duration, WatermarkStrategy
from pyflink.datastream.checkpoint_config import CheckpointingMode

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60000, CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_timeout(120000)
env.set_state_backend(EmbeddedRocksDBStateBackend(True))

ds = env.from_source(source, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)), "source")
keyed = ds.key_by(lambda x: x.customer_id)
windowed = keyed.window(TumblingEventTimeWindows.of(Time.seconds(60)))
result = windowed.process(MyProcessWindowFunction())
result.add_sink(SinkFn())
"#;

    #[test]
    fn test_flink_parser_rejects_non_flink_code() {
        let parser = FlinkFrameworkParser;
        let result = parser.parse("print('hello world')");
        assert!(result.is_err());
    }

    #[test]
    fn test_flink_parser_extracts_checkpoint_config() {
        let parser = FlinkFrameworkParser;
        let ir = parser.parse(REALISTIC_FLINK_PIPELINE).unwrap();

        let node = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_checkpoint_config"))
            .expect("checkpoint config node");

        assert_eq!(node.config["interval_ms"], 60000);
        assert_eq!(node.config["mode"], "EXACTLY_ONCE");
        assert_eq!(node.config["timeout_ms"], 120000);
    }

    #[test]
    fn test_flink_parser_extracts_state_backend_and_incremental_flag() {
        let parser = FlinkFrameworkParser;
        let ir = parser.parse(REALISTIC_FLINK_PIPELINE).unwrap();

        let node = ir
            .nodes
            .iter()
            .find(
                |n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_state_backend"),
            )
            .expect("state backend node");

        assert_eq!(node.config["backend"], "EmbeddedRocksDBStateBackend");
        assert_eq!(node.config["incremental"], true);
    }

    #[test]
    fn test_flink_parser_extracts_keyed_state_and_watermark_and_window() {
        let parser = FlinkFrameworkParser;
        let ir = parser.parse(REALISTIC_FLINK_PIPELINE).unwrap();

        let keyed = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_keyed_state"))
            .expect("keyed state node");
        assert!(keyed.config["key_expr"]
            .as_str()
            .unwrap()
            .contains("customer_id"));

        let watermark = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_watermark"))
            .expect("watermark node");
        assert_eq!(watermark.config["strategy"], "bounded_out_of_orderness");
        assert_eq!(watermark.config["out_of_orderness_sec"], 30);

        let window = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Windowing { .. }))
            .expect("window node");
        match &window.node_type {
            TransformType::Windowing {
                window_type,
                duration_sec,
                ..
            } => {
                assert_eq!(window_type, "tumbling");
                assert_eq!(*duration_sec, Some(60));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_flink_parser_no_checkpoint_config_when_absent() {
        let parser = FlinkFrameworkParser;
        let code = r#"
from pyflink.datastream import StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
ds = env.from_source(source, watermark, "source")
ds.key_by(lambda x: x[0]).process(ProcessFn()).add_sink(SinkFn())
"#;
        let ir = parser.parse(code).unwrap();
        assert!(!ir.nodes.iter().any(
            |n| matches!(&n.node_type, TransformType::Custom(t) if t == "flink_checkpoint_config")
        ));
    }
}
