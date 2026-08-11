use crate::ir::*;
use anyhow::{anyhow, Result};
use regex::Regex;

pub struct BeamPipelineParser {
    pattern_cache: PatternCache,
}

struct PatternCache {
    beam_import_re: Regex,
    pardo_re: Regex,
    groupby_re: Regex,
    windowing_re: Regex,
    sink_re: Regex,
}

impl PatternCache {
    fn new() -> Self {
        Self {
            beam_import_re: Regex::new(r"import apache_beam|from apache_beam").unwrap(),
            pardo_re: Regex::new(r"ParDo|beam\.ParDo").unwrap(),
            groupby_re: Regex::new(r"GroupByKey|beam\.GroupByKey").unwrap(),
            windowing_re: Regex::new(r"Windowing|FixedWindows|SlidingWindows|SessionWindows")
                .unwrap(),
            // `beam\.io\.Write` (not bare `beam\.io`) — the latter also
            // matches `beam.io.ReadFromX(...)` source calls, misclassifying
            // every source line as a sink too.
            sink_re: Regex::new(r"WriteToText|WriteToBigQuery|WriteToPubSub|beam\.io\.Write")
                .unwrap(),
        }
    }
}

impl BeamPipelineParser {
    pub fn new() -> Self {
        Self {
            pattern_cache: PatternCache::new(),
        }
    }

    pub fn parse(&self, python_code: &str) -> Result<PipelineIR> {
        let mut ir = PipelineIR::new("beam_pipeline".to_string());

        // Basic validation
        if !self.pattern_cache.beam_import_re.is_match(python_code) {
            return Err(anyhow!("No Apache Beam imports detected in code"));
        }

        // Extract transforms via regex patterns and basic AST analysis
        self.extract_transforms(python_code, &mut ir)?;

        // Build edges from the detected transforms
        self.build_edges(&mut ir);

        Ok(ir)
    }

    fn extract_transforms(&self, code: &str, ir: &mut PipelineIR) -> Result<()> {
        let lines: Vec<&str> = code.lines().collect();

        for (line_num, line) in lines.iter().enumerate() {
            let trimmed = line.trim();

            // Skip comments and empty lines
            if trimmed.starts_with('#') || trimmed.is_empty() {
                continue;
            }

            // Detect source (Read operations)
            if trimmed.contains("ReadFromText")
                || trimmed.contains("ReadFromBigQuery")
                || trimmed.contains("ReadFromPubSub")
            {
                self.add_source_transform(trimmed, line_num, ir);
            }

            // Detect ParDo (stateful if contains @beam.utils.timestamp_utils or state).
            // Real Beam pipelines are conventionally written with the `|`
            // pipe operator (`p | 'Name' >> beam.ParDo(...)`), so excluding
            // lines containing `|` would silently miss ParDo in almost
            // every idiomatically-written pipeline; comments are still
            // excluded via `trimmed.starts_with('#')` above.
            if self.pattern_cache.pardo_re.is_match(trimmed) {
                self.add_pardo_transform(trimmed, line_num, ir);
            }

            // Detect GroupByKey
            if self.pattern_cache.groupby_re.is_match(trimmed) {
                self.add_groupbykey_transform(trimmed, line_num, ir);
            }

            // Detect CombinePerKey
            if trimmed.contains("CombinePerKey") {
                self.add_combineperkey_transform(trimmed, line_num, ir);
            }

            // Detect CoGroupByKey
            if trimmed.contains("CoGroupByKey") {
                self.add_cogroupbykey_transform(trimmed, line_num, ir);
            }

            // Detect Windowing
            if self.pattern_cache.windowing_re.is_match(trimmed) {
                self.add_windowing_transform(trimmed, line_num, ir);
            }

            // Detect Sink (Write operations)
            if self.pattern_cache.sink_re.is_match(trimmed) {
                self.add_sink_transform(trimmed, line_num, ir);
            }

            // Detect Flatten
            if trimmed.contains("Flatten") {
                self.add_flatten_transform(trimmed, line_num, ir);
            }

            // Detect Partition
            if trimmed.contains("Partition") {
                self.add_partition_transform(trimmed, line_num, ir);
            }
        }

        Ok(())
    }

    fn add_source_transform(&self, line: &str, line_num: usize, ir: &mut PipelineIR) {
        let source_type = if line.contains("ReadFromText") {
            "text_file"
        } else if line.contains("ReadFromBigQuery") {
            "bigquery"
        } else if line.contains("ReadFromPubSub") {
            "pubsub"
        } else {
            "unknown"
        };

        let name = self.extract_var_name(line).unwrap_or("source".to_string());

        ir.nodes.push(TransformNode {
            id: self.generate_id(&name),
            name: name.clone(),
            node_type: TransformType::Source {
                source_type: source_type.to_string(),
            },
            inputs: vec![],
            outputs: vec![name],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_pardo_transform(&self, line: &str, line_num: usize, ir: &mut PipelineIR) {
        let dofn_name = self.extract_dofn_name(line).unwrap_or("DoFn".to_string());
        let name = format!("ParDo_{}", dofn_name);

        ir.nodes.push(TransformNode {
            id: self.generate_id(&name),
            name: name.clone(),
            node_type: TransformType::ParDo {
                dofn_name,
                is_stateful: line.contains("@beam.utils.timestamp_utils")
                    || line.contains("StateSpec")
                    || line.contains("TimerSpec"),
                has_side_inputs: line.contains("beam.pvalue.AsSingleton")
                    || line.contains("beam.pvalue.AsIter"),
                has_side_outputs: line.contains("beam.pvalue.TaggedOutput"),
            },
            inputs: vec![],
            outputs: vec![name],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_groupbykey_transform(&self, line: &str, line_num: usize, ir: &mut PipelineIR) {
        let key_expr = self.extract_key_expr(line);
        ir.nodes.push(TransformNode {
            id: self.generate_id("GroupByKey"),
            name: "GroupByKey".to_string(),
            node_type: TransformType::GroupByKey { key_expr },
            inputs: vec![],
            outputs: vec!["grouped_data".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_combineperkey_transform(&self, line: &str, line_num: usize, ir: &mut PipelineIR) {
        let combine_fn = self
            .extract_dofn_name(line)
            .unwrap_or("CombineFn".to_string());
        ir.nodes.push(TransformNode {
            id: self.generate_id("CombinePerKey"),
            name: "CombinePerKey".to_string(),
            node_type: TransformType::CombinePerKey { combine_fn },
            inputs: vec![],
            outputs: vec!["combined_data".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_cogroupbykey_transform(&self, _line: &str, line_num: usize, ir: &mut PipelineIR) {
        ir.nodes.push(TransformNode {
            id: self.generate_id("CoGroupByKey"),
            name: "CoGroupByKey".to_string(),
            node_type: TransformType::CoGroupByKey { key_expr: None },
            inputs: vec![],
            outputs: vec!["cogrouped_data".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_windowing_transform(&self, line: &str, line_num: usize, ir: &mut PipelineIR) {
        let window_type = if line.contains("FixedWindows") {
            "fixed"
        } else if line.contains("SlidingWindows") {
            "sliding"
        } else if line.contains("SessionWindows") {
            "session"
        } else {
            "global"
        };

        let duration_sec = self.extract_duration(line);
        let allowed_lateness_sec = self.extract_allowed_lateness(line);

        ir.nodes.push(TransformNode {
            id: self.generate_id("Windowing"),
            name: "Windowing".to_string(),
            node_type: TransformType::Windowing {
                window_type: window_type.to_string(),
                duration_sec,
                trigger: None,
                allowed_lateness_sec,
                accumulation_mode: None,
            },
            inputs: vec![],
            outputs: vec!["windowed_data".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_sink_transform(&self, line: &str, line_num: usize, ir: &mut PipelineIR) {
        let sink_type = if line.contains("WriteToBigQuery") {
            "bigquery"
        } else if line.contains("WriteToPubSub") {
            "pubsub"
        } else if line.contains("WriteToText") {
            "text"
        } else {
            "unknown"
        };

        ir.nodes.push(TransformNode {
            id: self.generate_id("Sink"),
            name: "Sink".to_string(),
            node_type: TransformType::Sink {
                sink_type: sink_type.to_string(),
            },
            inputs: vec![],
            outputs: vec![],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_flatten_transform(&self, _line: &str, line_num: usize, ir: &mut PipelineIR) {
        ir.nodes.push(TransformNode {
            id: self.generate_id("Flatten"),
            name: "Flatten".to_string(),
            node_type: TransformType::Flatten,
            inputs: vec![],
            outputs: vec!["flattened_data".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn add_partition_transform(&self, _line: &str, line_num: usize, ir: &mut PipelineIR) {
        ir.nodes.push(TransformNode {
            id: self.generate_id("Partition"),
            name: "Partition".to_string(),
            node_type: TransformType::Partition,
            inputs: vec![],
            outputs: vec!["partitioned_data".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: Some(line_num),
        });
    }

    fn build_edges(&self, ir: &mut PipelineIR) {
        // Simple heuristic: connect transforms in source→sink order
        let node_ids: Vec<String> = ir.nodes.iter().map(|n| n.id.clone()).collect();

        for i in 0..node_ids.len().saturating_sub(1) {
            ir.edges.push(Edge {
                from: node_ids[i].clone(),
                to: node_ids[i + 1].clone(),
                pcollection_name: format!("pcol_{}", i),
            });
        }
    }

    fn extract_var_name(&self, line: &str) -> Option<String> {
        line.split('=').next().map(|s| s.trim().to_string())
    }

    fn extract_dofn_name(&self, line: &str) -> Option<String> {
        if let Some(start) = line.find('(') {
            if let Some(end) = line[start + 1..].find(')') {
                let content = &line[start + 1..start + 1 + end];
                return Some(content.trim().to_string());
            }
        }
        None
    }

    fn extract_key_expr(&self, line: &str) -> Option<String> {
        if let Some(start) = line.find('(') {
            if let Some(end) = line[start + 1..].find(')') {
                let content = &line[start + 1..start + 1 + end];
                return Some(content.trim().to_string());
            }
        }
        None
    }

    fn extract_duration(&self, line: &str) -> Option<u64> {
        if let Some(start) = line.find("seconds(") {
            if let Some(end) = line[start + 8..].find(')') {
                let num_str = &line[start + 8..start + 8 + end];
                return num_str.trim().parse().ok();
            }
        }
        None
    }

    fn extract_allowed_lateness(&self, line: &str) -> Option<u64> {
        // "allowed_lateness(" is 17 bytes; skipping only 16 (as this used
        // to) left the opening '(' inside `num_str`, so `.parse()` always
        // failed and this function silently returned None for every input.
        const PREFIX_LEN: usize = "allowed_lateness(".len();
        if let Some(start) = line.find("allowed_lateness(") {
            if let Some(end) = line[start + PREFIX_LEN..].find(')') {
                let num_str = &line[start + PREFIX_LEN..start + PREFIX_LEN + end];
                return num_str.trim().parse().ok();
            }
        }
        None
    }

    fn generate_id(&self, name: &str) -> String {
        format!("node_{}", name.to_lowercase().replace(' ', "_"))
    }
}

impl Default for BeamPipelineParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_pipeline() {
        let code = r#"
import apache_beam as beam

with beam.Pipeline() as p:
    result = (
        p
        | 'Read' >> beam.io.ReadFromText('input.txt')
        | 'ParseData' >> beam.ParDo(ParseFn())
        | 'WriteOutput' >> beam.io.WriteToText('output.txt')
    )
"#;

        let parser = BeamPipelineParser::new();
        let ir = parser.parse(code).unwrap();
        assert!(!ir.nodes.is_empty());
    }

    #[test]
    fn test_parse_rejects_code_without_beam_import() {
        let parser = BeamPipelineParser::new();
        let result = parser.parse("print('hello world')");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_detects_each_source_type() {
        let parser = BeamPipelineParser::new();

        for (snippet, expected_type) in [
            (
                "import apache_beam as beam\nx = beam.io.ReadFromText('f.txt')",
                "text_file",
            ),
            (
                "import apache_beam as beam\nx = beam.io.ReadFromBigQuery(query='q')",
                "bigquery",
            ),
            (
                "import apache_beam as beam\nx = beam.io.ReadFromPubSub(topic='t')",
                "pubsub",
            ),
        ] {
            let ir = parser.parse(snippet).unwrap();
            let source = ir
                .nodes
                .iter()
                .find(|n| matches!(&n.node_type, TransformType::Source { .. }))
                .unwrap_or_else(|| panic!("no Source node detected for {snippet:?}"));
            match &source.node_type {
                TransformType::Source { source_type } => assert_eq!(source_type, expected_type),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_detects_each_sink_type() {
        let parser = BeamPipelineParser::new();

        for (snippet, expected_type) in [
            (
                "import apache_beam as beam\nx | beam.io.WriteToText('f.txt')",
                "text",
            ),
            (
                "import apache_beam as beam\nx | beam.io.WriteToBigQuery(table='t')",
                "bigquery",
            ),
            (
                "import apache_beam as beam\nx | beam.io.WriteToPubSub(topic='t')",
                "pubsub",
            ),
        ] {
            let ir = parser.parse(snippet).unwrap();
            let sink = ir
                .nodes
                .iter()
                .find(|n| matches!(&n.node_type, TransformType::Sink { .. }))
                .unwrap_or_else(|| panic!("no Sink node detected for {snippet:?}"));
            match &sink.node_type {
                TransformType::Sink { sink_type } => assert_eq!(sink_type, expected_type),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_pardo_flags_stateful_and_side_inputs() {
        let parser = BeamPipelineParser::new();
        let code = r#"
import apache_beam as beam
x = beam.ParDo(MyStatefulFn(), StateSpec('count'))
y = beam.ParDo(MyFn(), side=beam.pvalue.AsSingleton(other))
z = beam.ParDo(PlainFn())
"#;
        let ir = parser.parse(code).unwrap();
        let pardos: Vec<_> = ir
            .nodes
            .iter()
            .filter_map(|n| match &n.node_type {
                TransformType::ParDo {
                    is_stateful,
                    has_side_inputs,
                    ..
                } => Some((*is_stateful, *has_side_inputs)),
                _ => None,
            })
            .collect();

        assert_eq!(pardos.len(), 3);
        assert!(pardos.iter().any(|(stateful, _)| *stateful));
        assert!(pardos.iter().any(|(_, side)| *side));
        assert!(pardos.iter().any(|(stateful, side)| !stateful && !side));
    }

    #[test]
    fn test_parse_windowing_extracts_type_duration_and_lateness() {
        let parser = BeamPipelineParser::new();
        let code = r#"
import apache_beam as beam
x = beam.WindowInto(FixedWindows(seconds(60)), allowed_lateness(300))
"#;
        let ir = parser.parse(code).unwrap();
        let windowing = ir
            .nodes
            .iter()
            .find(|n| matches!(&n.node_type, TransformType::Windowing { .. }))
            .unwrap();

        match &windowing.node_type {
            TransformType::Windowing {
                window_type,
                duration_sec,
                allowed_lateness_sec,
                ..
            } => {
                assert_eq!(window_type, "fixed");
                assert_eq!(*duration_sec, Some(60));
                assert_eq!(*allowed_lateness_sec, Some(300));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_windowing_distinguishes_sliding_and_session() {
        let parser = BeamPipelineParser::new();

        let sliding = parser
            .parse("import apache_beam as beam\nx = beam.WindowInto(SlidingWindows(seconds(30)))")
            .unwrap();
        let session = parser
            .parse("import apache_beam as beam\nx = beam.WindowInto(SessionWindows(gap_size=seconds(10)))")
            .unwrap();

        let window_type = |ir: &PipelineIR| {
            ir.nodes
                .iter()
                .find_map(|n| match &n.node_type {
                    TransformType::Windowing { window_type, .. } => Some(window_type.clone()),
                    _ => None,
                })
                .unwrap()
        };
        assert_eq!(window_type(&sliding), "sliding");
        assert_eq!(window_type(&session), "session");
    }

    #[test]
    fn test_parse_detects_groupby_combine_cogroup() {
        let parser = BeamPipelineParser::new();
        let code = r#"
import apache_beam as beam
a = x | beam.GroupByKey()
b = y | beam.CombinePerKey(sum)
c = z | beam.CoGroupByKey()
"#;
        let ir = parser.parse(code).unwrap();

        assert!(ir
            .nodes
            .iter()
            .any(|n| matches!(n.node_type, TransformType::GroupByKey { .. })));
        assert!(ir
            .nodes
            .iter()
            .any(|n| matches!(n.node_type, TransformType::CombinePerKey { .. })));
        assert!(ir
            .nodes
            .iter()
            .any(|n| matches!(n.node_type, TransformType::CoGroupByKey { .. })));
    }

    #[test]
    fn test_parse_detects_flatten_and_partition() {
        let parser = BeamPipelineParser::new();
        let code = r#"
import apache_beam as beam
a = (x, y) | beam.Flatten()
b = x | beam.Partition(fn, 2)
"#;
        let ir = parser.parse(code).unwrap();

        assert!(ir
            .nodes
            .iter()
            .any(|n| matches!(n.node_type, TransformType::Flatten)));
        assert!(ir
            .nodes
            .iter()
            .any(|n| matches!(n.node_type, TransformType::Partition)));
    }

    #[test]
    fn test_parse_skips_comments_and_blank_lines() {
        let parser = BeamPipelineParser::new();
        let code = r#"
import apache_beam as beam

# x = beam.io.ReadFromText('should_be_ignored.txt')

x = beam.io.ReadFromText('real.txt')
"#;
        let ir = parser.parse(code).unwrap();
        let sources: Vec<_> = ir
            .nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::Source { .. }))
            .collect();
        assert_eq!(sources.len(), 1);
    }

    #[test]
    fn test_build_edges_connects_nodes_sequentially() {
        let parser = BeamPipelineParser::new();
        let code = r#"
import apache_beam as beam
a = beam.io.ReadFromText('f.txt')
b = beam.ParDo(MyFn())
c = beam.io.WriteToText('out.txt')
"#;
        let ir = parser.parse(code).unwrap();

        assert_eq!(ir.edges.len(), ir.nodes.len().saturating_sub(1));
        for (i, edge) in ir.edges.iter().enumerate() {
            assert_eq!(edge.from, ir.nodes[i].id);
            assert_eq!(edge.to, ir.nodes[i + 1].id);
        }
    }

    #[test]
    fn test_parse_realistic_pipeline_detects_all_stages_in_order() {
        let parser = BeamPipelineParser::new();
        let code = r#"
import apache_beam as beam

with beam.Pipeline() as p:
    result = (
        p
        | 'Read' >> beam.io.ReadFromPubSub(topic='projects/x/topics/y')
        | 'Parse' >> beam.ParDo(ParseFn())
        | 'Window' >> beam.WindowInto(FixedWindows(seconds(60)))
        | 'Group' >> beam.GroupByKey()
        | 'Aggregate' >> beam.CombinePerKey(sum)
        | 'Write' >> beam.io.WriteToBigQuery(table='dataset.table')
    )
"#;
        let ir = parser.parse(code).unwrap();

        let types: Vec<&str> = ir
            .nodes
            .iter()
            .map(|n| match &n.node_type {
                TransformType::Source { .. } => "source",
                TransformType::ParDo { .. } => "pardo",
                TransformType::Windowing { .. } => "windowing",
                TransformType::GroupByKey { .. } => "groupby",
                TransformType::CombinePerKey { .. } => "combine",
                TransformType::Sink { .. } => "sink",
                _ => "other",
            })
            .collect();

        assert_eq!(
            types,
            vec!["source", "pardo", "windowing", "groupby", "combine", "sink"]
        );
    }
}
