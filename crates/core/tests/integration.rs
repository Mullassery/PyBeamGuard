//! End-to-end integration tests for `analyze_pipeline`.
//!
//! Unlike the unit tests colocated with each analyzer (which construct a
//! `PipelineIR` by hand and exercise one analyzer at a time), these tests
//! feed realistic, multi-line Beam pipeline *source text* through the full
//! `analyze_pipeline` path: parsing -> all ten analyzers -> aggregated
//! results. They also cover the adversarial inputs called out in the code
//! audit (empty file, malformed data profile, non-UTF8-like content) to
//! guard against panics in a CLI that is meant to run unattended in CI on
//! third-party/PR-submitted pipeline code.

use pybeamguard_core::{analyze_pipeline, DataProfile};

const REALISTIC_STREAMING_PIPELINE: &str = r#"
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        events = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic='projects/x/topics/events')
            | 'ParseEvent' >> beam.ParDo(ParseEventFn())
            | 'Window' >> beam.WindowInto(FixedWindows(60))
            | 'KeyByCustomer' >> beam.GroupByKey()
            | 'CountPerCustomer' >> beam.CombinePerKey(sum)
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('dataset.table')
        )

class ParseEventFn(beam.DoFn):
    def process(self, element):
        try:
            yield parse(element)
        except Exception:
            pass

if __name__ == '__main__':
    run()
"#;

#[test]
fn analyze_pipeline_realistic_streaming_source_end_to_end() {
    let results = analyze_pipeline(REALISTIC_STREAMING_PIPELINE, None)
        .expect("realistic pipeline should parse and analyze without error");

    // All ten analyzers should have produced a result (registry order).
    assert_eq!(results.len(), 10, "expected all 10 analyzers to run");

    let names: Vec<&str> = results.iter().map(|r| r.analyzer_name.as_str()).collect();
    for expected in [
        "GraphAnalyzer",
        "HotKeyAnalyzer",
        "ShuffleAnalyzer",
        "WindowingAnalyzer",
        "StateAnalyzer",
        "CostAnalyzer",
        "ReliabilityAnalyzer",
        "BestPracticesAnalyzer",
        "DeploymentAnalyzer",
        "SynthesisEngine",
    ] {
        assert!(
            names.contains(&expected),
            "missing analyzer result: {expected}"
        );
    }

    // The pipeline aggregates by 'CountPerCustomer' on a GroupByKey with no
    // explicit customer_id key expression captured by the parser's naive
    // key extraction, but HotKeyAnalyzer should still run cleanly and the
    // GraphAnalyzer should see a non-trivial node count.
    let graph = results
        .iter()
        .find(|r| r.analyzer_name == "GraphAnalyzer")
        .unwrap();
    assert!(graph.metrics.get("node_count").copied().unwrap_or(0.0) >= 5.0);

    // SynthesisEngine (runs last) should always emit an executive summary finding.
    let synthesis = results
        .iter()
        .find(|r| r.analyzer_name == "SynthesisEngine")
        .unwrap();
    assert!(synthesis
        .findings
        .iter()
        .any(|f| f.id == "SYNTHESIS_EXECUTIVE_SUMMARY"));
}

#[test]
fn analyze_pipeline_with_data_profile_end_to_end() {
    let profile = DataProfile {
        estimated_throughput_per_sec: Some(5_000.0),
        average_element_size_bytes: Some(256),
        key_cardinality: Some(20),
        estimated_state_size_gb: Some(3.0),
    };

    let results = analyze_pipeline(REALISTIC_STREAMING_PIPELINE, Some(profile))
        .expect("pipeline with a data profile should analyze without error");

    let hotkey = results
        .iter()
        .find(|r| r.analyzer_name == "HotKeyAnalyzer")
        .unwrap();
    // key_cardinality: 20 is well below the "dangerously low" threshold, so
    // the data-profile-aware path should fire a critical finding.
    assert!(hotkey
        .findings
        .iter()
        .any(|f| f.id == "HOTKEY_LOW_CARDINALITY_MEASURED"));

    let cost = results
        .iter()
        .find(|r| r.analyzer_name == "CostAnalyzer")
        .unwrap();
    // With a data profile supplied, CostAnalyzer's confidence should be
    // higher than its no-profile default.
    assert!(cost.confidence > 0.6);
}

#[test]
fn analyze_pipeline_empty_source_does_not_panic() {
    let result = analyze_pipeline("", None);
    assert!(
        result.is_err(),
        "empty input has no Beam import and should be a clean error, not a panic"
    );
}

#[test]
fn analyze_pipeline_non_beam_source_does_not_panic() {
    let result = analyze_pipeline("print('hello world')\nx = 1 + 1\n", None);
    assert!(result.is_err());
}

#[test]
fn analyze_pipeline_malformed_data_profile_json_is_a_clean_error() {
    // This mirrors what the CLI and Python bindings do: attempt to
    // deserialize caller-supplied JSON into a DataProfile. Malformed JSON
    // must produce an Err, never a panic.
    let malformed = "{ this is not valid json ";
    let parsed = serde_json::from_str::<DataProfile>(malformed);
    assert!(parsed.is_err());
}

#[test]
fn analyze_pipeline_handles_unicode_and_pathological_content_without_panicking() {
    // Exercises the parser's byte-offset string slicing (extract_dofn_name,
    // extract_key_expr, extract_duration, extract_allowed_lateness) with
    // multi-byte UTF-8 characters and unusual punctuation near the
    // delimiters those helpers search for.
    let pathological = r#"
import apache_beam as beam

with beam.Pipeline() as p:
    data = (
        p
        | 'Read' >> beam.io.ReadFromText('input_😀.txt')
        | 'Parse😀Data' >> beam.ParDo(Pärse😀Fn())
        | 'GroupBy' >> beam.GroupByKey()
        | 'Write' >> beam.io.WriteToText('output_日本語.txt')
    )
"#;

    let result = analyze_pipeline(pathological, None);
    assert!(
        result.is_ok(),
        "unicode-heavy pipeline source should not panic the parser"
    );
}

#[test]
fn analyze_pipeline_extremely_long_single_line_does_not_panic() {
    // A pathological input: one very long line that still contains a
    // recognizable Beam import, to make sure line-based regex scanning
    // doesn't choke or panic on unusually long lines.
    let mut src = String::from("import apache_beam as beam\n# ");
    src.push_str(&"x".repeat(200_000));
    src.push('\n');

    let result = analyze_pipeline(&src, None);
    // No transforms detected beyond the import, but this must not panic;
    // whether it errors (no pipeline) or succeeds with zero nodes is fine.
    let _ = result;
}
