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

use pybeamguard_core::{
    analyze_flink_pipeline, analyze_pipeline, analyze_spark_pipeline, DataProfile,
};

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

// ---------------------------------------------------------------------------
// Flink: end-to-end through analyze_flink_pipeline
// ---------------------------------------------------------------------------

const REALISTIC_FLINK_PIPELINE_WITH_RISKS: &str = r#"
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state_backend import HashMapStateBackend

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(200)
env.set_state_backend(HashMapStateBackend())

ds = env.from_source(source, watermark, "source")
keyed = ds.key_by(lambda x: x.customer_id)
windowed = keyed.window(TumblingEventTimeWindows.of(Time.seconds(60)))
result = windowed.process(MyProcessWindowFunction())
result.add_sink(SinkFn())
"#;

#[test]
fn analyze_flink_pipeline_realistic_source_end_to_end() {
    let results = analyze_flink_pipeline(REALISTIC_FLINK_PIPELINE_WITH_RISKS, None)
        .expect("realistic Flink pipeline should parse and analyze without error");

    assert_eq!(results.len(), 3, "expected all 3 Flink analyzers to run");
    let names: Vec<&str> = results.iter().map(|r| r.analyzer_name.as_str()).collect();
    for expected in [
        "FlinkCheckpointAnalyzer",
        "FlinkStateAnalyzer",
        "FlinkWatermarkAnalyzer",
    ] {
        assert!(
            names.contains(&expected),
            "missing Flink analyzer result: {expected}"
        );
    }

    let all_findings: Vec<&str> = results
        .iter()
        .flat_map(|r| r.findings.iter().map(|f| f.id.as_str()))
        .collect();

    // Aggressive 200ms checkpoint interval.
    assert!(all_findings.contains(&"FLINK_CHECKPOINT_INTERVAL_TOO_AGGRESSIVE"));
    // HashMapStateBackend is heap-bound.
    assert!(all_findings.contains(&"FLINK_STATE_BACKEND_HEAP_RISK"));
    // key_by(customer_id) matches the high-risk key domain heuristic.
    assert!(all_findings.contains(&"FLINK_KEYED_STATE_HOT_KEY_RISK"));
    // Event-time window with no watermark strategy assigned.
    assert!(all_findings.contains(&"FLINK_WATERMARK_MISSING"));
}

#[test]
fn analyze_flink_pipeline_rejects_non_flink_source() {
    let result = analyze_flink_pipeline("print('not flink')", None);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Spark: end-to-end through analyze_spark_pipeline
// ---------------------------------------------------------------------------

const REALISTIC_SPARK_PIPELINE_WITH_RISKS: &str = r#"
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("orders").getOrCreate()
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
fn analyze_spark_pipeline_realistic_source_end_to_end() {
    let results = analyze_spark_pipeline(REALISTIC_SPARK_PIPELINE_WITH_RISKS, None)
        .expect("realistic Spark pipeline should parse and analyze without error");

    assert_eq!(results.len(), 3, "expected all 3 Spark analyzers to run");
    let names: Vec<&str> = results.iter().map(|r| r.analyzer_name.as_str()).collect();
    for expected in [
        "SparkShuffleAnalyzer",
        "SparkJoinAnalyzer",
        "SparkStreamingAnalyzer",
    ] {
        assert!(
            names.contains(&expected),
            "missing Spark analyzer result: {expected}"
        );
    }

    let all_findings: Vec<&str> = results
        .iter()
        .flat_map(|r| r.findings.iter().map(|f| f.id.as_str()))
        .collect();

    // autoBroadcastJoinThreshold disabled (-1).
    assert!(all_findings.contains(&"SPARK_BROADCAST_THRESHOLD_DISABLED"));
    // join(on="customer_id") matches the high-risk key domain heuristic.
    assert!(all_findings.contains(&"SPARK_JOIN_HIGH_RISK_KEY"));
    // writeStream with no checkpointLocation option.
    assert!(all_findings.contains(&"SPARK_STREAMING_NO_CHECKPOINT_LOCATION"));
    // groupBy aggregation + append mode + no watermark: fails at runtime in real Spark.
    assert!(all_findings.contains(&"SPARK_AGGREGATION_APPEND_MODE_INCOMPATIBLE"));
}

#[test]
fn analyze_spark_pipeline_rejects_non_spark_source() {
    let result = analyze_spark_pipeline("print('not spark')", None);
    assert!(result.is_err());
}
