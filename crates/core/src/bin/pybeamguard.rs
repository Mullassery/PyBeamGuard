use pybeamguard_core::{
    analyze_flink_pipeline, analyze_pipeline, analyze_spark_pipeline, reporting::JsonReporter,
    reporting::Reporter, reporting::TextReporter, AnalysisResult, DataProfile, RiskSeverity,
};
use std::fs;
use std::io::Read;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_help();
        return;
    }

    let command = &args[1];

    match command.as_str() {
        "analyze" => {
            let arg_strs: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            cmd_analyze(&arg_strs);
        }
        "help" | "--help" | "-h" => print_help(),
        "version" | "--version" | "-v" => println!("pybeamguard {VERSION}"),
        _ => {
            eprintln!("Unknown command: {}", command);
            print_help();
            std::process::exit(1);
        }
    }
}

/// Parse a `--fail-on` severity argument. Returns `None` for unrecognized values.
fn parse_severity(s: &str) -> Option<RiskSeverity> {
    match s.to_lowercase().as_str() {
        "info" => Some(RiskSeverity::Info),
        "low" => Some(RiskSeverity::Low),
        "medium" => Some(RiskSeverity::Medium),
        "high" => Some(RiskSeverity::High),
        "critical" => Some(RiskSeverity::Critical),
        _ => None,
    }
}

fn cmd_analyze(args: &[&str]) {
    if args.is_empty() {
        eprintln!(
            "Usage: pybeamguard analyze <pipeline.py> [--framework beam|flink|spark] [--data-profile <profile.json>] [--format json|text] [--fail-on <severity>]"
        );
        std::process::exit(1);
    }

    let pipeline_file = args[0];
    let mut format = "text";
    let mut data_profile_file: Option<&str> = None;
    let mut fail_on: Option<RiskSeverity> = None;
    let mut framework = "beam";

    // Parse options
    let mut i = 1;
    while i < args.len() {
        match args[i] {
            "--format" => {
                if i + 1 < args.len() {
                    format = args[i + 1];
                    i += 2;
                } else {
                    eprintln!("--format requires a value");
                    std::process::exit(1);
                }
            }
            "--framework" => {
                if i + 1 < args.len() {
                    framework = args[i + 1];
                    if !matches!(framework, "beam" | "flink" | "spark") {
                        eprintln!(
                            "Invalid --framework value '{}'. Expected one of: beam, flink, spark",
                            framework
                        );
                        std::process::exit(1);
                    }
                    i += 2;
                } else {
                    eprintln!("--framework requires a value (beam|flink|spark)");
                    std::process::exit(1);
                }
            }
            "--data-profile" => {
                if i + 1 < args.len() {
                    data_profile_file = Some(args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("--data-profile requires a file path");
                    std::process::exit(1);
                }
            }
            "--fail-on" => {
                if i + 1 < args.len() {
                    match parse_severity(args[i + 1]) {
                        Some(sev) => fail_on = Some(sev),
                        None => {
                            eprintln!(
                                "Invalid --fail-on value '{}'. Expected one of: info, low, medium, high, critical",
                                args[i + 1]
                            );
                            std::process::exit(1);
                        }
                    }
                    i += 2;
                } else {
                    eprintln!(
                        "--fail-on requires a severity value (info|low|medium|high|critical)"
                    );
                    std::process::exit(1);
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    // Read pipeline file
    let mut pipeline_code = String::new();
    if let Err(e) =
        fs::File::open(pipeline_file).and_then(|mut f| f.read_to_string(&mut pipeline_code))
    {
        eprintln!("Error reading pipeline file '{}': {}", pipeline_file, e);
        std::process::exit(1);
    }

    // Parse data profile if provided
    let data_profile = if let Some(profile_file) = data_profile_file {
        let mut profile_json = String::new();
        if let Err(e) =
            fs::File::open(profile_file).and_then(|mut f| f.read_to_string(&mut profile_json))
        {
            eprintln!("Error reading data profile file '{}': {}", profile_file, e);
            std::process::exit(1);
        }

        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(profile) => Some(profile),
            Err(e) => {
                eprintln!("Error parsing data profile JSON: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // Run analysis
    let analysis: anyhow::Result<Vec<AnalysisResult>> = match framework {
        "flink" => analyze_flink_pipeline(&pipeline_code, data_profile),
        "spark" => analyze_spark_pipeline(&pipeline_code, data_profile),
        _ => analyze_pipeline(&pipeline_code, data_profile),
    };

    match analysis {
        Ok(results) => {
            let output = match format {
                "json" => JsonReporter.format(&results),
                _ => TextReporter.format(&results),
            };
            println!("{}", output);

            if let Some(threshold) = fail_on {
                let threshold_score = threshold.score();
                let breached = results
                    .iter()
                    .flat_map(|r| r.findings.iter())
                    .any(|f| f.severity.score() >= threshold_score);

                if breached {
                    eprintln!(
                        "\n--fail-on {}: at least one finding meets or exceeds this severity.",
                        args_fail_on_display(threshold)
                    );
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("Analysis failed: {}", e);
            std::process::exit(1);
        }
    }
}

fn args_fail_on_display(sev: RiskSeverity) -> &'static str {
    match sev {
        RiskSeverity::Info => "info",
        RiskSeverity::Low => "low",
        RiskSeverity::Medium => "medium",
        RiskSeverity::High => "high",
        RiskSeverity::Critical => "critical",
    }
}

fn print_help() {
    println!(
        r#"PyBeamGuard v{VERSION} - Apache Beam & Dataflow Analysis Platform

USAGE:
    pybeamguard <COMMAND> [OPTIONS]

COMMANDS:
    analyze             Analyze a Beam pipeline
    version             Show version and exit
    help                Show this help message

OPTIONS for 'analyze':
    <pipeline.py>           Path to pipeline source file (required)
    --framework FRAMEWORK   Pipeline framework: beam (default), flink, or spark
    --data-profile FILE     Path to data profile JSON file (optional)
    --format FORMAT         Output format: text (default) or json
    --fail-on SEVERITY      Exit non-zero if any finding is >= this severity
                             (info|low|medium|high|critical)

EXAMPLES:
    # Analyze a Beam pipeline
    pybeamguard analyze pipeline.py

    # Analyze a PyFlink pipeline (checkpointing, state backend, watermarks)
    pybeamguard analyze streaming_job.py --framework flink

    # Analyze a PySpark pipeline (shuffle partitions, joins, streaming)
    pybeamguard analyze etl.py --framework spark

    # With data profile
    pybeamguard analyze pipeline.py --data-profile profile.json

    # JSON output
    pybeamguard analyze pipeline.py --format json

    # CI gating: fail the build on critical findings
    pybeamguard analyze pipeline.py --fail-on critical

DATA PROFILE FORMAT:
    {{
        "estimated_throughput_per_sec": 10000,
        "average_element_size_bytes": 500,
        "key_cardinality": 50000,
        "estimated_state_size_gb": 5.0
    }}
"#
    );
}
