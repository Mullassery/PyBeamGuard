use pybeamguard_core::{analyze_pipeline, reporting::TextReporter, reporting::JsonReporter, reporting::Reporter, DataProfile};
use std::fs;
use std::io::Read;

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
        _ => {
            eprintln!("Unknown command: {}", command);
            print_help();
            std::process::exit(1);
        }
    }
}

fn cmd_analyze(args: &[&str]) {
    if args.is_empty() {
        eprintln!("Usage: pybeamguard analyze <pipeline.py> [--data-profile <profile.json>] [--format json|text]");
        std::process::exit(1);
    }

    let pipeline_file = args[0];
    let mut format = "text";
    let mut data_profile_file: Option<&str> = None;

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
            "--data-profile" => {
                if i + 1 < args.len() {
                    data_profile_file = Some(args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("--data-profile requires a file path");
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
    if let Err(e) = fs::File::open(pipeline_file)
        .and_then(|mut f| f.read_to_string(&mut pipeline_code))
    {
        eprintln!("Error reading pipeline file '{}': {}", pipeline_file, e);
        std::process::exit(1);
    }

    // Parse data profile if provided
    let data_profile = if let Some(profile_file) = data_profile_file {
        let mut profile_json = String::new();
        if let Err(e) = fs::File::open(profile_file)
            .and_then(|mut f| f.read_to_string(&mut profile_json))
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
    match analyze_pipeline(&pipeline_code, data_profile) {
        Ok(results) => {
            let output = match format {
                "json" => JsonReporter.format(&results),
                "text" | _ => TextReporter.format(&results),
            };
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Analysis failed: {}", e);
            std::process::exit(1);
        }
    }
}

fn print_help() {
    println!(
        r#"PyBeamGuard v0.4.0 - Apache Beam & Dataflow Analysis Platform

USAGE:
    pybeamguard <COMMAND> [OPTIONS]

COMMANDS:
    analyze             Analyze a Beam pipeline
    help                Show this help message

OPTIONS for 'analyze':
    <pipeline.py>           Path to Beam pipeline Python file (required)
    --data-profile FILE     Path to data profile JSON file (optional)
    --format FORMAT         Output format: text (default) or json

EXAMPLES:
    # Analyze a pipeline
    pybeamguard analyze pipeline.py

    # With data profile
    pybeamguard analyze pipeline.py --data-profile profile.json

    # JSON output
    pybeamguard analyze pipeline.py --format json

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
