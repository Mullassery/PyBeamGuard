use pyo3::prelude::*;
use pybeamguard_core::{analyze_pipeline, DataProfile};

/// PyBeamGuard: Apache Beam & Dataflow Pipeline Analysis Platform
#[pymodule]
fn pybeamguard(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.4.0")?;
    m.add("__doc__", "Apache Beam & Dataflow pipeline analysis, reliability validation, and cost optimization platform")?;
    m.add_function(wrap_pyfunction!(analyze, m)?)?;
    Ok(())
}

/// Analyze a Beam pipeline
///
/// Args:
///     code (str): Python Beam pipeline code
///     data_profile (str, optional): JSON string with data profile (throughput, element size, cardinality)
///
/// Returns:
///     str: JSON analysis results from all 10 analyzers
#[pyfunction]
#[pyo3(signature = (code, data_profile=None))]
fn analyze(code: String, data_profile: Option<String>) -> PyResult<String> {
    // Parse optional data profile from JSON string
    let profile = if let Some(profile_json) = data_profile {
        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(p) => Some(p),
            Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid data profile JSON: {}", e)
            )),
        }
    } else {
        None
    };

    // Run analysis
    let results = analyze_pipeline(&code, profile)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Convert results to JSON string
    serde_json::to_string(&results)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}
