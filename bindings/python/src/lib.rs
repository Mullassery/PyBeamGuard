use pyo3::prelude::*;
use pybeamguard_core::{analyze_pipeline, DataProfile, PipelineIR as RustPipelineIR, TransformNode as RustTransformNode, Finding as RustFinding, AnalysisResult as RustAnalysisResult, RiskSeverity, FindingType, Impact, BeamPipelineParser, AnalysisContext, Reporter, JsonReporter, TextReporter};
use pybeamguard_core::analyzers::registry::{create_analyzers, create_analyzers_by_names};
use std::collections::HashMap;

// ============================================================================
// ENUMS: RiskSeverity
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub enum PyRiskSeverity {
    #[pyo3(name = "INFO")]
    Info,
    #[pyo3(name = "LOW")]
    Low,
    #[pyo3(name = "MEDIUM")]
    Medium,
    #[pyo3(name = "HIGH")]
    High,
    #[pyo3(name = "CRITICAL")]
    Critical,
}

#[pymethods]
impl PyRiskSeverity {
    fn score(&self) -> u32 {
        match self {
            PyRiskSeverity::Info => 1,
            PyRiskSeverity::Low => 2,
            PyRiskSeverity::Medium => 3,
            PyRiskSeverity::High => 4,
            PyRiskSeverity::Critical => 5,
        }
    }

    fn __str__(&self) -> String {
        match self {
            PyRiskSeverity::Info => "INFO".to_string(),
            PyRiskSeverity::Low => "LOW".to_string(),
            PyRiskSeverity::Medium => "MEDIUM".to_string(),
            PyRiskSeverity::High => "HIGH".to_string(),
            PyRiskSeverity::Critical => "CRITICAL".to_string(),
        }
    }

    fn __repr__(&self) -> String {
        format!("RiskSeverity.{}", self.__str__())
    }
}

// ============================================================================
// CLASSES: Impact
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyImpact {
    pub latency_multiplier: Option<f64>,
    pub cost_delta_monthly: Option<f64>,
    pub affected_records_percent: Option<f64>,
}

#[pymethods]
impl PyImpact {
    #[new]
    fn new(
        latency_multiplier: Option<f64>,
        cost_delta_monthly: Option<f64>,
        affected_records_percent: Option<f64>,
    ) -> Self {
        PyImpact {
            latency_multiplier,
            cost_delta_monthly,
            affected_records_percent,
        }
    }

    #[getter]
    fn get_latency_multiplier(&self) -> Option<f64> {
        self.latency_multiplier
    }

    #[getter]
    fn get_cost_delta_monthly(&self) -> Option<f64> {
        self.cost_delta_monthly
    }

    #[getter]
    fn get_affected_records_percent(&self) -> Option<f64> {
        self.affected_records_percent
    }

    fn __repr__(&self) -> String {
        format!(
            "Impact(latency_multiplier={:?}, cost_delta_monthly={:?}, affected_records_percent={:?})",
            self.latency_multiplier, self.cost_delta_monthly, self.affected_records_percent
        )
    }
}

// ============================================================================
// CLASSES: Finding
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyFinding {
    pub id: String,
    pub severity: PyRiskSeverity,
    pub title: String,
    pub description: String,
    pub affected_nodes: Vec<String>,
    pub recommendation: Option<String>,
    pub estimated_impact: Option<PyImpact>,
    pub confidence: f32,
}

#[pymethods]
impl PyFinding {
    #[getter]
    fn get_id(&self) -> String {
        self.id.clone()
    }

    #[getter]
    fn get_severity(&self) -> PyRiskSeverity {
        self.severity.clone()
    }

    #[getter]
    fn get_title(&self) -> String {
        self.title.clone()
    }

    #[getter]
    fn get_description(&self) -> String {
        self.description.clone()
    }

    #[getter]
    fn get_affected_nodes(&self) -> Vec<String> {
        self.affected_nodes.clone()
    }

    #[getter]
    fn get_recommendation(&self) -> Option<String> {
        self.recommendation.clone()
    }

    #[getter]
    fn get_estimated_impact(&self) -> Option<PyImpact> {
        self.estimated_impact.clone()
    }

    #[getter]
    fn get_confidence(&self) -> f32 {
        self.confidence
    }

    fn __repr__(&self) -> String {
        format!(
            "Finding(id='{}', severity={}, title='{}', confidence={})",
            self.id,
            self.severity.__str__(),
            self.title,
            self.confidence
        )
    }
}

// ============================================================================
// CLASSES: AnalysisResult
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyAnalysisResult {
    pub analyzer_name: String,
    pub version: String,
    pub findings: Vec<PyFinding>,
    pub metrics: HashMap<String, f64>,
    pub summary: String,
    pub confidence: f32,
}

#[pymethods]
impl PyAnalysisResult {
    #[getter]
    fn get_analyzer_name(&self) -> String {
        self.analyzer_name.clone()
    }

    #[getter]
    fn get_version(&self) -> String {
        self.version.clone()
    }

    #[getter]
    fn get_findings(&self) -> Vec<PyFinding> {
        self.findings.clone()
    }

    #[getter]
    fn get_metrics(&self) -> HashMap<String, f64> {
        self.metrics.clone()
    }

    #[getter]
    fn get_summary(&self) -> String {
        self.summary.clone()
    }

    #[getter]
    fn get_confidence(&self) -> f32 {
        self.confidence
    }

    fn critical_findings(&self) -> Vec<PyFinding> {
        self.findings
            .iter()
            .filter(|f| matches!(f.severity, PyRiskSeverity::Critical))
            .cloned()
            .collect()
    }

    fn high_findings(&self) -> Vec<PyFinding> {
        self.findings
            .iter()
            .filter(|f| matches!(f.severity, PyRiskSeverity::High))
            .cloned()
            .collect()
    }

    fn total_score(&self) -> u32 {
        self.findings.iter().map(|f| f.severity.score()).sum()
    }

    fn finding_count(&self) -> usize {
        self.findings.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "AnalysisResult(analyzer='{}', version='{}', findings={}, confidence={})",
            self.analyzer_name,
            self.version,
            self.findings.len(),
            self.confidence
        )
    }
}

// ============================================================================
// CLASSES: TransformNode
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyTransformNode {
    pub id: String,
    pub name: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub annotations: Vec<String>,
    pub line_number: Option<usize>,
    pub node_type: String,
}

#[pymethods]
impl PyTransformNode {
    #[getter]
    fn get_id(&self) -> String {
        self.id.clone()
    }

    #[getter]
    fn get_name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn get_node_type(&self) -> String {
        self.node_type.clone()
    }

    #[getter]
    fn get_inputs(&self) -> Vec<String> {
        self.inputs.clone()
    }

    #[getter]
    fn get_outputs(&self) -> Vec<String> {
        self.outputs.clone()
    }

    #[getter]
    fn get_annotations(&self) -> Vec<String> {
        self.annotations.clone()
    }

    #[getter]
    fn get_line_number(&self) -> Option<usize> {
        self.line_number
    }

    fn __repr__(&self) -> String {
        format!(
            "TransformNode(id='{}', name='{}', type='{}')",
            self.id, self.name, self.node_type
        )
    }
}

// ============================================================================
// CLASSES: PipelineIR
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyPipelineIR {
    pub id: String,
    pub name: String,
    pub nodes: Vec<PyTransformNode>,
    pub edges: Vec<(String, String)>, // (from, to)
}

#[pymethods]
impl PyPipelineIR {
    #[getter]
    fn get_id(&self) -> String {
        self.id.clone()
    }

    #[getter]
    fn get_name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn get_nodes(&self) -> Vec<PyTransformNode> {
        self.nodes.clone()
    }

    #[getter]
    fn get_edges(&self) -> Vec<(String, String)> {
        self.edges.clone()
    }

    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn edge_count(&self) -> usize {
        self.edges.len()
    }

    fn get_node(&self, node_id: &str) -> Option<PyTransformNode> {
        self.nodes.iter().find(|n| n.id == node_id).cloned()
    }

    fn get_source_nodes(&self) -> Vec<PyTransformNode> {
        self.nodes
            .iter()
            .filter(|n| n.node_type.contains("Source"))
            .cloned()
            .collect()
    }

    fn get_sink_nodes(&self) -> Vec<PyTransformNode> {
        self.nodes
            .iter()
            .filter(|n| n.node_type.contains("Sink"))
            .cloned()
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "PipelineIR(name='{}', nodes={}, edges={})",
            self.name,
            self.nodes.len(),
            self.edges.len()
        )
    }
}

// ============================================================================
// HELPER FUNCTIONS: Type Conversion
// ============================================================================

fn convert_risk_severity(rs: &RiskSeverity) -> PyRiskSeverity {
    match rs {
        RiskSeverity::Info => PyRiskSeverity::Info,
        RiskSeverity::Low => PyRiskSeverity::Low,
        RiskSeverity::Medium => PyRiskSeverity::Medium,
        RiskSeverity::High => PyRiskSeverity::High,
        RiskSeverity::Critical => PyRiskSeverity::Critical,
    }
}

fn convert_impact(impact: &Impact) -> PyImpact {
    PyImpact {
        latency_multiplier: impact.latency_multiplier,
        cost_delta_monthly: impact.cost_delta_monthly,
        affected_records_percent: impact.affected_records_percent,
    }
}

fn convert_finding(finding: &RustFinding) -> PyFinding {
    PyFinding {
        id: finding.id.clone(),
        severity: convert_risk_severity(&finding.severity),
        title: finding.title.clone(),
        description: finding.description.clone(),
        affected_nodes: finding.affected_nodes.clone(),
        recommendation: finding.recommendation.clone(),
        estimated_impact: finding.estimated_impact.as_ref().map(convert_impact),
        confidence: finding.confidence,
    }
}

fn convert_analysis_result(result: &RustAnalysisResult) -> PyAnalysisResult {
    PyAnalysisResult {
        analyzer_name: result.analyzer_name.clone(),
        version: result.version.clone(),
        findings: result.findings.iter().map(convert_finding).collect(),
        metrics: result.metrics.clone(),
        summary: result.summary.clone(),
        confidence: result.confidence,
    }
}

fn convert_transform_node(node: &RustTransformNode) -> PyTransformNode {
    let node_type_str = format!("{:?}", node.node_type);

    PyTransformNode {
        id: node.id.clone(),
        name: node.name.clone(),
        inputs: node.inputs.clone(),
        outputs: node.outputs.clone(),
        annotations: node.annotations.clone(),
        line_number: node.line_number,
        node_type: node_type_str,
    }
}

fn convert_pipeline_ir(ir: &RustPipelineIR) -> PyPipelineIR {
    let edges = ir
        .edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();

    PyPipelineIR {
        id: ir.id.clone(),
        name: ir.name.clone(),
        nodes: ir.nodes.iter().map(convert_transform_node).collect(),
        edges,
    }
}

// ============================================================================
// CORE API: analyze (backward compatible)
// ============================================================================

#[pyfunction]
#[pyo3(signature = (code, data_profile=None))]
fn analyze(code: String, data_profile: Option<String>) -> PyResult<String> {
    let profile = if let Some(profile_json) = data_profile {
        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(p) => Some(p),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid data profile JSON: {}",
                    e
                )))
            }
        }
    } else {
        None
    };

    let results = analyze_pipeline(&code, profile)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    serde_json::to_string(&results)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

// ============================================================================
// ENHANCED API: analyze_structured (NEW)
// ============================================================================

#[pyfunction]
#[pyo3(signature = (code, data_profile=None))]
fn analyze_structured(
    code: String,
    data_profile: Option<String>,
) -> PyResult<Vec<PyAnalysisResult>> {
    let profile = if let Some(profile_json) = data_profile {
        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(p) => Some(p),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid data profile JSON: {}",
                    e
                )))
            }
        }
    } else {
        None
    };

    let results = analyze_pipeline(&code, profile)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok(results.iter().map(convert_analysis_result).collect())
}

// ============================================================================
// ENHANCED API: parse_pipeline (NEW) - Get pipeline IR structure
// ============================================================================

#[pyfunction]
fn parse_pipeline(code: String) -> PyResult<PyPipelineIR> {
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(&code)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok(convert_pipeline_ir(&ir))
}

// ============================================================================
// PHASE 3: AnalyzerInfo - Information about available analyzers
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyAnalyzerInfo {
    pub name: String,
    pub version: String,
    pub priority: u32,
}

#[pymethods]
impl PyAnalyzerInfo {
    #[getter]
    fn get_name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn get_version(&self) -> String {
        self.version.clone()
    }

    #[getter]
    fn get_priority(&self) -> u32 {
        self.priority
    }

    fn __repr__(&self) -> String {
        format!("AnalyzerInfo(name='{}', version='{}', priority={})", self.name, self.version, self.priority)
    }
}

// ============================================================================
// PHASE 3: get_available_analyzers - List all analyzers
// ============================================================================

#[pyfunction]
fn get_available_analyzers() -> PyResult<Vec<PyAnalyzerInfo>> {
    let analyzers = create_analyzers();
    let infos: Vec<PyAnalyzerInfo> = analyzers
        .iter()
        .map(|a| PyAnalyzerInfo {
            name: a.name().to_string(),
            version: a.version().to_string(),
            priority: a.priority(),
        })
        .collect();

    Ok(infos)
}

// ============================================================================
// PHASE 3: analyze_with_analyzers - Selective analyzer execution
// ============================================================================

#[pyfunction]
#[pyo3(signature = (code, analyzer_names, data_profile=None))]
fn analyze_with_analyzers(
    code: String,
    analyzer_names: Vec<String>,
    data_profile: Option<String>,
) -> PyResult<Vec<PyAnalysisResult>> {
    let profile = if let Some(profile_json) = data_profile {
        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(p) => Some(p),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid data profile JSON: {}",
                    e
                )))
            }
        }
    } else {
        None
    };

    // Parse pipeline
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(&code)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Filter analyzers by name
    let names: Vec<&str> = analyzer_names.iter().map(|s| s.as_str()).collect();
    let analyzers = create_analyzers_by_names(&names);

    if analyzers.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("No analyzers matched: {:?}. Available: GraphAnalyzer, HotKeyAnalyzer, ShuffleAnalyzer, WindowingAnalyzer, StateAnalyzer, CostAnalyzer, ReliabilityAnalyzer, BestPracticesAnalyzer, DeploymentAnalyzer, SynthesisEngine", analyzer_names)
        ));
    }

    // Run selected analyzers
    let mut results = Vec::new();
    for analyzer in analyzers {
        match analyzer.analyze(&ir) {
            Ok(result) => results.push(convert_analysis_result(&result)),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Analyzer {} failed: {}", analyzer.name(), e)
                ))
            }
        }
    }

    Ok(results)
}

// ============================================================================
// PHASE 4: Reporter API - Format analysis results
// ============================================================================

#[pyfunction]
fn get_json_report(code: String, data_profile: Option<String>) -> PyResult<String> {
    let profile = if let Some(profile_json) = data_profile {
        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(p) => Some(p),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid data profile JSON: {}",
                    e
                )))
            }
        }
    } else {
        None
    };

    let results = analyze_pipeline(&code, profile)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let reporter = JsonReporter;
    Ok(reporter.format(&results))
}

// ============================================================================
// PHASE 4: Reporter API - Format as human-readable text
// ============================================================================

#[pyfunction]
fn get_text_report(code: String, data_profile: Option<String>) -> PyResult<String> {
    let profile = if let Some(profile_json) = data_profile {
        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(p) => Some(p),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid data profile JSON: {}",
                    e
                )))
            }
        }
    } else {
        None
    };

    let results = analyze_pipeline(&code, profile)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let reporter = TextReporter;
    Ok(reporter.format(&results))
}

// ============================================================================
// PHASE 4: Reporter API - Format results with selected analyzers
// ============================================================================

#[pyfunction]
#[pyo3(signature = (code, analyzer_names, report_format = "json", data_profile=None))]
fn analyze_and_format(
    code: String,
    analyzer_names: Vec<String>,
    report_format: &str,
    data_profile: Option<String>,
) -> PyResult<String> {
    let profile = if let Some(profile_json) = data_profile {
        match serde_json::from_str::<DataProfile>(&profile_json) {
            Ok(p) => Some(p),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid data profile JSON: {}",
                    e
                )))
            }
        }
    } else {
        None
    };

    // Parse pipeline
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(&code)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Filter analyzers by name
    let names: Vec<&str> = analyzer_names.iter().map(|s| s.as_str()).collect();
    let analyzers = create_analyzers_by_names(&names);

    if analyzers.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("No analyzers matched: {:?}", analyzer_names)
        ));
    }

    // Run selected analyzers
    let mut results = Vec::new();
    for analyzer in analyzers {
        match analyzer.analyze(&ir) {
            Ok(result) => results.push(result),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Analyzer failed: {}", e)
                ))
            }
        }
    }

    // Format based on requested format
    match report_format {
        "json" => {
            let reporter = JsonReporter;
            Ok(reporter.format(&results))
        }
        "text" => {
            let reporter = TextReporter;
            Ok(reporter.format(&results))
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Unknown format '{}'. Use 'json' or 'text'", report_format)
        )),
    }
}

// ============================================================================
// PHASE 5: Enhanced pipeline inspection - Get multiple aspects
// ============================================================================

#[pyfunction]
fn get_pipeline_complexity_score(code: String) -> PyResult<f64> {
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(&code)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let nodes = ir.nodes.len() as f64;
    let edges = ir.edges.len() as f64;

    // Simple complexity: nodes + (edges * 0.5)
    Ok(nodes + (edges * 0.5))
}

// ============================================================================
// PHASE 5: Get pipeline summary without full analysis
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyPipelineSummary {
    pub name: String,
    pub node_count: usize,
    pub edge_count: usize,
    pub source_count: usize,
    pub sink_count: usize,
    pub complexity: f64,
}

#[pymethods]
impl PyPipelineSummary {
    #[getter]
    fn get_name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn get_node_count(&self) -> usize {
        self.node_count
    }

    #[getter]
    fn get_edge_count(&self) -> usize {
        self.edge_count
    }

    #[getter]
    fn get_source_count(&self) -> usize {
        self.source_count
    }

    #[getter]
    fn get_sink_count(&self) -> usize {
        self.sink_count
    }

    #[getter]
    fn get_complexity(&self) -> f64 {
        self.complexity
    }

    fn __repr__(&self) -> String {
        format!(
            "PipelineSummary(name='{}', nodes={}, edges={}, sources={}, sinks={}, complexity={})",
            self.name, self.node_count, self.edge_count, self.source_count, self.sink_count,
            self.complexity
        )
    }
}

#[pyfunction]
fn get_pipeline_summary(code: String) -> PyResult<PyPipelineSummary> {
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(&code)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let source_count = ir.get_source_nodes().len();
    let sink_count = ir.get_sink_nodes().len();
    let complexity = ir.nodes.len() as f64 + (ir.edges.len() as f64 * 0.5);

    Ok(PyPipelineSummary {
        name: ir.name.clone(),
        node_count: ir.nodes.len(),
        edge_count: ir.edges.len(),
        source_count,
        sink_count,
        complexity,
    })
}

// ============================================================================
// PHASE 6: Result Filtering - Filter findings by severity
// ============================================================================

#[pyfunction]
fn filter_findings_by_severity(results: Vec<PyAnalysisResult>, min_severity: u32) -> PyResult<Vec<PyAnalysisResult>> {
    let filtered: Vec<PyAnalysisResult> = results
        .into_iter()
        .map(|mut result| {
            result.findings.retain(|f| f.severity.score() >= min_severity);
            result
        })
        .collect();

    Ok(filtered)
}

// ============================================================================
// PHASE 6: Pipeline node type analysis
// ============================================================================

#[pyfunction]
fn get_node_types_in_pipeline(code: String) -> PyResult<std::collections::HashMap<String, usize>> {
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(&code)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let mut type_counts: HashMap<String, usize> = HashMap::new();
    for node in &ir.nodes {
        let node_type = format!("{:?}", node.node_type);
        *type_counts.entry(node_type).or_insert(0) += 1;
    }

    Ok(type_counts)
}

// ============================================================================
// PHASE 6: Parser validation without analysis
// ============================================================================

#[pyfunction]
fn validate_pipeline_syntax(code: String) -> PyResult<bool> {
    let parser = BeamPipelineParser::new();
    match parser.parse(&code) {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

// ============================================================================
// PHASE 6: Get pipeline statistics
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyPipelineStats {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub source_count: usize,
    pub sink_count: usize,
    pub avg_node_inputs: f64,
    pub avg_node_outputs: f64,
    pub max_depth: usize,
}

#[pymethods]
impl PyPipelineStats {
    #[getter]
    fn get_total_nodes(&self) -> usize {
        self.total_nodes
    }

    #[getter]
    fn get_total_edges(&self) -> usize {
        self.total_edges
    }

    #[getter]
    fn get_source_count(&self) -> usize {
        self.source_count
    }

    #[getter]
    fn get_sink_count(&self) -> usize {
        self.sink_count
    }

    #[getter]
    fn get_avg_node_inputs(&self) -> f64 {
        self.avg_node_inputs
    }

    #[getter]
    fn get_avg_node_outputs(&self) -> f64 {
        self.avg_node_outputs
    }

    #[getter]
    fn get_max_depth(&self) -> usize {
        self.max_depth
    }

    fn __repr__(&self) -> String {
        format!(
            "PipelineStats(nodes={}, edges={}, sources={}, sinks={}, depth={})",
            self.total_nodes, self.total_edges, self.source_count, self.sink_count, self.max_depth
        )
    }
}

#[pyfunction]
fn get_pipeline_stats(code: String) -> PyResult<PyPipelineStats> {
    let parser = BeamPipelineParser::new();
    let ir = parser.parse(&code)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let source_count = ir.get_source_nodes().len();
    let sink_count = ir.get_sink_nodes().len();

    let avg_inputs = if ir.nodes.is_empty() {
        0.0
    } else {
        ir.nodes.iter().map(|n| n.inputs.len()).sum::<usize>() as f64 / ir.nodes.len() as f64
    };

    let avg_outputs = if ir.nodes.is_empty() {
        0.0
    } else {
        ir.nodes.iter().map(|n| n.outputs.len()).sum::<usize>() as f64 / ir.nodes.len() as f64
    };

    // Simple max depth calculation
    let max_depth = if ir.edges.is_empty() {
        1
    } else {
        ir.edges.len()
    };

    Ok(PyPipelineStats {
        total_nodes: ir.nodes.len(),
        total_edges: ir.edges.len(),
        source_count,
        sink_count,
        avg_node_inputs: avg_inputs,
        avg_node_outputs: avg_outputs,
        max_depth,
    })
}

// ============================================================================
// PHASE 6: Result severity summary
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PySeveritySummary {
    pub critical_count: usize,
    pub high_count: usize,
    pub medium_count: usize,
    pub low_count: usize,
    pub info_count: usize,
}

#[pymethods]
impl PySeveritySummary {
    #[getter]
    fn get_critical_count(&self) -> usize {
        self.critical_count
    }

    #[getter]
    fn get_high_count(&self) -> usize {
        self.high_count
    }

    #[getter]
    fn get_medium_count(&self) -> usize {
        self.medium_count
    }

    #[getter]
    fn get_low_count(&self) -> usize {
        self.low_count
    }

    #[getter]
    fn get_info_count(&self) -> usize {
        self.info_count
    }

    fn __repr__(&self) -> String {
        format!(
            "SeveritySummary(critical={}, high={}, medium={}, low={}, info={})",
            self.critical_count, self.high_count, self.medium_count, self.low_count, self.info_count
        )
    }
}

#[pyfunction]
fn get_severity_summary(results: Vec<PyAnalysisResult>) -> PyResult<PySeveritySummary> {
    let mut summary = PySeveritySummary {
        critical_count: 0,
        high_count: 0,
        medium_count: 0,
        low_count: 0,
        info_count: 0,
    };

    for result in &results {
        for finding in &result.findings {
            match finding.severity {
                PyRiskSeverity::Critical => summary.critical_count += 1,
                PyRiskSeverity::High => summary.high_count += 1,
                PyRiskSeverity::Medium => summary.medium_count += 1,
                PyRiskSeverity::Low => summary.low_count += 1,
                PyRiskSeverity::Info => summary.info_count += 1,
            }
        }
    }

    Ok(summary)
}

// ============================================================================
// PHASE 7: Finding deduplication - Remove duplicate findings
// ============================================================================

#[pyfunction]
fn deduplicate_findings(results: Vec<PyAnalysisResult>) -> PyResult<Vec<PyAnalysisResult>> {
    let mut deduplicated = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();

    for result in results {
        let mut unique_findings = Vec::new();
        for finding in result.findings {
            if seen_ids.insert(finding.id.clone()) {
                unique_findings.push(finding);
            }
        }
        deduplicated.push(PyAnalysisResult {
            analyzer_name: result.analyzer_name,
            version: result.version,
            findings: unique_findings,
            metrics: result.metrics,
            summary: result.summary,
            confidence: result.confidence,
        });
    }

    Ok(deduplicated)
}

// ============================================================================
// PHASE 7: Result comparison - Compare two analysis runs
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyResultComparison {
    pub findings_added: usize,
    pub findings_removed: usize,
    pub findings_unchanged: usize,
    pub severity_changed: usize,
}

#[pymethods]
impl PyResultComparison {
    #[getter]
    fn get_findings_added(&self) -> usize {
        self.findings_added
    }

    #[getter]
    fn get_findings_removed(&self) -> usize {
        self.findings_removed
    }

    #[getter]
    fn get_findings_unchanged(&self) -> usize {
        self.findings_unchanged
    }

    #[getter]
    fn get_severity_changed(&self) -> usize {
        self.severity_changed
    }

    fn __repr__(&self) -> String {
        format!(
            "ResultComparison(added={}, removed={}, unchanged={}, severity_changed={})",
            self.findings_added, self.findings_removed, self.findings_unchanged, self.severity_changed
        )
    }
}

#[pyfunction]
fn compare_results(
    old_results: Vec<PyAnalysisResult>,
    new_results: Vec<PyAnalysisResult>,
) -> PyResult<PyResultComparison> {
    let old_ids: std::collections::HashSet<_> = old_results
        .iter()
        .flat_map(|r| r.findings.iter().map(|f| f.id.clone()))
        .collect();

    let new_ids: std::collections::HashSet<_> = new_results
        .iter()
        .flat_map(|r| r.findings.iter().map(|f| f.id.clone()))
        .collect();

    let added = new_ids.iter().filter(|id| !old_ids.contains(*id)).count();
    let removed = old_ids.iter().filter(|id| !new_ids.contains(*id)).count();
    let unchanged = new_ids.iter().filter(|id| old_ids.contains(*id)).count();

    Ok(PyResultComparison {
        findings_added: added,
        findings_removed: removed,
        findings_unchanged: unchanged,
        severity_changed: 0, // Simplified for this phase
    })
}

// ============================================================================
// PHASE 7: Analyzer ranking - Rank analyzers by finding count
// ============================================================================

#[pyclass]
#[derive(Clone)]
pub struct PyAnalyzerRanking {
    pub analyzer_name: String,
    pub finding_count: usize,
    pub avg_confidence: f32,
}

#[pymethods]
impl PyAnalyzerRanking {
    #[getter]
    fn get_analyzer_name(&self) -> String {
        self.analyzer_name.clone()
    }

    #[getter]
    fn get_finding_count(&self) -> usize {
        self.finding_count
    }

    #[getter]
    fn get_avg_confidence(&self) -> f32 {
        self.avg_confidence
    }

    fn __repr__(&self) -> String {
        format!(
            "AnalyzerRanking(analyzer='{}', findings={}, confidence={})",
            self.analyzer_name, self.finding_count, self.avg_confidence
        )
    }
}

#[pyfunction]
fn rank_analyzers_by_impact(results: Vec<PyAnalysisResult>) -> PyResult<Vec<PyAnalyzerRanking>> {
    let mut rankings = Vec::new();

    for result in results {
        if result.findings.is_empty() {
            continue;
        }

        let avg_confidence = result.findings.iter().map(|f| f.confidence).sum::<f32>()
            / result.findings.len() as f32;

        rankings.push(PyAnalyzerRanking {
            analyzer_name: result.analyzer_name,
            finding_count: result.findings.len(),
            avg_confidence,
        });
    }

    rankings.sort_by(|a, b| b.finding_count.cmp(&a.finding_count));
    Ok(rankings)
}

// ============================================================================
// PHASE 7: Finding search - Search findings by title/description
// ============================================================================

#[pyfunction]
fn search_findings(results: Vec<PyAnalysisResult>, query: &str) -> PyResult<Vec<PyFinding>> {
    let mut matches = Vec::new();
    let query_lower = query.to_lowercase();

    for result in results {
        for finding in result.findings {
            if finding.title.to_lowercase().contains(&query_lower)
                || finding.description.to_lowercase().contains(&query_lower)
            {
                matches.push(finding);
            }
        }
    }

    Ok(matches)
}

// ============================================================================
// PHASE 7: Result aggregation - Merge multiple analysis results
// ============================================================================

#[pyfunction]
fn merge_results(result_sets: Vec<Vec<PyAnalysisResult>>) -> PyResult<Vec<PyAnalysisResult>> {
    let mut merged: std::collections::HashMap<String, PyAnalysisResult> =
        std::collections::HashMap::new();

    for results in result_sets {
        for result in results {
            merged
                .entry(result.analyzer_name.clone())
                .or_insert_with(|| result.clone())
                .findings
                .extend(result.findings);
        }
    }

    let mut final_results: Vec<PyAnalysisResult> = merged.into_values().collect();
    final_results.sort_by(|a, b| a.analyzer_name.cmp(&b.analyzer_name));

    Ok(final_results)
}

// ============================================================================
// PYMODULE: Registration
// ============================================================================

#[pymodule]
fn pybeamguard(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.9.0")?;
    m.add(
        "__doc__",
        "Apache Beam & Dataflow pipeline analysis with result comparison and analytics",
    )?;

    // Register classes (Phase 1-2)
    m.add_class::<PyRiskSeverity>()?;
    m.add_class::<PyImpact>()?;
    m.add_class::<PyFinding>()?;
    m.add_class::<PyAnalysisResult>()?;
    m.add_class::<PyTransformNode>()?;
    m.add_class::<PyPipelineIR>()?;

    // Register Phase 3 classes
    m.add_class::<PyAnalyzerInfo>()?;

    // Register Phase 4-5 classes
    m.add_class::<PyPipelineSummary>()?;

    // Register Phase 6 classes
    m.add_class::<PyPipelineStats>()?;
    m.add_class::<PySeveritySummary>()?;

    // Register Phase 7 classes
    m.add_class::<PyResultComparison>()?;
    m.add_class::<PyAnalyzerRanking>()?;

    // Register functions (Phase 1-2)
    m.add_function(wrap_pyfunction!(analyze, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_structured, m)?)?;
    m.add_function(wrap_pyfunction!(parse_pipeline, m)?)?;

    // Register Phase 3 functions
    m.add_function(wrap_pyfunction!(get_available_analyzers, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_with_analyzers, m)?)?;

    // Register Phase 4 functions (Reporters)
    m.add_function(wrap_pyfunction!(get_json_report, m)?)?;
    m.add_function(wrap_pyfunction!(get_text_report, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_and_format, m)?)?;

    // Register Phase 5 functions (Pipeline inspection)
    m.add_function(wrap_pyfunction!(get_pipeline_complexity_score, m)?)?;
    m.add_function(wrap_pyfunction!(get_pipeline_summary, m)?)?;

    // Register Phase 6 functions (Filtering & statistics)
    m.add_function(wrap_pyfunction!(filter_findings_by_severity, m)?)?;
    m.add_function(wrap_pyfunction!(get_node_types_in_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(validate_pipeline_syntax, m)?)?;
    m.add_function(wrap_pyfunction!(get_pipeline_stats, m)?)?;
    m.add_function(wrap_pyfunction!(get_severity_summary, m)?)?;

    // Register Phase 7 functions (Result analysis & comparison)
    m.add_function(wrap_pyfunction!(deduplicate_findings, m)?)?;
    m.add_function(wrap_pyfunction!(compare_results, m)?)?;
    m.add_function(wrap_pyfunction!(rank_analyzers_by_impact, m)?)?;
    m.add_function(wrap_pyfunction!(search_findings, m)?)?;
    m.add_function(wrap_pyfunction!(merge_results, m)?)?;

    Ok(())
}
