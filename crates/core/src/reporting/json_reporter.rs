use super::Reporter;
use crate::analyzer::AnalysisResult;

pub struct JsonReporter;

impl Reporter for JsonReporter {
    fn format(&self, results: &[AnalysisResult]) -> String {
        serde_json::to_string_pretty(&results).unwrap_or_else(|_| "{}".to_string())
    }
}
