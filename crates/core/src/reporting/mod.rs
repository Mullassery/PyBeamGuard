pub mod json_reporter;
pub mod text_reporter;

pub use json_reporter::JsonReporter;
pub use text_reporter::TextReporter;

use crate::analyzer::AnalysisResult;

pub trait Reporter {
    fn format(&self, results: &[AnalysisResult]) -> String;
}
