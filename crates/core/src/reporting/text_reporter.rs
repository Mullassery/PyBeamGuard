use super::Reporter;
use crate::analyzer::{AnalysisResult, RiskSeverity};

pub struct TextReporter;

impl Reporter for TextReporter {
    fn format(&self, results: &[AnalysisResult]) -> String {
        let mut output = String::new();
        output.push_str("=== PyBeamGuard Analysis Report ===\n\n");

        let total_findings: usize = results.iter().map(|r| r.findings.len()).sum();
        let total_score: u32 = results.iter().map(|r| r.total_score()).sum();

        output.push_str(&format!(
            "Overall Risk Score: {}/100\n",
            (total_score as f32 / 50.0).min(100.0) as u32
        ));
        output.push_str(&format!("Total Findings: {}\n\n", total_findings));

        // Group findings by severity
        let mut critical: Vec<_> = Vec::new();
        let mut high: Vec<_> = Vec::new();
        let mut medium: Vec<_> = Vec::new();
        let mut low: Vec<_> = Vec::new();

        for result in results {
            for finding in &result.findings {
                match finding.severity {
                    RiskSeverity::Critical => critical.push(finding),
                    RiskSeverity::High => high.push(finding),
                    RiskSeverity::Medium => medium.push(finding),
                    RiskSeverity::Low => low.push(finding),
                    _ => {}
                }
            }
        }

        if !critical.is_empty() {
            output.push_str("🔴 CRITICAL ISSUES\n");
            output.push_str(&format!("Found {} critical issue(s):\n\n", critical.len()));
            for finding in critical {
                output.push_str(&format!("  • {}\n", finding.title));
                output.push_str(&format!("    {}\n", finding.description));
                if let Some(rec) = &finding.recommendation {
                    output.push_str(&format!("    → {}\n", rec));
                }
                output.push_str(&format!(
                    "    Confidence: {:.0}%\n\n",
                    finding.confidence * 100.0
                ));
            }
        }

        if !high.is_empty() {
            output.push_str("🟠 HIGH PRIORITY ISSUES\n");
            output.push_str(&format!("Found {} high-priority issue(s):\n\n", high.len()));
            for finding in high {
                output.push_str(&format!("  • {}\n", finding.title));
                if let Some(rec) = &finding.recommendation {
                    output.push_str(&format!("    → {}\n", rec));
                }
            }
            output.push('\n');
        }

        if !medium.is_empty() {
            output.push_str("🟡 MEDIUM PRIORITY ISSUES\n");
            output.push_str(&format!(
                "Found {} medium-priority issue(s). Review when convenient.\n\n",
                medium.len()
            ));
        }

        if !low.is_empty() {
            output.push_str("🟢 LOW PRIORITY ISSUES\n");
            output.push_str(&format!("Found {} low-priority issue(s).\n\n", low.len()));
        }

        // Metrics summary
        output.push_str("=== Analysis Metrics ===\n\n");
        for result in results {
            output.push_str(&format!("{}: {}\n", result.analyzer_name, result.summary));
            for (key, value) in &result.metrics {
                output.push_str(&format!("  {}: {:.2}\n", key, value));
            }
            output.push('\n');
        }

        output
    }
}
