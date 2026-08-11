use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct SynthesisEngine;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutiveSummary {
    pub overall_risk_score: f32,
    pub performance_score: f32,
    pub reliability_score: f32,
    pub cost_efficiency_score: f32,
    pub top_risks: Vec<Finding>,
    pub cost_analysis: CostSummary,
    pub recommendations: Vec<Recommendation>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CostSummary {
    pub estimated_monthly_cost: f64,
    pub cost_categories: HashMap<String, f64>,
    pub cost_hotspots: Vec<CostHotspot>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CostHotspot {
    pub stage: String,
    pub estimated_cost: f64,
    pub percentage_of_total: f32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Recommendation {
    pub priority: u32,
    pub title: String,
    pub description: String,
    pub estimated_impact: Impact,
    pub implementation_effort_hours: u32,
    pub roi_multiplier: f32,
}

impl Analyzer for SynthesisEngine {
    fn name(&self) -> &str {
        "SynthesisEngine"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        10 // Run last, after all other analyzers
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        // Calculate component scores
        let performance_score = self.calculate_performance_score(ir);
        let reliability_score = self.calculate_reliability_score(ir);
        let cost_efficiency_score = self.calculate_cost_efficiency_score(ir);

        // Overall score: weighted average
        let overall_risk_score =
            performance_score * 0.35 + reliability_score * 0.35 + cost_efficiency_score * 0.30;

        metrics.insert("overall_risk_score".to_string(), overall_risk_score as f64);
        metrics.insert("performance_score".to_string(), performance_score as f64);
        metrics.insert("reliability_score".to_string(), reliability_score as f64);
        metrics.insert(
            "cost_efficiency_score".to_string(),
            cost_efficiency_score as f64,
        );

        // Determine overall risk level
        let risk_level = if overall_risk_score >= 80.0 {
            "EXCELLENT - Production Ready"
        } else if overall_risk_score >= 60.0 {
            "GOOD - Minor Issues"
        } else if overall_risk_score >= 40.0 {
            "WARNING - Address Before Production"
        } else {
            "CRITICAL - Do Not Deploy"
        };

        let summary = format!(
            "PyBeamGuard Executive Summary: Overall Risk Score {:.0}/100 ({}) | \
             Performance {:.0} | Reliability {:.0} | Cost Efficiency {:.0}",
            overall_risk_score,
            risk_level,
            performance_score,
            reliability_score,
            cost_efficiency_score
        );

        findings.push(Finding {
            id: "SYNTHESIS_EXECUTIVE_SUMMARY".to_string(),
            severity: match overall_risk_score {
                x if x >= 80.0 => RiskSeverity::Info,
                x if x >= 60.0 => RiskSeverity::Low,
                x if x >= 40.0 => RiskSeverity::Medium,
                _ => RiskSeverity::Critical,
            },
            finding_type: FindingType::ConfigurationIssue,
            title: "PyBeamGuard Analysis Complete".to_string(),
            description: format!("Risk Score Breakdown:\n- Performance: {:.0}/100\n- Reliability: {:.0}/100\n- Cost Efficiency: {:.0}/100",
                performance_score, reliability_score, cost_efficiency_score),
            affected_nodes: vec![],
            recommendation: Some(
                if overall_risk_score < 60.0 {
                    "Address critical findings before deploying to production.".to_string()
                } else {
                    "Pipeline appears ready for production. Monitor metrics after deployment.".to_string()
                }
            ),
            estimated_impact: None,
            confidence: 0.90,
        });

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary,
            confidence: 0.90,
        })
    }
}

impl SynthesisEngine {
    fn calculate_performance_score(&self, ir: &PipelineIR) -> f32 {
        let mut score = 100.0;

        // Penalize for each shuffle operation (-10 per shuffle)
        let shuffle_count = ir
            .nodes
            .iter()
            .filter(|n| n.node_type.is_shuffle_operation())
            .count();
        score -= (shuffle_count as f32) * 10.0;

        // Penalize for excessive depth (-5 per level beyond 5)
        let depth = ir.nodes.len(); // Simplified
        if depth > 5 {
            score -= ((depth - 5) as f32) * 5.0;
        }

        // Bonus for CombinePerKey usage (+10 per)
        let combine_count = ir
            .nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::CombinePerKey { .. }))
            .count();
        score += (combine_count as f32) * 10.0;

        // Penalize for hot key risk (-15)
        let groupbykey_count = ir
            .nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::GroupByKey { .. }))
            .count();
        if groupbykey_count > 0 {
            score -= 15.0;
        }

        score.clamp(0.0, 100.0)
    }

    fn calculate_reliability_score(&self, ir: &PipelineIR) -> f32 {
        let mut score = 100.0;

        // Penalize for missing sink (-50)
        if ir.get_sink_nodes().is_empty() {
            score -= 50.0;
        }

        // Penalize for stateful operations without proper error handling (-15)
        let stateful_count = ir
            .nodes
            .iter()
            .filter(|n| n.node_type.is_stateful())
            .count();
        if stateful_count > 0 {
            score -= 15.0;
        }

        // Penalize for parsing-like operations without side outputs (-10)
        let parsing_ops = ir
            .nodes
            .iter()
            .filter(|n| {
                if let TransformType::ParDo { dofn_name, .. } = &n.node_type {
                    dofn_name.to_lowercase().contains("parse")
                } else {
                    false
                }
            })
            .count();
        if parsing_ops > 0 {
            score -= (parsing_ops as f32) * 10.0;
        }

        // Penalize for streaming without windowing (-20)
        let is_streaming = ir.get_source_nodes().iter().any(|s| {
            if let TransformType::Source { source_type } = &s.node_type {
                source_type.contains("pubsub") || source_type.contains("kafka")
            } else {
                false
            }
        });

        let has_windowing = ir
            .nodes
            .iter()
            .any(|n| matches!(n.node_type, TransformType::Windowing { .. }));

        if is_streaming && !has_windowing {
            score -= 20.0;
        }

        score.clamp(0.0, 100.0)
    }

    fn calculate_cost_efficiency_score(&self, ir: &PipelineIR) -> f32 {
        let mut score = 100.0;

        // Penalize for multiple shuffles (-20 per extra shuffle)
        let shuffle_count = ir
            .nodes
            .iter()
            .filter(|n| n.node_type.is_shuffle_operation())
            .count();
        if shuffle_count > 1 {
            score -= ((shuffle_count - 1) as f32) * 20.0;
        }

        // Penalize for stateful operations without time-based cleanup (-15 per operation)
        let stateful_count = ir
            .nodes
            .iter()
            .filter(|n| n.node_type.is_stateful())
            .count();
        score -= (stateful_count as f32) * 15.0;

        // Bonus for using Dataflow Shuffle (+20)
        if let Some(config) = &ir.metadata.deployment_config {
            if config.streaming_engine == Some(true) {
                score += 20.0;
            }
        }

        // Penalize for excessive min workers (-10 per 10 workers)
        if let Some(config) = &ir.metadata.deployment_config {
            if let Some(min_workers) = config.min_workers {
                if min_workers > 10 {
                    score -= ((min_workers - 10) / 10) as f32 * 10.0;
                }
            }
        }

        score.clamp(0.0, 100.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synthesis_scoring() {
        let ir = PipelineIR::new("test".to_string());
        let engine = SynthesisEngine;
        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = engine.analyze(&ctx).unwrap();

        // Score should be 0-100
        assert!(result.metrics.get("overall_risk_score").unwrap() >= &0.0);
        assert!(result.metrics.get("overall_risk_score").unwrap() <= &100.0);
    }
}
