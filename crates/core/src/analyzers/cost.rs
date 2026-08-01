use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct CostAnalyzer;

// Dataflow pricing (as of 2026, US region)
const DATAFLOW_SHUFFLE_COST_PER_GB: f64 = 0.30;
const WORKER_MACHINE_COST_PER_HOUR: f64 = 0.35;
const PERSISTENT_DISK_COST_PER_GB_MONTH: f64 = 0.30;

impl Analyzer for CostAnalyzer {
    fn name(&self) -> &str {
        "CostAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        6
    }

    fn analyze(&self, ir: &PipelineIR) -> anyhow::Result<AnalysisResult> {
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        // Estimate costs
        let (compute_cost, shuffle_cost, state_cost, total_cost) = self.estimate_costs(ir);

        metrics.insert("estimated_compute_cost_per_month".to_string(), compute_cost);
        metrics.insert("estimated_shuffle_cost_per_month".to_string(), shuffle_cost);
        metrics.insert("estimated_state_cost_per_month".to_string(), state_cost);
        metrics.insert("estimated_total_cost_per_month".to_string(), total_cost);

        // Find cost hotspots
        let shuffle_ops = ir.nodes
            .iter()
            .filter(|n| n.node_type.is_shuffle_operation())
            .count();

        if shuffle_ops > 3 {
            findings.push(Finding {
                id: "COST_MULTIPLE_SHUFFLES".to_string(),
                severity: RiskSeverity::High,
                finding_type: FindingType::CostRisk,
                title: format!("Multiple shuffle operations ({}) detected", shuffle_ops),
                description: "Each shuffle is expensive. Multiple shuffles multiply the cost.".to_string(),
                affected_nodes: vec![],
                recommendation: Some("Reorganize the pipeline to minimize shuffles. Pre-aggregate before joins when possible.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: Some(shuffle_cost * 0.4),
                    affected_records_percent: None,
                }),
                confidence: 0.80,
            });
        }

        // Check for stateful operations (PD cost)
        let stateful_ops = ir.nodes
            .iter()
            .filter(|n| n.node_type.is_stateful())
            .count();

        if stateful_ops > 0 {
            findings.push(Finding {
                id: "COST_STATE_STORAGE".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::CostRisk,
                title: format!("Stateful operations ({}) contribute to state storage cost", stateful_ops),
                description: format!(
                    "Estimated state storage cost: ${:.0}/month. This can grow significantly with long allowed lateness.",
                    state_cost
                ),
                affected_nodes: vec![],
                recommendation: Some("Reduce allowed lateness and implement state cleanup strategies to minimize state storage.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: None,
                    cost_delta_monthly: Some(state_cost * 0.3),
                    affected_records_percent: None,
                }),
                confidence: 0.70,
            });
        }

        // Overall cost assessment
        let cost_tier = if total_cost < 100.0 {
            "low"
        } else if total_cost < 500.0 {
            "moderate"
        } else if total_cost < 2000.0 {
            "high"
        } else {
            "very high"
        };

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: format!(
                "Estimated monthly cost: ${:.0} ({}) | Compute: ${:.0} | Shuffle: ${:.0} | State: ${:.0}",
                total_cost, cost_tier, compute_cost, shuffle_cost, state_cost
            ),
            confidence: 0.65, // Cost estimation has inherent uncertainty
        })
    }
}

impl CostAnalyzer {
    fn estimate_costs(&self, ir: &PipelineIR) -> (f64, f64, f64, f64) {
        // Heuristic-based estimation
        let base_worker_hours = (ir.nodes.len() as f64) * 10.0; // Rough estimate: 10h per stage
        let compute_cost = base_worker_hours * WORKER_MACHINE_COST_PER_HOUR;

        // Shuffle cost proportional to number of shuffle operations
        let shuffle_count = ir.nodes
            .iter()
            .filter(|n| n.node_type.is_shuffle_operation())
            .count();
        let estimated_shuffle_volume_gb = (shuffle_count as f64) * 50.0; // Assume 50GB per shuffle
        let shuffle_cost = estimated_shuffle_volume_gb * DATAFLOW_SHUFFLE_COST_PER_GB;

        // State cost proportional to stateful operations
        let stateful_count = ir.nodes
            .iter()
            .filter(|n| n.node_type.is_stateful())
            .count();
        let estimated_state_size_gb = (stateful_count as f64) * 10.0; // Assume 10GB state per stateful op
        let state_cost = estimated_state_size_gb * PERSISTENT_DISK_COST_PER_GB_MONTH;

        let total_cost = compute_cost + shuffle_cost + state_cost;

        (compute_cost, shuffle_cost, state_cost, total_cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_cost_estimation() {
        let ir = PipelineIR::new("test".to_string());
        let analyzer = CostAnalyzer;
        let result = analyzer.analyze(&ir).unwrap();
        assert!(result.metrics.contains_key("estimated_total_cost_per_month"));
    }
}
