use crate::analyzer::*;
use crate::ir::*;
use std::collections::HashMap;

pub struct CostAnalyzer;

// ----------------------------------------------------------------------------
// Cost model assumptions (heuristic, NOT validated against live GCP billing)
// ----------------------------------------------------------------------------
// These constants back an order-of-magnitude, pre-deployment cost signal for
// CI/triage purposes. They are NOT a substitute for the GCP Pricing
// Calculator or an actual test run, and this analyzer's `confidence` field
// reflects that uncertainty explicitly.
//
// - PERSISTENT_DISK_COST_PER_GB_MONTH is anchored to GCP's published
//   pd-standard list price in us-central1 (~$0.04/GB-month as of the time
//   this comment was written); this is the one figure here with a direct,
//   checkable public price.
// - WORKER_MACHINE_COST_PER_HOUR is a blended proxy for a mid-size Dataflow
//   worker (roughly n1-standard-2/4 class). Dataflow's actual billing is
//   per-vCPU-hour + per-GB-memory-hour (+ PD), not a flat per-VM rate, so
//   this is deliberately a simplification, not a machine-type-specific rate.
// - DATAFLOW_SHUFFLE_COST_PER_GB is a rough proxy for Dataflow Shuffle /
//   Streaming Engine data-processed pricing, which varies by tier and is not
//   published as a single flat per-GB figure.
//
// Treat CostAnalyzer's dollar output as a directional planning signal
// ("is this pipeline cheap, moderate, or expensive"), not a bill forecast.
const DATAFLOW_SHUFFLE_COST_PER_GB: f64 = 0.30;
const WORKER_MACHINE_COST_PER_HOUR: f64 = 0.35;
const PERSISTENT_DISK_COST_PER_GB_MONTH: f64 = 0.04;

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

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let profile = ctx.data_profile.as_ref();
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        // Estimate costs
        let (compute_cost, shuffle_cost, state_cost, total_cost) = self.estimate_costs(ir, profile);

        metrics.insert("estimated_compute_cost_per_month".to_string(), compute_cost);
        metrics.insert("estimated_shuffle_cost_per_month".to_string(), shuffle_cost);
        metrics.insert("estimated_state_cost_per_month".to_string(), state_cost);
        metrics.insert("estimated_total_cost_per_month".to_string(), total_cost);

        // Find cost hotspots
        let shuffle_ops = ir
            .nodes
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
        let stateful_ops = ir
            .nodes
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
                "Estimated monthly cost: ${:.0} ({}) | Compute: ${:.0} | Shuffle: ${:.0} | State: ${:.0}{}",
                total_cost, cost_tier, compute_cost, shuffle_cost, state_cost,
                if profile.is_some() { " | informed by data profile" } else { " | rough heuristic (no data profile supplied)" }
            ),
            // A data profile replaces flat per-op assumptions with real
            // throughput/state figures, which narrows (but does not
            // eliminate) the model's uncertainty -- this is still a rough
            // heuristic, not a validated billing forecast.
            confidence: if profile.is_some() { 0.75 } else { 0.55 },
        })
    }
}

impl CostAnalyzer {
    fn estimate_costs(
        &self,
        ir: &PipelineIR,
        profile: Option<&DataProfile>,
    ) -> (f64, f64, f64, f64) {
        // Heuristic-based estimation
        let base_worker_hours = (ir.nodes.len() as f64) * 10.0; // Rough estimate: 10h per stage
        let compute_cost = base_worker_hours * WORKER_MACHINE_COST_PER_HOUR;

        // Shuffle cost proportional to number of shuffle operations. When a
        // data profile supplies real throughput and element size, use that
        // to derive an actual monthly shuffle volume instead of the flat
        // 50GB-per-shuffle guess.
        let shuffle_count = ir
            .nodes
            .iter()
            .filter(|n| n.node_type.is_shuffle_operation())
            .count();
        let estimated_shuffle_volume_gb = match profile.and_then(|p| {
            match (p.estimated_throughput_per_sec, p.average_element_size_bytes) {
                (Some(throughput), Some(elem_bytes)) => Some(throughput * elem_bytes as f64),
                _ => None,
            }
        }) {
            Some(bytes_per_sec) => {
                const SECONDS_PER_MONTH: f64 = 86_400.0 * 30.0;
                const BYTES_PER_GB: f64 = 1_073_741_824.0;
                let monthly_gb = (bytes_per_sec * SECONDS_PER_MONTH) / BYTES_PER_GB;
                monthly_gb * (shuffle_count as f64)
            }
            None => (shuffle_count as f64) * 50.0, // Assume 50GB per shuffle
        };
        let shuffle_cost = estimated_shuffle_volume_gb * DATAFLOW_SHUFFLE_COST_PER_GB;

        // State cost proportional to stateful operations. Prefer the
        // caller-supplied state size over the flat 10GB-per-stateful-op
        // guess when a data profile is provided.
        let stateful_count = ir
            .nodes
            .iter()
            .filter(|n| n.node_type.is_stateful())
            .count();
        let estimated_state_size_gb = match profile.and_then(|p| p.estimated_state_size_gb) {
            Some(gb) => gb,
            None => (stateful_count as f64) * 10.0, // Assume 10GB state per stateful op
        };
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
        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = analyzer.analyze(&ctx).unwrap();
        assert!(result
            .metrics
            .contains_key("estimated_total_cost_per_month"));
        // Without a data profile the estimate is a rough heuristic; confidence should reflect that.
        assert!(result.confidence < 0.7);
    }

    #[test]
    fn test_data_profile_increases_confidence_and_changes_estimate() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(TransformNode {
            id: "node_1".to_string(),
            name: "GroupByKey".to_string(),
            node_type: TransformType::GroupByKey { key_expr: None },
            inputs: vec![],
            outputs: vec!["grouped".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        });

        let analyzer = CostAnalyzer;
        let ctx_no_profile = AnalysisContext {
            pipeline_ir: ir.clone(),
            data_profile: None,
        };
        let ctx_with_profile = AnalysisContext {
            pipeline_ir: ir,
            data_profile: Some(DataProfile {
                estimated_throughput_per_sec: Some(1000.0),
                average_element_size_bytes: Some(1024),
                key_cardinality: None,
                estimated_state_size_gb: Some(2.5),
            }),
        };

        let result_no_profile = analyzer.analyze(&ctx_no_profile).unwrap();
        let result_with_profile = analyzer.analyze(&ctx_with_profile).unwrap();

        assert!(result_with_profile.confidence > result_no_profile.confidence);
        // The data-profile-informed state cost should reflect the supplied
        // 2.5GB directly (2.5 * PERSISTENT_DISK_COST_PER_GB_MONTH), not the
        // flat per-stateful-op guess (this pipeline has no stateful ParDo,
        // so the heuristic default would be $0).
        let state_cost = result_with_profile
            .metrics
            .get("estimated_state_cost_per_month")
            .unwrap();
        assert!(*state_cost > 0.0);
    }
}
