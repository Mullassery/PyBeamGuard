use crate::analyzer::*;
use crate::ir::*;
use std::collections::{HashMap, HashSet};

pub struct GraphAnalyzer;

impl Analyzer for GraphAnalyzer {
    fn name(&self) -> &str {
        "GraphAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        1 // Run first
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        // Check for cycles (invalid in Beam DAGs)
        if self.has_cycle(ir) {
            findings.push(Finding {
                id: "GRAPH_CYCLE".to_string(),
                severity: RiskSeverity::Critical,
                finding_type: FindingType::ConfigurationIssue,
                title: "Pipeline DAG contains cycles".to_string(),
                description: "Beam pipelines must be directed acyclic graphs (DAGs). A cycle was detected in the pipeline topology.".to_string(),
                affected_nodes: vec![],
                recommendation: Some("Review pipeline construction and remove circular dependencies.".to_string()),
                estimated_impact: None,
                confidence: 0.95,
            });
        }

        // Calculate graph metrics
        let depth = self.calculate_depth(ir);
        let fan_in = self.calculate_max_fan_in(ir);
        let fan_out = self.calculate_max_fan_out(ir);
        let complexity = self.calculate_complexity(ir);

        metrics.insert("pipeline_depth".to_string(), depth as f64);
        metrics.insert("max_fan_in".to_string(), fan_in as f64);
        metrics.insert("max_fan_out".to_string(), fan_out as f64);
        metrics.insert("complexity_score".to_string(), complexity);
        metrics.insert("node_count".to_string(), ir.nodes.len() as f64);
        metrics.insert("edge_count".to_string(), ir.edges.len() as f64);

        // Warn if very complex pipeline (depth > 20)
        if depth > 20 {
            findings.push(Finding {
                id: "GRAPH_DEPTH_HIGH".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::PerformanceRisk,
                title: format!("Pipeline depth is very high ({})", depth),
                description: "Deep pipelines have more stages and higher latency. Consider breaking into multiple jobs.".to_string(),
                affected_nodes: vec![],
                recommendation: Some("Consider splitting this pipeline into multiple independent jobs for better parallelism.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(1.5),
                    cost_delta_monthly: None,
                    affected_records_percent: None,
                }),
                confidence: 0.75,
            });
        }

        // Check for fusion opportunities
        let fusion_opps = self.find_fusion_opportunities(ir);
        if !fusion_opps.is_empty() {
            findings.push(Finding {
                id: "GRAPH_FUSION_OPPORTUNITY".to_string(),
                severity: RiskSeverity::Low,
                finding_type: FindingType::PerformanceRisk,
                title: format!("Found {} fusion opportunities", fusion_opps.len()),
                description: "Multiple ParDo operations can potentially be fused into a single transform for better performance.".to_string(),
                affected_nodes: fusion_opps,
                recommendation: Some("Consider combining light-weight ParDo operations to reduce overhead.".to_string()),
                estimated_impact: Some(Impact {
                    latency_multiplier: Some(0.85),
                    cost_delta_monthly: None,
                    affected_records_percent: None,
                }),
                confidence: 0.70,
            });
        }

        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: format!(
                "Pipeline has {} nodes, {} edges, complexity score {:.1}, depth {}",
                ir.nodes.len(),
                ir.edges.len(),
                complexity,
                depth
            ),
            confidence: 0.90,
        })
    }
}

impl GraphAnalyzer {
    fn has_cycle(&self, ir: &PipelineIR) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for node in &ir.nodes {
            if !visited.contains(&node.id)
                && self.has_cycle_dfs(ir, &node.id, &mut visited, &mut rec_stack)
            {
                return true;
            }
        }

        false
    }

    fn has_cycle_dfs(
        &self,
        ir: &PipelineIR,
        node_id: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        visited.insert(node_id.to_string());
        rec_stack.insert(node_id.to_string());

        let downstream = ir.get_downstream_nodes(node_id);
        for downstream_node in downstream {
            if !visited.contains(&downstream_node.id) {
                if self.has_cycle_dfs(ir, &downstream_node.id, visited, rec_stack) {
                    return true;
                }
            } else if rec_stack.contains(&downstream_node.id) {
                return true;
            }
        }

        rec_stack.remove(node_id);
        false
    }

    fn calculate_depth(&self, ir: &PipelineIR) -> usize {
        let sources = ir.get_source_nodes();
        if sources.is_empty() {
            return ir.nodes.len();
        }

        let mut max_depth = 0;
        for source in sources {
            let depth = self.calculate_depth_from(ir, &source.id);
            max_depth = max_depth.max(depth);
        }

        max_depth
    }

    fn calculate_depth_from(&self, ir: &PipelineIR, node_id: &str) -> usize {
        self.calculate_depth_with_visited(ir, node_id, &mut std::collections::HashSet::new())
    }

    fn calculate_depth_with_visited(
        &self,
        ir: &PipelineIR,
        node_id: &str,
        visited: &mut std::collections::HashSet<String>,
    ) -> usize {
        if visited.contains(node_id) {
            return 1; // Prevent cycles
        }
        visited.insert(node_id.to_string());

        let downstream = ir.get_downstream_nodes(node_id);
        if downstream.is_empty() {
            return 1;
        }

        1 + downstream
            .iter()
            .map(|n| self.calculate_depth_with_visited(ir, &n.id, visited))
            .max()
            .unwrap_or(0)
    }

    fn calculate_max_fan_in(&self, ir: &PipelineIR) -> usize {
        ir.nodes
            .iter()
            .map(|n| ir.edges.iter().filter(|e| e.to == n.id).count())
            .max()
            .unwrap_or(0)
    }

    fn calculate_max_fan_out(&self, ir: &PipelineIR) -> usize {
        ir.nodes
            .iter()
            .map(|n| ir.edges.iter().filter(|e| e.from == n.id).count())
            .max()
            .unwrap_or(0)
    }

    fn calculate_complexity(&self, ir: &PipelineIR) -> f64 {
        let n = ir.nodes.len() as f64;
        let e = ir.edges.len() as f64;
        let depth = self.calculate_depth(ir) as f64;
        let fan_in = self.calculate_max_fan_in(ir) as f64;
        let fan_out = self.calculate_max_fan_out(ir) as f64;

        // Complexity = nodes + edges + depth + fan_factors
        (n + e + (depth * 1.5) + (fan_in * 0.5) + (fan_out * 0.5)) / (ir.nodes.len() as f64 + 1.0)
            * 100.0
    }

    fn find_fusion_opportunities(&self, ir: &PipelineIR) -> Vec<String> {
        let mut opportunities = Vec::new();

        for node in &ir.nodes {
            if let TransformType::ParDo { .. } = node.node_type {
                let downstream = ir.get_downstream_nodes(&node.id);
                for down in downstream {
                    if let TransformType::ParDo { .. } = down.node_type {
                        // Both are ParDo with single connection - can fuse
                        if ir.edges.iter().filter(|e| e.from == node.id).count() == 1 {
                            opportunities.push(node.name.clone());
                            break;
                        }
                    }
                }
            }
        }

        opportunities
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_pipeline_analysis() {
        let mut ir = PipelineIR::new("test".to_string());
        ir.nodes.push(TransformNode {
            id: "node_1".to_string(),
            name: "Source".to_string(),
            node_type: TransformType::Source {
                source_type: "text".to_string(),
            },
            inputs: vec![],
            outputs: vec!["data".to_string()],
            config: serde_json::json!({}),
            annotations: vec![],
            line_number: None,
        });

        let analyzer = GraphAnalyzer;
        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = analyzer.analyze(&ctx).unwrap();
        assert_eq!(result.findings.len(), 0); // No issues with single node
    }
}
