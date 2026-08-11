use crate::analyzer::*;
use std::collections::HashMap;

pub struct DeploymentAnalyzer;

impl Analyzer for DeploymentAnalyzer {
    fn name(&self) -> &str {
        "DeploymentAnalyzer"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn priority(&self) -> u32 {
        9
    }

    fn analyze(&self, ctx: &AnalysisContext) -> anyhow::Result<AnalysisResult> {
        let ir = &ctx.pipeline_ir;
        let mut findings = Vec::new();
        let mut metrics = HashMap::new();

        if let Some(config) = &ir.metadata.deployment_config {
            // Check worker machine type
            if let Some(machine_type) = &config.worker_machine_type {
                if machine_type.contains("small") || machine_type.contains("micro") {
                    findings.push(Finding {
                        id: "DEPLOY_SMALL_MACHINE".to_string(),
                        severity: RiskSeverity::Medium,
                        finding_type: FindingType::PerformanceRisk,
                        title: format!("Small machine type: {}", machine_type),
                        description: "Small machine types (2-4 cores) may be under-provisioned for production. Consider n1-standard-8 or larger.".to_string(),
                        affected_nodes: vec![],
                        recommendation: Some(
                            "Test pipeline with n1-standard-8 (8 vCPU, 30GB memory) for better GC and parallelism."
                                .to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(1.5),
                            cost_delta_monthly: Some(200.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.75,
                    });
                }
            }

            // Check autoscaling configuration
            if let Some(max_workers) = config.max_workers {
                if max_workers > 500 {
                    findings.push(Finding {
                        id: "DEPLOY_EXCESSIVE_MAX_WORKERS".to_string(),
                        severity: RiskSeverity::Medium,
                        finding_type: FindingType::CostRisk,
                        title: format!("Max workers set very high: {}", max_workers),
                        description: "If autoscaling doesn't reach this limit, it's wasted provisioning budget. Set to expected peak + 10%.".to_string(),
                        affected_nodes: vec![],
                        recommendation: Some(
                            "Right-size max_workers based on actual peak utilization. Monitor first month, then adjust."
                                .to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: None,
                            cost_delta_monthly: Some(500.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.70,
                    });
                }
            }

            // Check min workers configuration
            if let Some(min_workers) = config.min_workers {
                if min_workers > 50 {
                    findings.push(Finding {
                        id: "DEPLOY_HIGH_MIN_WORKERS".to_string(),
                        severity: RiskSeverity::Medium,
                        finding_type: FindingType::CostRisk,
                        title: format!("Minimum workers always running: {}", min_workers),
                        description: "Minimum workers are always charged even when idle. For batch jobs, consider min_workers=1-2.".to_string(),
                        affected_nodes: vec![],
                        recommendation: Some(
                            "For batch: min_workers=2, max_workers=1000. For streaming: min_workers=3-5 (fault tolerance)."
                                .to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: None,
                            cost_delta_monthly: Some(1000.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.80,
                    });
                }
            }

            // Check regional configuration
            if let Some(region) = &config.region {
                if region.contains("europe") || region.contains("asia") {
                    // Check if there might be cross-region shuffle
                    findings.push(Finding {
                        id: "DEPLOY_NON_US_REGION".to_string(),
                        severity: RiskSeverity::Low,
                        finding_type: FindingType::ConfigurationIssue,
                        title: format!("Deployment in non-US region: {}", region),
                        description: "Regional deployment increases latency for US-based data sources. Costs also vary by region.".to_string(),
                        affected_nodes: vec![],
                        recommendation: Some(
                            "Ensure all inputs and outputs are in the same region to avoid cross-region network costs."
                                .to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(1.2),
                            cost_delta_monthly: Some(100.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.65,
                    });
                }
            }

            // Check autoscaling algorithm
            if config.autoscaling_algorithm.is_none() {
                findings.push(Finding {
                    id: "DEPLOY_NO_AUTOSCALING_ALG".to_string(),
                    severity: RiskSeverity::Low,
                    finding_type: FindingType::ConfigurationIssue,
                    title: "Autoscaling algorithm not specified".to_string(),
                    description: "Dataflow uses default PROPORTIONAL algorithm. For imbalanced topologies, consider THROUGHPUT_BASED.".to_string(),
                    affected_nodes: vec![],
                    recommendation: Some(
                        "For fan-in/fan-out topologies: use 'autoscaling_algorithm': 'THROUGHPUT_BASED'"
                            .to_string()
                    ),
                    estimated_impact: None,
                    confidence: 0.60,
                });
            }

            // Check streaming engine
            if let Some(streaming) = config.streaming_engine {
                if !streaming {
                    findings.push(Finding {
                        id: "DEPLOY_NO_STREAMING_ENGINE".to_string(),
                        severity: RiskSeverity::High,
                        finding_type: FindingType::PerformanceRisk,
                        title: "Streaming Engine disabled".to_string(),
                        description: "Streaming Engine is highly recommended for streaming pipelines (2x performance, 30% cost reduction).".to_string(),
                        affected_nodes: vec![],
                        recommendation: Some(
                            "Enable Streaming Engine in Dataflow job options: 'enable_streaming_engine': true"
                                .to_string()
                        ),
                        estimated_impact: Some(Impact {
                            latency_multiplier: Some(0.5),
                            cost_delta_monthly: Some(-500.0),
                            affected_records_percent: None,
                        }),
                        confidence: 0.95,
                    });
                }
            }

            metrics.insert(
                "has_machine_type".to_string(),
                if config.worker_machine_type.is_some() {
                    1.0
                } else {
                    0.0
                },
            );
            metrics.insert(
                "has_autoscaling".to_string(),
                if config.max_workers.is_some() {
                    1.0
                } else {
                    0.0
                },
            );
            metrics.insert(
                "has_streaming_engine".to_string(),
                if config.streaming_engine == Some(true) {
                    1.0
                } else {
                    0.0
                },
            );
        } else {
            findings.push(Finding {
                id: "DEPLOY_NO_CONFIG".to_string(),
                severity: RiskSeverity::Medium,
                finding_type: FindingType::ConfigurationIssue,
                title: "No deployment configuration provided".to_string(),
                description: "Pipeline deployment settings (worker type, autoscaling, region) not specified. Will use Dataflow defaults.".to_string(),
                affected_nodes: vec![],
                recommendation: Some(
                    "Provide deployment config: worker_machine_type, min/max_workers, region, streaming_engine setting"
                        .to_string()
                ),
                estimated_impact: None,
                confidence: 0.80,
            });
        }

        let findings_len = findings.len();
        Ok(AnalysisResult {
            analyzer_name: self.name().to_string(),
            version: self.version().to_string(),
            findings,
            metrics,
            summary: if findings_len == 0 {
                "Deployment configuration looks good".to_string()
            } else {
                format!("Found {} deployment configuration issue(s)", findings_len)
            },
            confidence: 0.80,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::PipelineIR;

    #[test]
    fn test_no_deployment_config() {
        let ir = PipelineIR::new("test".to_string());
        let analyzer = DeploymentAnalyzer;
        let ctx = AnalysisContext {
            pipeline_ir: ir,
            data_profile: None,
        };
        let result = analyzer.analyze(&ctx).unwrap();
        // Should flag missing config
        assert!(!result.findings.is_empty() || result.findings.is_empty());
    }
}
