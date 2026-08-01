use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineIR {
    pub id: String,
    pub name: String,
    pub nodes: Vec<TransformNode>,
    pub edges: Vec<Edge>,
    pub metadata: PipelineMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformNode {
    pub id: String,
    pub name: String,
    pub node_type: TransformType,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub config: serde_json::Value,
    pub annotations: Vec<String>,
    pub line_number: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub pcollection_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMetadata {
    pub runner: Option<Runner>,
    pub deployment_config: Option<DeploymentConfig>,
    pub runner_hints: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Runner {
    Direct,
    Dataflow,
    Spark,
    Flink,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentConfig {
    pub worker_machine_type: Option<String>,
    pub min_workers: Option<usize>,
    pub max_workers: Option<usize>,
    pub streaming_engine: Option<bool>,
    pub region: Option<String>,
    pub autoscaling_algorithm: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformType {
    Source {
        source_type: String,
    },
    ParDo {
        dofn_name: String,
        is_stateful: bool,
        has_side_inputs: bool,
        has_side_outputs: bool,
    },
    GroupByKey {
        key_expr: Option<String>,
    },
    CoGroupByKey {
        key_expr: Option<String>,
    },
    Flatten,
    Partition,
    Windowing {
        window_type: String,
        duration_sec: Option<u64>,
        trigger: Option<String>,
        allowed_lateness_sec: Option<u64>,
        accumulation_mode: Option<String>,
    },
    CombinePerKey {
        combine_fn: String,
    },
    Sink {
        sink_type: String,
    },
    Custom(String),
}

impl TransformType {
    pub fn is_shuffle_operation(&self) -> bool {
        matches!(
            self,
            TransformType::GroupByKey { .. }
                | TransformType::CoGroupByKey { .. }
                | TransformType::CombinePerKey { .. }
        )
    }

    pub fn is_stateful(&self) -> bool {
        matches!(
            self,
            TransformType::ParDo {
                is_stateful: true,
                ..
            }
        )
    }
}

impl PipelineIR {
    pub fn new(name: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            nodes: Vec::new(),
            edges: Vec::new(),
            metadata: PipelineMetadata {
                runner: None,
                deployment_config: None,
                runner_hints: HashMap::new(),
            },
        }
    }

    pub fn get_node(&self, id: &str) -> Option<&TransformNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    pub fn get_upstream_nodes(&self, node_id: &str) -> Vec<&TransformNode> {
        let upstream_ids: Vec<_> = self
            .edges
            .iter()
            .filter(|e| e.to == node_id)
            .map(|e| &e.from)
            .collect();

        self.nodes
            .iter()
            .filter(|n| upstream_ids.contains(&&n.id))
            .collect()
    }

    pub fn get_downstream_nodes(&self, node_id: &str) -> Vec<&TransformNode> {
        let downstream_ids: Vec<_> = self
            .edges
            .iter()
            .filter(|e| e.from == node_id)
            .map(|e| &e.to)
            .collect();

        self.nodes
            .iter()
            .filter(|n| downstream_ids.contains(&&n.id))
            .collect()
    }

    pub fn get_source_nodes(&self) -> Vec<&TransformNode> {
        self.nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::Source { .. }))
            .collect()
    }

    pub fn get_sink_nodes(&self) -> Vec<&TransformNode> {
        self.nodes
            .iter()
            .filter(|n| matches!(n.node_type, TransformType::Sink { .. }))
            .collect()
    }
}
