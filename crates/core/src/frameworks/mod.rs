pub mod beam;
pub mod flink;
pub mod spark;

use crate::ir::PipelineIR;
use anyhow::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framework {
    ApacheBeam,
    ApacheFlink,
    ApacheSpark,
    KafkaStreams,
    RayData,
}

impl Framework {
    pub fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "beam" | "apache_beam" => Some(Framework::ApacheBeam),
            "flink" | "apache_flink" => Some(Framework::ApacheFlink),
            "spark" | "apache_spark" => Some(Framework::ApacheSpark),
            "kafka" | "kafka_streams" => Some(Framework::KafkaStreams),
            "ray" | "ray_data" => Some(Framework::RayData),
            _ => None,
        }
    }

    pub fn to_string(&self) -> &str {
        match self {
            Framework::ApacheBeam => "Apache Beam",
            Framework::ApacheFlink => "Apache Flink",
            Framework::ApacheSpark => "Apache Spark Structured Streaming",
            Framework::KafkaStreams => "Kafka Streams",
            Framework::RayData => "Ray Data",
        }
    }
}

pub trait FrameworkParser {
    fn parse(&self, code: &str) -> Result<PipelineIR>;
    fn framework(&self) -> Framework;
}
