use super::{Framework, FrameworkParser};
use crate::ir::PipelineIR;
use crate::parser::BeamPipelineParser;
use anyhow::Result;

pub struct BeamFrameworkParser;

impl FrameworkParser for BeamFrameworkParser {
    fn parse(&self, code: &str) -> Result<PipelineIR> {
        let parser = BeamPipelineParser::new();
        parser.parse(code)
    }

    fn framework(&self) -> Framework {
        Framework::ApacheBeam
    }
}
