use crate::aof::AofEntry;
use crate::command::CommandExecute;
use crate::context::Context;
use crate::frame::Frame;

/// 处理 CONFIG GET 等，返回空数组
pub struct Config;

impl Config {
    pub fn parse(_args: &[Frame]) -> anyhow::Result<Config> {
        Ok(Config)
    }
}

impl CommandExecute for Config {
    fn execute(self, _ctx: &Context) -> (Frame, Option<AofEntry>) {
        (Frame::Array(vec![]), None)
    }
}
