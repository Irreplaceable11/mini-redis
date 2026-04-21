use crate::aof::AofEntry;
use crate::command::CommandExecute;
use crate::context::Context;
use crate::frame::Frame;

/// 处理 COMMAND / COMMAND DOCS / COMMAND COUNT 等，返回最小兼容响应
pub struct CommandCmd;

impl CommandCmd {
    pub fn parse(_args: &[Frame]) -> anyhow::Result<CommandCmd> {
        Ok(CommandCmd)
    }
}

impl CommandExecute for CommandCmd {
    fn execute(self, _ctx: &Context) -> (Frame, Option<AofEntry>) {
        // 返回空数组，RedisInsight 能接受
        (Frame::Array(vec![]), None)
    }
}
