use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::CommandExecute;
use crate::context::Context;
use crate::frame::Frame;

/// CLIENT 子命令 — 返回最小兼容响应
pub struct Client;

impl Client {
    pub fn parse(_args: &[Frame]) -> anyhow::Result<Client> {
        Ok(Client)
    }
}

impl CommandExecute for Client {
    fn execute(self, _ctx: &Context) -> (Frame, Option<AofEntry>) {
        (Frame::SimpleString(Bytes::from_static(b"OK")), None)
    }
}
