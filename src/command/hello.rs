use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::CommandExecute;
use crate::context::Context;
use crate::frame::Frame;

/// HELLO — RESP3 协议协商，我们只支持 RESP2，返回兼容响应
pub struct Hello;

impl Hello {
    pub fn parse(_args: &[Frame]) -> anyhow::Result<Hello> {
        Ok(Hello)
    }
}

impl CommandExecute for Hello {
    fn execute(self, _ctx: &Context) -> (Frame, Option<AofEntry>) {
        // 返回类似 Redis 的 HELLO 响应（RESP2 格式的 key-value 数组）
        (Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"server")),
            Frame::BulkString(Bytes::from_static(b"mini-redis")),
            Frame::BulkString(Bytes::from_static(b"version")),
            Frame::BulkString(Bytes::from_static(b"7.0.0")),
            Frame::BulkString(Bytes::from_static(b"proto")),
            Frame::Integer(2),
            Frame::BulkString(Bytes::from_static(b"mode")),
            Frame::BulkString(Bytes::from_static(b"standalone")),
        ]), None)
    }
}
