use crate::aof::AofEntry;
use crate::command::CommandExecute;
use crate::context::Context;
use crate::frame::Frame;
use bytes::Bytes;

pub struct Info;

impl Info {
    pub fn parse(_args: &[Frame]) -> anyhow::Result<Info> {
        // INFO [section] — 忽略 section 参数，返回固定内容
        Ok(Info)
    }
}

impl CommandExecute for Info {
    fn execute(self, _ctx: &Context) -> (Frame, Option<AofEntry>) {
        let info = "\
            # Server\r\n\
            redis_version:7.0.0\r\n\
            redis_mode:standalone\r\n\
            os:Linux\r\n\
            tcp_port:6377\r\n\
            \r\n\
            # Clients\r\n\
            connected_clients:1\r\n\
            \r\n\
            # Memory\r\n\
            used_memory:0\r\n\
            used_memory_human:0B\r\n\
            \r\n\
            # Replication\r\n\
            role:master\r\n\
            connected_slaves:0\r\n\
            \r\n\
            # Keyspace\r\n\
            db0:keys=0,expires=0,avg_ttl=0\r\n";

        (Frame::BulkString(Bytes::from(info)), None)
    }
}
