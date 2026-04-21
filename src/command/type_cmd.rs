use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{CommandExecute, extract_bytes};
use crate::context::Context;
use crate::db::EntryValue;
use crate::frame::Frame;

/// TYPE key — 返回 key 对应值的类型：string / list / none
pub struct TypeCmd {
    key: Bytes,
}

impl TypeCmd {
    pub fn parse(args: &[Frame]) -> anyhow::Result<TypeCmd> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("ERR wrong number of arguments for 'type' command"));
        }
        let key = extract_bytes(&args[0])?;
        Ok(TypeCmd { key })
    }
}

impl CommandExecute for TypeCmd {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let idx = ctx.db().shard_index(&self.key);
        let shard = &ctx.db().shards[idx];
        let type_name = match shard.get(&self.key) {
            Some(entry) if !entry.is_expired() => {
                match &entry.value {
                    EntryValue::String(_) => "string",
                    EntryValue::List(_) => "list",
                }
            }
            _ => "none",
        };
        (Frame::SimpleString(Bytes::from(type_name)), None)
    }
}
