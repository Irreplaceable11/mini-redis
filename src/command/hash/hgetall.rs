use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HGETALL key
pub struct Hgetall {
    key: Bytes,
}

impl Hgetall {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hgetall> {
        if args.len() < 1 {
            return Err(anyhow!("ERR wrong number of arguments for 'hgetall' command"));
        }
        let key = extract_bytes(&args[0])?;
        Ok(Hgetall { key })
    }
}

impl CommandExecute for Hgetall {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hgetall(&self.key) {
            Ok(pairs) => {
                let mut items = Vec::with_capacity(pairs.len() * 2);
                for (field, value) in pairs {
                    items.push(Frame::BulkString(field));
                    items.push(Frame::BulkString(value));
                }
                (Frame::Array(items), None)
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
