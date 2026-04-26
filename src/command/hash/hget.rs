use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HGET key field
pub struct Hget {
    key: Bytes,
    field: Bytes,
}

impl Hget {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hget> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'hget' command"));
        }
        let key = extract_bytes(&args[0])?;
        let field = extract_bytes(&args[1])?;
        Ok(Hget { key, field })
    }
}

impl CommandExecute for Hget {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hget(&self.key, &self.field) {
            Ok(Some(value)) => (Frame::BulkString(value), None),
            Ok(None) => (Frame::Null, None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
