use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HEXISTS key field
pub struct Hexists {
    key: Bytes,
    field: Bytes,
}

impl Hexists {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hexists> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'hexists' command"));
        }
        let key = extract_bytes(&args[0])?;
        let field = extract_bytes(&args[1])?;
        Ok(Hexists { key, field })
    }
}

impl CommandExecute for Hexists {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hexists(&self.key, &self.field) {
            Ok(val) => (Frame::Integer(val), None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
