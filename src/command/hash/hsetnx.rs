use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HSETNX key field value
pub struct Hsetnx {
    key: Bytes,
    field: Bytes,
    value: Bytes,
}

impl Hsetnx {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hsetnx> {
        if args.len() < 3 {
            return Err(anyhow!("ERR wrong number of arguments for 'hsetnx' command"));
        }
        let key = extract_bytes(&args[0])?;
        let field = extract_bytes(&args[1])?;
        let value = extract_bytes(&args[2])?;
        Ok(Hsetnx { key, field, value })
    }
}

impl CommandExecute for Hsetnx {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hsetnx(&self.key, self.field, self.value) {
            Ok(val) => (Frame::Integer(val), None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
