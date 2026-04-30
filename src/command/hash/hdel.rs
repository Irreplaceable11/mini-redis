use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HDEL key field [field ...]
pub struct Hdel {
    key: Bytes,
    fields: Vec<Bytes>,
}

impl Hdel {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hdel> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'hdel' command"));
        }
        let key = extract_bytes(&args[0])?;
        let mut fields = Vec::with_capacity(args.len() - 1);
        for i in 1..args.len() {
            fields.push(extract_bytes(&args[i])?);
        }
        Ok(Hdel { key, fields })
    }
}

impl CommandExecute for Hdel {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hdel(&self.key, self.fields) {
            Ok(count) => (Frame::Integer(count), None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
