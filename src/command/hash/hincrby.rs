use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::command::parse::extract_i64;
use crate::context::Context;
use crate::frame::Frame;

/// HINCRBY key field increment
pub struct Hincrby {
    key: Bytes,
    field: Bytes,
    delta: i64,
}

impl Hincrby {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hincrby> {
        if args.len() < 3 {
            return Err(anyhow!("ERR wrong number of arguments for 'hincrby' command"));
        }
        let key = extract_bytes(&args[0])?;
        let field = extract_bytes(&args[1])?;
        let delta = extract_i64(&args[2])?;
        Ok(Hincrby { key, field, delta })
    }
}

impl CommandExecute for Hincrby {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hincrby(self.key, self.field, self.delta) {
            Ok(val) => (Frame::Integer(val), None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
