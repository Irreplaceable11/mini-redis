use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HLEN key
pub struct Hlen {
    key: Bytes,
}

impl Hlen {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hlen> {
        if args.len() < 1 {
            return Err(anyhow!("ERR wrong number of arguments for 'hlen' command"));
        }
        let key = extract_bytes(&args[0])?;
        Ok(Hlen { key })
    }
}

impl CommandExecute for Hlen {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hlen(&self.key) {
            Ok(len) => (Frame::Integer(len), None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
