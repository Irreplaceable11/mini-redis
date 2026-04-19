use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

pub struct Llen {
    key: Bytes,
}

impl Llen {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Llen> {
        if args.is_empty() {
            return Err(anyhow!("ERR wrong number of arguments for 'llen' command"));
        }
        let key = extract_bytes(&args[0])?;
        Ok(Llen { key })
    }
}

impl CommandExecute for Llen {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().len(&self.key) {
            Ok(len) => (Frame::Integer(len), None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
