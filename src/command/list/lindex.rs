use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::command::parse::extract_isize;
use crate::context::Context;
use crate::frame::Frame;

pub struct Lindex {
    key: Bytes,
    index: isize,
}

impl Lindex {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Lindex> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'lindex' command"));
        }
        let key = extract_bytes(&args[0])?;
        let index = extract_isize(&args[1])?;
        Ok(Lindex { key, index })
    }
}

impl CommandExecute for Lindex {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().index(&self.key, self.index) {
            Ok(Some(value)) => (Frame::BulkString(value), None),
            Ok(None) => (Frame::Null, None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
