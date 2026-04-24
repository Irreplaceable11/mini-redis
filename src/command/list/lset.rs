use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::command::parse::extract_isize;
use crate::context::Context;
use crate::frame::Frame;

pub struct Lset {
    key: Bytes,
    index: isize,
    value: Bytes,
}

impl Lset {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Lset> {
        if args.len() < 3 {
            return Err(anyhow!("ERR wrong number of arguments for 'lset' command"));
        }
        let key = extract_bytes(&args[0])?;
        let index = extract_isize(&args[1])?;
        let value = extract_bytes(&args[2])?;
        Ok(Lset { key, index, value })
    }
}

impl CommandExecute for Lset {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let entry = AofEntry::Lset(self.key.clone(), self.index, self.value.clone());
        match ctx.db().lset(&self.key, self.index, self.value) {
            Ok(()) => (Frame::SimpleString(Bytes::from_static(b"OK")), Some(entry)),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
