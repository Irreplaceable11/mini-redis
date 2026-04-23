use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, extract_i64, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// LTRIM key start stop
pub struct Ltrim {
    key: Bytes,
    start: i64,
    stop: i64,
}

impl Ltrim {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Ltrim> {
        if args.len() < 3 {
            return Err(anyhow!("ERR wrong number of arguments for 'ltrim' command"));
        }
        let key = extract_bytes(&args[0])?;
        let start = extract_i64(&args[1])?;
        let stop = extract_i64(&args[2])?;
        Ok(Ltrim { key, start, stop })
    }
}

impl CommandExecute for Ltrim {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().trim(&self.key, self.start, self.stop) {
            Ok(()) => {
                let entry = AofEntry::Ltrim(self.key, self.start, self.stop);
                (Frame::SimpleString(Bytes::from_static(b"OK")), Some(entry))
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
