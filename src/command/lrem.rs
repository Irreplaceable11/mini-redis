use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::command::parse::extract_isize;
use crate::context::Context;
use crate::frame::Frame;

pub struct Lrem {
    key: Bytes,
    count: isize,
    value: Bytes,
}

impl Lrem {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Lrem> {
        if args.len() < 3 {
            return Err(anyhow!("ERR wrong number of arguments for 'lrem' command"));
        }
        let key = extract_bytes(&args[0])?;
        let count = extract_isize(&args[1])?;
        let value = extract_bytes(&args[2])?;
        Ok(Lrem { key, count, value })
    }
}

impl CommandExecute for Lrem {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().lrem(&self.key, self.count, self.value.clone()) {
            Ok(removed) => {
                let entry = AofEntry::Lrem(self.key.clone(), self.count, self.value);
                (Frame::Integer(removed), if removed > 0 { Some(entry) } else { None })
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
