use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use bytes::Bytes;

pub struct Incr {
    pub key: Bytes,
}

impl Incr {
    pub fn new(key: Bytes) -> Incr {
        Incr { key }
    }

    pub fn parse(args: &[Frame]) -> Result<Incr> {
        if args.len() != 1 {
            return Err(anyhow!("ERR wrong number of arguments for 'incr' command"));
        }
        let key = extract_bytes(&args[0])?;
        Ok(Incr::new(key))
    }
}

impl CommandExecute for Incr {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().incr_by(self.key.clone(), 1) {
            Ok(new_val) => {
                let mut buffer = itoa::Buffer::new();
                let val_bytes = Bytes::copy_from_slice(buffer.format(new_val).as_bytes());
                let entry = AofEntry::from_set(self.key, val_bytes, None, false, false);
                (Frame::Integer(new_val), Some(entry))
            }
            Err(msg) => (Frame::Error(Bytes::from(msg)), None),
        }
    }
}
