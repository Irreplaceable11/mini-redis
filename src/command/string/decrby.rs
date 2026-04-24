use crate::aof::AofEntry;
use crate::command::{extract_bytes, extract_i64, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use bytes::Bytes;

pub struct DecrBy {
    pub key: Bytes,
    pub delta: i64,
}

impl DecrBy {
    pub fn new(key: Bytes, delta: i64) -> DecrBy {
        DecrBy { key, delta }
    }

    pub fn parse(args: &[Frame]) -> Result<DecrBy> {
        if args.len() != 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'decrby' command"));
        }
        let key = extract_bytes(&args[0])?;
        let delta = extract_i64(&args[1])?;
        Ok(DecrBy::new(key, delta))
    }
}

impl CommandExecute for DecrBy {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        // DECRBY key n 等同于 incr(key, -n)
        match ctx.db().incr_by(self.key.clone(), -self.delta) {
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
