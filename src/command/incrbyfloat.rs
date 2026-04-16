use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, extract_f64, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

pub struct IncrByFloat {

    pub key: Bytes,

    pub delta: f64
}

impl IncrByFloat {
    pub fn new(key: Bytes, delta: f64) -> IncrByFloat {
        IncrByFloat { key, delta }
    }

    pub fn parse(args: &[Frame]) -> anyhow::Result<IncrByFloat> {
        if args.len() != 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'incrbyfloat' command"));
        }
        let key = extract_bytes(&args[0])?;
        let delta = extract_f64(&args[1])?;
        Ok(IncrByFloat::new(key, delta))
    }
}

impl CommandExecute for IncrByFloat {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().incr_by_float(self.key.clone(), self.delta) {
            Ok(new_val) => {
                let entry = AofEntry::from_set(self.key, new_val.clone(), None, false, false);
                (Frame::BulkString(new_val), Some(entry))
            }
            Err(msg) => (Frame::Error(Bytes::from(msg)), None),
        }
    }
}