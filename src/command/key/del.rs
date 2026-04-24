use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use bytes::Bytes;

pub struct Del {
    keys: Vec<Bytes>,
}

impl Del {
    pub fn new(keys: Vec<Bytes>) -> Del {
        Del { keys }
    }

    pub fn parse(args: &[Frame]) -> Result<Del> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'del'"));
        }
        let keys: Vec<Bytes> = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Del::new(keys))
    }
}

impl CommandExecute for Del {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let entry = AofEntry::Del(self.keys.clone());
        let res = ctx.db().del(self.keys);
        if res > 0 {
            (Frame::Integer(res as i64), Some(entry))
        } else {
            (Frame::Integer(0), None)
        }
    }
}
