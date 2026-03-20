use crate::frame::Frame;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;

pub struct Exists {
    keys: Vec<Bytes>,
}

impl Exists {
    pub fn new(keys: Vec<Bytes>) -> Exists {
        Exists { keys }
    }

    pub fn parse(args: &[Frame]) -> Result<Exists> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'exists'"));
        }
        let keys = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<Bytes>>>()?;
        Ok(Exists { keys })
    }
}

impl CommandExecute for Exists {
    fn execute(self, ctx: &Context) -> Frame {
        let res = ctx.db().exists(self.keys);
        Frame::Integer(res as i64)
    }
}
