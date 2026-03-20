use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;
use anyhow::Result;
use bytes::Bytes;

pub struct Get {
    pub key: Bytes,
}

impl Get {
    pub fn new(key: Bytes) -> Self {
        Get { key }
    }

    pub fn parse(args: &[Frame]) -> Result<Get> {
        let key = extract_bytes(&args[0])?;
        Ok(Get::new(key))
    }
}

impl CommandExecute for Get {
    fn execute(self, ctx: &Context) -> Frame {
        match ctx.db().get(&self.key) {
            Some(value) => Frame::BulkString(value),
            None => Frame::Null,
        }
    }
}