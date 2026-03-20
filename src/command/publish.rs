use bytes::Bytes;
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use crate::command::{extract_bytes, extract_string, CommandExecute};
use crate::context::Context;

pub struct Publish {
    channel: String,

    message: Bytes
}

impl Publish {
    pub fn new(channel: String, message: Bytes) -> Publish {
        Publish { channel, message }
    }

    pub fn parse(args: &[Frame]) -> Result<Publish> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'publish'"))
        }
        let channel_name = extract_string(&args[0])?;

        let message = extract_bytes(&args[1])?;
        Ok(Publish::new(channel_name, message))
    }
}

impl CommandExecute for Publish {
    fn execute(self, ctx: &Context) -> Frame {
        match ctx.pub_sub().publish(&self.channel, self.message) {
            Ok(cnt) => Frame::Integer(cnt as i64),
            Err(err) => Frame::Error(Bytes::from(err.to_string()))
        }
    }
}