use crate::command::{extract_bytes, CommandExecute};
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::context::Context;

pub struct Pop {
    key: Bytes,

    is_left: bool
}

impl Pop {
    pub fn new(key: Bytes, is_left: bool) -> Pop {
        Pop {key , is_left}
    }

    pub fn parse(args: &[Frame], is_left: bool) -> Result<Pop> {
        if args.is_empty() {
            let command_name = if is_left { "lpop" } else { "rpop" };
            return Err(anyhow!("ERR wrong number of arguments for '{}' command", command_name));
        }
        let key = extract_bytes(&args[0])?;
        Ok(Pop::new(key, is_left))
    }
}

impl CommandExecute for Pop {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().pop(&self.key, self.is_left) {
            Ok(Some(value)) => {
                let entry = AofEntry::Pop(self.key.clone(), self.is_left);
                (Frame::BulkString(value), Some(entry))
            }
            Ok(None) => (Frame::Null, None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None)
        }
    }
}