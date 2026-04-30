use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HKEYS key / HVALS key
pub struct Hkeys {
    key: Bytes,
    is_keys: bool,
}

impl Hkeys {
    pub fn parse(args: &[Frame], is_keys: bool) -> anyhow::Result<Hkeys> {
        let cmd_name = if is_keys { "hkeys" } else { "hvals" };
        if args.len() < 1 {
            return Err(anyhow!("ERR wrong number of arguments for '{}' command", cmd_name));
        }
        let key = extract_bytes(&args[0])?;
        Ok(Hkeys { key, is_keys })
    }
}

impl CommandExecute for Hkeys {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hkeys_or_vals(&self.key, self.is_keys) {
            Ok(items) => {
                let frames: Vec<Frame> = items.into_iter().map(Frame::BulkString).collect();
                (Frame::Array(frames), None)
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
