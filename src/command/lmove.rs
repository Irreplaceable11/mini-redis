use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, extract_string, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
pub struct Lmove {
    source: Bytes,
    destination: Bytes,
    source_left: bool,
    dest_left: bool,
}

impl Lmove {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Lmove> {
        if args.len() < 4 {
            return Err(anyhow!("ERR wrong number of arguments for 'lmove' command"));
        }
        let source = extract_bytes(&args[0])?;
        let destination = extract_bytes(&args[1])?;
        let source_left = Self::parse_direction(&args[2])?;
        let dest_left = Self::parse_direction(&args[3])?;
        Ok(Lmove { source, destination, source_left, dest_left })
    }

    fn parse_direction(frame: &Frame) -> anyhow::Result<bool> {
        match extract_string(frame)?.to_uppercase().as_str() {
            "LEFT" => Ok(true),
            "RIGHT" => Ok(false),
            _ => Err(anyhow!("ERR syntax error")),
        }
    }
}

impl CommandExecute for Lmove {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().lmove(&self.source, &self.destination, self.source_left, self.dest_left) {
            Ok(Some(value)) => {
                let entry = AofEntry::Lmove(self.source, self.destination, self.source_left, self.dest_left);
                (Frame::BulkString(value), Some(entry))
            }
            Ok(None) => (Frame::Null, None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
