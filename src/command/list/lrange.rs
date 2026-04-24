use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::command::parse::extract_isize;
use crate::context::Context;
use crate::frame::Frame;

pub struct Lrange {
    key: Bytes,
    start: isize,
    end: isize,
}

impl Lrange {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Lrange> {
        if args.len() < 3 {
            return Err(anyhow!("ERR wrong number of arguments for 'lrange' command"));
        }
        let key = extract_bytes(&args[0])?;
        let start = extract_isize(&args[1])?;
        let end = extract_isize(&args[2])?;
        Ok(Lrange { key, start, end })
    }
}

impl CommandExecute for Lrange {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().range(&self.key, self.start, self.end) {
            Ok(values) => {
                let frames: Vec<Frame> = values
                    .into_iter()
                    .map(Frame::BulkString)
                    .collect();
                (Frame::Array(frames), None)
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
