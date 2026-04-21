use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{CommandExecute, extract_i64};
use crate::context::Context;
use crate::frame::Frame;

/// SELECT db — 只支持 db0
pub struct Select {
    db: i64,
}

impl Select {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Select> {
        if args.is_empty() {
            return Err(anyhow::anyhow!("ERR wrong number of arguments for 'select' command"));
        }
        let db = extract_i64(&args[0])?;
        Ok(Select { db })
    }
}

impl CommandExecute for Select {
    fn execute(self, _ctx: &Context) -> (Frame, Option<AofEntry>) {
        if self.db == 0 {
            (Frame::SimpleString(Bytes::from_static(b"OK")), None)
        } else {
            (Frame::Error(Bytes::from("ERR DB index is out of range")), None)
        }
    }
}
