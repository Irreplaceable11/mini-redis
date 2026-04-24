use crate::frame::Frame;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use crate::aof::{AofEntry, RewriteState};
use crate::command::CommandExecute;
use crate::context::Context;

pub struct BgRewriteAof;


impl BgRewriteAof {
    pub fn parse(args: &[Frame]) -> Result<BgRewriteAof> {
        if !args.is_empty() {
            return Err(anyhow!("ERR wrong number of arguments for 'bgrewriteaof' command"));
        }
        Ok(BgRewriteAof)
    }
}

impl CommandExecute for BgRewriteAof {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.send_rewrite_state(RewriteState::Rewriting) {
            Ok(_) => (Frame::SimpleString(Bytes::from_static(b"Background append only file rewriting started")), None),
            Err(e) => (Frame::Error(Bytes::from(format!("ERR {}", e))), None)
        }
    }
}