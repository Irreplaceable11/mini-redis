use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// LPUSHX / RPUSHX 命令：仅当 key 已存在时才 push
pub struct Pushx {
    key: Bytes,
    values: Vec<Bytes>,
    is_left: bool,
}

impl Pushx {
    pub fn parse(args: &[Frame], is_left: bool) -> anyhow::Result<Pushx> {
        let cmd = if is_left { "lpushx" } else { "rpushx" };
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for '{}' command", cmd));
        }
        let key = extract_bytes(&args[0])?;
        let mut values = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            values.push(extract_bytes(arg)?);
        }
        Ok(Pushx { key, values, is_left })
    }
}

impl CommandExecute for Pushx {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().pushx(&self.key, self.values.clone(), self.is_left) {
            Ok(len) => {
                // 只有实际 push 了（len > 0）才写 AOF，复用 Push entry
                let aof = if len > 0 {
                    Some(AofEntry::Push(self.key, self.values, self.is_left))
                } else {
                    None
                };
                (Frame::Integer(len), aof)
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
