use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, extract_string, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

pub struct Linsert {
    key: Bytes,
    is_before: bool,
    pivot: Bytes,
    value: Bytes,
}

impl Linsert {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Linsert> {
        // LINSERT key BEFORE|AFTER pivot element
        if args.len() < 4 {
            return Err(anyhow!("ERR wrong number of arguments for 'linsert' command"));
        }
        let key = extract_bytes(&args[0])?;
        let position = extract_string(&args[1])?.to_uppercase();
        let is_before = match position.as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => return Err(anyhow!("ERR syntax error")),
        };
        let pivot = extract_bytes(&args[2])?;
        let value = extract_bytes(&args[3])?;
        Ok(Linsert { key, is_before, pivot, value })
    }
}

impl CommandExecute for Linsert {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let entry = AofEntry::Linsert(self.key.clone(), self.is_before, self.pivot.clone(), self.value.clone());
        match ctx.db().linsert(&self.key, self.is_before, &self.pivot, self.value) {
            Ok(len) => (Frame::Integer(len), Some(entry)),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
