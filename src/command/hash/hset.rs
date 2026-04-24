use bytes::Bytes;
use crate::{command::CommandExecute, frame::Frame};
use anyhow::{anyhow, Result};
use crate::aof::AofEntry;
use crate::command::extract_bytes;
use crate::context::Context;

pub struct Hset {

    key: Bytes,

    fields: Vec<(Bytes, Bytes)>,
}

impl Hset {
    pub fn new(key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Hset {
        Hset { key, fields }
    }

    pub fn parse(args: &[Frame]) -> Result<Hset> {

        // 至少 3 个参数：key field value，且 field-value 必须成对
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return Err(anyhow!("ERR wrong number of arguments for 'hset' command"));
        }
        let key = extract_bytes(&args[0])?;
        let mut fields = Vec::with_capacity((args.len() - 1) / 2);
        let mut i = 1;
        while i < args.len() {
            let field = extract_bytes(&args[i])?;
            let val = extract_bytes(&args[i + 1])?;
            fields.push((field, val));
            i += 2;
        }
        Ok(Hset::new(key, fields))

    }
}

impl CommandExecute for Hset {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let aof = AofEntry::Hset(self.key.clone(), self.fields.clone());
        match ctx.db().hset(self.key, self.fields) {
            Ok(res) => (Frame::Integer(res), Some(aof)),
            Err(e) => (Frame::Error(Bytes::from_static(e.as_bytes())), None),
        }
    }
}