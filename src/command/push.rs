use anyhow::anyhow;
use crate::frame::Frame;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;

pub struct Push {
    pub key: Bytes,

    pub values: Vec<Bytes>,

    pub is_left: bool,
}

impl Push {
    pub fn new(key: Bytes, values: Vec<Bytes>, is_left: bool) -> Self {
        Push {key, values, is_left}
    }

    pub fn parse(args: &[Frame], is_left: bool) -> anyhow::Result<Push> {
        if args.len() < 2 {
            let command_name = if is_left { "lpush" } else { "rpush" };
            return Err(anyhow!("wrong arg number for command '{}'", command_name));
        }
        let key = extract_bytes(&args[0])?;
        let mut values = Vec::with_capacity(args.len());
        for arg in &args[1..] {
            let value = extract_bytes(arg)?;
            values.push(value);
        }
        Ok(Push {key, values, is_left})
    }
}

impl CommandExecute for Push {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let aof_entry = AofEntry::Push(self.key.clone(), self.values.clone(), self.is_left);
        match ctx.db().push(self.key, self.values, self.is_left) {
            Ok(cnt) => (Frame::Integer(cnt), Some(aof_entry)),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}