use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// HMGET key field [field ...]
pub struct Hmget {
    key: Bytes,
    fields: Vec<Bytes>,
}

impl Hmget {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hmget> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'hmget' command"));
        }
        let key = extract_bytes(&args[0])?;
        let mut fields = Vec::with_capacity(args.len() - 1);
        for i in 1..args.len() {
            fields.push(extract_bytes(&args[i])?);
        }
        Ok(Hmget { key, fields })
    }
}

impl CommandExecute for Hmget {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let field_refs: Vec<&Bytes> = self.fields.iter().collect();
        match ctx.db().hmget(&self.key, field_refs) {
            Ok(values) => {
                let frames: Vec<Frame> = values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(v) => Frame::BulkString(v),
                        None => Frame::Null,
                    })
                    .collect();
                (Frame::Array(frames), None)
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
