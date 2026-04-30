use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::command::extract_f64;
use crate::context::Context;
use crate::frame::Frame;

/// HINCRBYFLOAT key field increment
pub struct Hincrbyfloat {
    key: Bytes,
    field: Bytes,
    delta: f64,
}

impl Hincrbyfloat {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hincrbyfloat> {
        if args.len() < 3 {
            return Err(anyhow!("ERR wrong number of arguments for 'hincrbyfloat' command"));
        }
        let key = extract_bytes(&args[0])?;
        let field = extract_bytes(&args[1])?;
        let delta = extract_f64(&args[2])?;
        Ok(Hincrbyfloat { key, field, delta })
    }
}

impl CommandExecute for Hincrbyfloat {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hincrbyfloat(self.key, self.field, self.delta) {
            Ok(val) => (Frame::BulkString(val), None),
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
