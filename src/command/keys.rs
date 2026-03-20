use crate::command::extract_string;
use crate::context::Context;
use crate::frame::Frame;
use anyhow::anyhow;
use anyhow::Result;

pub struct Keys {
    pub key_pattern: String,
}

impl Keys {
    pub fn new(key_pattern: String) -> Self {
        Keys { key_pattern }
    }

    pub fn parse(args: &[Frame]) -> Result<Keys> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'keys'"));
        }
        let key_pattern = extract_string(&args[0])?;
        Ok(Keys::new(key_pattern))
    }

    /// 单独的 async execute，因为底层 db.keys() 需要 spawn_blocking
    pub async fn execute(self, ctx: &Context) -> Frame {
        let vec = ctx.db().keys(&self.key_pattern).await;
        let results: Vec<Frame> = vec
            .into_iter()
            .map(|k| Frame::BulkString(k))
            .collect();
        Frame::Array(results)
    }
}
