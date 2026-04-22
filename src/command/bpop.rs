use crate::command::extract_bytes;
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use crate::context::Context;
use crate::command::parse::extract_u64;

pub struct BPop {
    keys: Vec<Bytes>,
    timeout: u64,
    is_left: bool,
}

impl BPop {
    pub fn parse(args: &[Frame], is_left: bool) -> Result<BPop> {
        // BLPOP key1 [key2 ...] timeout
        // 至少需要 2 个参数：一个 key + timeout
        if args.len() < 2 {
            let cmd = if is_left { "blpop" } else { "brpop" };
            return Err(anyhow!("ERR wrong number of arguments for '{}' command", cmd));
        }
        // 最后一个是 timeout
        let timeout = extract_u64(args.last().unwrap())?;
        // 前面的都是 key
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .map(|f| extract_bytes(f))
            .collect::<Result<Vec<_>>>()?;

        Ok(BPop { keys, timeout, is_left })
    }

    /// async 执行，不走 CommandExecute trait
    pub async fn execute(self, ctx: &Context) -> Frame {
        match ctx.db().bpop(self.keys, self.timeout, self.is_left).await {
            Ok(result) if result.is_empty() => Frame::Null,
            Ok(result) => {
                // 返回 [key, value] 的数组
                let frames = result.into_iter().map(Frame::BulkString).collect();
                Frame::Array(frames)
            }
            Err(err) => Frame::Error(Bytes::from_static(err.as_bytes())),
        }
    }
}
