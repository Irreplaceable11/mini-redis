use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, extract_i64, extract_string, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;

/// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
pub struct Lpos {
    key: Bytes,
    element: Bytes,
    rank: Option<i64>,
    count: Option<u64>,
    max_len: Option<u64>,
}

impl Lpos {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Lpos> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'lpos' command"));
        }
        let key = extract_bytes(&args[0])?;
        let element = extract_bytes(&args[1])?;
        let mut rank = None;
        let mut count = None;
        let mut max_len = None;

        let mut i = 2;
        while i < args.len() {
            let opt = extract_string(&args[i])?.to_uppercase();
            match opt.as_str() {
                "RANK" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(anyhow!("ERR syntax error"));
                    }
                    rank = Some(extract_i64(&args[i])?);
                }
                "COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(anyhow!("ERR syntax error"));
                    }
                    let v = extract_i64(&args[i])?;
                    if v < 0 {
                        return Err(anyhow!("ERR COUNT can't be negative"));
                    }
                    count = Some(v as u64);
                }
                "MAXLEN" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(anyhow!("ERR syntax error"));
                    }
                    let v = extract_i64(&args[i])?;
                    if v < 0 {
                        return Err(anyhow!("ERR MAXLEN can't be negative"));
                    }
                    max_len = Some(v as u64);
                }
                _ => return Err(anyhow!("ERR syntax error")),
            }
            i += 1;
        }
        Ok(Lpos { key, element, rank, count, max_len })
    }
}

impl CommandExecute for Lpos {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().pos(&self.key, self.element, self.rank, self.count, self.max_len) {
            Ok(positions) => {
                // 如果调用时指定了 COUNT，返回 Array；否则返回单个 Integer 或 Nil
                if self.count.is_some() {
                    let arr = positions.iter().map(|&idx| Frame::Integer(idx)).collect();
                    (Frame::Array(arr), None)
                } else if let Some(&idx) = positions.first() {
                    (Frame::Integer(idx), None)
                } else {
                    (Frame::Null, None)
                }
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
