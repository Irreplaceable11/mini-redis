use crate::command::parse::extract_u64;
use crate::command::extract_string;
use crate::context::Context;
use crate::frame::Frame;
use anyhow::anyhow;

pub struct Scan {

    cursor: u64,

    pattern: Option<String>,

    count: Option<u64>
}

impl Scan {

    pub fn new( cursor: u64, pattern: Option<String>, count: Option<u64>) -> Self {
        Scan{cursor, pattern, count}
    }

    pub fn parse(args: &[Frame]) -> anyhow::Result<Scan> {
        if args.len() < 1 {
            return Err(anyhow!("ERR wrong number of arguments for 'scan' command"));
        }
        let mut pattern = None;
        let mut count = None;
        let cursor = extract_u64(&args[0])?;
        let mut i = 1;
        while i < args.len() {
            let keyword = extract_string(&args[i])?;
            match keyword.to_uppercase().as_str() {
                "MATCH" => {
                    if i + 1 >= args.len() {
                        return Err(anyhow!("ERR syntax error"));
                    }
                    pattern = Some(extract_string(&args[i + 1])?);
                },
                "COUNT" => {
                    if i + 1 >= args.len() {
                        return Err(anyhow!("ERR syntax error"));
                    }
                    count = Some(extract_u64(&args[i + 1])?);
                }
                _ => return Err(anyhow!("ERR syntax error")),
            }
            i += 2;
        }
        Ok(Scan::new(cursor, pattern, count))
    }

    /// SCAN 需要 async 执行，和 KEYS 类似，因为遍历 shard 可能耗时
    pub async fn execute(self, ctx: &Context) -> Frame {
        let pattern = self.pattern;
        let cursor = self.cursor;
        let count = self.count;
        let db = ctx.db().clone();

        let (next_cursor, keys) = tokio::task::spawn_blocking(move || {
            db.scan(pattern.as_deref(), cursor, count)
        })
        .await
        .unwrap_or((0, Vec::new()));

        // SCAN 返回格式：[cursor, [key1, key2, ...]]
        let keys_frame: Vec<Frame> = keys
            .into_iter()
            .map(Frame::BulkString)
            .collect();

        Frame::Array(vec![
            Frame::Integer(next_cursor as i64),
            Frame::Array(keys_frame),
        ])
    }
}