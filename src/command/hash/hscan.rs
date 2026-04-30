use anyhow::anyhow;
use bytes::Bytes;
use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::command::parse::extract_u64;
use crate::command::extract_string;
use crate::context::Context;
use crate::frame::Frame;

/// HSCAN key cursor [MATCH pattern] [COUNT count]
pub struct Hscan {
    key: Bytes,
    cursor: u64,
    pattern: Option<String>,
    count: Option<u64>,
}

impl Hscan {
    pub fn parse(args: &[Frame]) -> anyhow::Result<Hscan> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'hscan' command"));
        }
        let key = extract_bytes(&args[0])?;
        let cursor = extract_u64(&args[1])?;
        let mut pattern = None;
        let mut count = None;
        let mut i = 2;
        while i < args.len() {
            let keyword = extract_string(&args[i])?;
            match keyword.to_uppercase().as_str() {
                "MATCH" => {
                    if i + 1 >= args.len() {
                        return Err(anyhow!("ERR syntax error"));
                    }
                    pattern = Some(extract_string(&args[i + 1])?);
                }
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
        Ok(Hscan { key, cursor, pattern, count })
    }
}

impl CommandExecute for Hscan {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match ctx.db().hscan(&self.key, self.pattern.as_deref(), self.cursor, self.count) {
            Ok((next_cursor, pairs)) => {
                // 返回格式：[cursor, [field1, value1, field2, value2, ...]]
                let mut items = Vec::with_capacity(pairs.len() * 2);
                for (field, value) in pairs {
                    items.push(Frame::BulkString(field));
                    items.push(Frame::BulkString(value));
                }
                let resp = Frame::Array(vec![
                    Frame::Integer(next_cursor as i64),
                    Frame::Array(items),
                ]);
                (resp, None)
            }
            Err(err) => (Frame::Error(Bytes::from_static(err.as_bytes())), None),
        }
    }
}
