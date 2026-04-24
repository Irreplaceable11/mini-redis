use std::time::{Duration, Instant};

use crate::command::parse::extract_u64;
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::aof::AofEntry;
use crate::command::{extract_bytes, CommandExecute};
use crate::context::Context;

pub struct Set {
    pub key: Bytes,

    pub value: Bytes,

    pub ttl: Option<Expiration>,

    pub nx: bool,

    pub xx: bool,
}

pub enum Expiration {
    Seconds(u64),
    Milliseconds(u64),
}

/// 在 &[u8] 上做 ASCII 大小写无关比较，避免 to_string + to_uppercase 的堆分配
#[inline]
fn bytes_eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// 从 Frame 中提取原始字节切片引用，不做任何拷贝
#[inline]
fn extract_bytes_ref(frame: &Frame) -> Result<&[u8]> {
    match frame {
        Frame::BulkString(bytes) => Ok(bytes),
        _ => Err(anyhow!("ERR expect BulkString")),
    }
}

impl Set {
    pub fn new(key: Bytes, value: Bytes, ttl: Option<Expiration>, nx: bool, xx: bool) -> Self {
        Set {
            key,
            value,
            ttl,
            nx,
            xx,
        }
    }

    pub fn parse(args: &[Frame]) -> Result<Set> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'set' command"));
        }

        let key = extract_bytes(&args[0])?;
        let value = extract_bytes(&args[1])?;

        let mut ttl = None;
        let mut nx = false;
        let mut xx = false;
        let mut idx = 2;

        while idx < args.len() {
            let keyword = extract_bytes_ref(&args[idx])?;

            if bytes_eq_ignore_ascii_case(keyword, b"EX") {
                if idx + 1 >= args.len() {
                    return Err(anyhow!(
                        "ERR syntax error: option 'EX' requires an argument"
                    ));
                }
                let val = extract_u64(&args[idx + 1])?;
                ttl = Some(Expiration::Seconds(val));
                idx += 2;
            } else if bytes_eq_ignore_ascii_case(keyword, b"PX") {
                if idx + 1 >= args.len() {
                    return Err(anyhow!(
                        "ERR syntax error: option 'PX' requires an argument"
                    ));
                }
                let val = extract_u64(&args[idx + 1])?;
                ttl = Some(Expiration::Milliseconds(val));
                idx += 2;
            } else if bytes_eq_ignore_ascii_case(keyword, b"NX") {
                if xx {
                    return Err(anyhow!(
                        "ERR XX and NX options at the same time are not compatible"
                    ));
                }
                nx = true;
                idx += 1;
            } else if bytes_eq_ignore_ascii_case(keyword, b"XX") {
                if nx {
                    return Err(anyhow!(
                        "ERR XX and NX options at the same time are not compatible"
                    ));
                }
                xx = true;
                idx += 1;
            } else {
                return Err(anyhow!(
                    "ERR syntax error: unknown option '{}'",
                    String::from_utf8_lossy(keyword)
                ));
            }
        }

        Ok(Set::new(key, value, ttl, nx, xx))
    }

    pub fn expires_at_direct(&self) -> Option<Instant> {
        match &self.ttl {
            Some(exp) => {
                let duration = match exp {
                    Expiration::Seconds(secs) => Duration::from_secs(*secs),
                    Expiration::Milliseconds(ms) => Duration::from_millis(*ms),
                };
                Some(Instant::now() + duration)
            }
            None => None,
        }
    }
}

impl CommandExecute for Set {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let instant = self.expires_at_direct();
        match ctx
            .db()
            .set(self.key.clone(), self.value.clone(), instant, self.nx, self.xx)
        {
            Some(_) => {
                let entry = AofEntry::from_set(self.key, self.value, instant, self.nx, self.xx);
                (Frame::SimpleString(Bytes::from_static(b"OK")), Some(entry))
            }
            None => (Frame::Null, None),
        }
    }
}
