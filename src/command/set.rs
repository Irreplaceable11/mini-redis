use std::time::{Duration, Instant};
use crate::{
    command::parse::extract_u64,
    frame::Frame,
};
use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::command::{extract_bytes, extract_string, CommandExecute};
use crate::context::Context;

pub struct Set {
    pub key: String,

    pub value: Bytes,

    pub ttl: Option<Expiration>,

    pub nx: bool,

    pub xx: bool,
}

pub enum Expiration {
    Seconds(u64),
    Milliseconds(u64),
}
impl Set {
    pub fn new(
        key: String,
        value: Bytes,
        ttl: Option<Expiration>,
        nx: bool,
        xx: bool,
    ) -> Self {
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

        let key = extract_string(&args[0])?;

        let value = extract_bytes(&args[1])?;

        // 循环提取参数

        let mut ttl = None;
        let mut nx = false;
        let mut xx = false;
        // 从2开始排除key value
        let mut idx = 2;
        while idx < args.len() {
            let keyword = extract_string(&args[idx])?.to_uppercase();
            // EX PX不需要互斥 后者覆盖前者
            match keyword.as_str() {
                "EX" => {
                    if idx + 1 >= args.len() {
                        return Err(anyhow!("ERR syntax error: option '{}' requires an argument", keyword));
                    }
                    let val = extract_u64(&args[idx + 1])?;
                    ttl = Some(Expiration::Seconds(val));
                    idx += 2;
                }

                "PX" => {
                    if idx + 1 >= args.len() {
                        return Err(anyhow!("ERR syntax error: option '{}' requires an argument", keyword));
                    }
                    let val = extract_u64(&args[idx + 1])?;
                    ttl = Some(Expiration::Milliseconds(val));
                    idx += 2;
                }

                "NX" => {
                    if xx {
                        return Err(anyhow!("ERR XX and NX options at the same time are not compatible"));
                    }
                    nx = true;
                    idx += 1;
                }

                "XX" => {
                    if nx {
                        return Err(anyhow!("ERR XX and NX options at the same time are not compatible"));
                    }
                    xx = true;
                    idx += 1;
                }

                _ => {
                    // 遇到不认识的关键字
                    return Err(anyhow!("ERR syntax error: unknown option '{}'", keyword));
                }
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
    fn execute(self, ctx: &Context) -> Frame {
        let instant = self.expires_at_direct();
        match ctx.db().set(&self.key, self.value, instant, self.nx, self.xx) {
            Some(_) => Frame::SimpleString("OK".to_string()),
            None => Frame::Null
        }

    }
}
