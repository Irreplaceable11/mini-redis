use crate::aof::AofEntry;
use crate::frame::Frame;
use std::time::{Duration, Instant};

use crate::command::{extract_bytes, extract_i64, CommandExecute};
use crate::context::Context;
use anyhow::{anyhow, Result};
use bytes::Bytes;

pub struct Expire {
    pub key: Bytes,
    pub expire_at: ExpireType,
}

#[derive(PartialEq)]
pub enum ExpireType {
    Seconds(i64),
    Milliseconds(i64),
}

impl Expire {
    pub fn new(key: Bytes, expire_at: ExpireType) -> Expire {
        Expire { key, expire_at }
    }

    pub fn parse(cmd_name: &[u8], args: &[Frame]) -> Result<Expire> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'expire'"));
        }
        let key = extract_bytes(&args[0])?;
        let expire_at = extract_i64(&args[1])?;
        match &cmd_name[..] {
            b"EXPIRE" => Ok(Expire::new(key, ExpireType::Seconds(expire_at))),
            b"PEXPIRE" => Ok(Expire::new(key, ExpireType::Milliseconds(expire_at))),
            _ => Err(anyhow!("wrong command 'expire'")),
        }
    }

    pub fn expires_at_direct(&self) -> Option<Instant> {
        match &self.expire_at {
            ExpireType::Seconds(secs) => {
                if *secs > 0 {
                    return Some(Instant::now() + Duration::from_secs(*secs as u64));
                }
            }
            ExpireType::Milliseconds(ms) => {
                if *ms > 0 {
                    return Some(Instant::now() + Duration::from_millis(*ms as u64));
                }
            }
        }
        None
    }
}

impl CommandExecute for Expire {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let expire_at = self.expires_at_direct();
        let result = ctx.db().expire(&self.key, expire_at);
        if result > 0 {
            let entry = AofEntry::from_expire(self.key, expire_at);
            (Frame::Integer(result as i64), Some(entry))
        } else {
            (Frame::Integer(0), None)
        }
    }
}
