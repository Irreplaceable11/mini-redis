pub mod ping;
pub mod set;
pub mod parse;
pub mod get;
pub mod del;
pub mod exists;
pub mod ttl;
pub mod expire;
pub mod keys;
pub mod publish;
pub mod subscribe;
pub mod unsubscribe;
mod bgrewriteaof;
pub mod incr;
pub mod decr;
pub mod incrby;
pub mod decrby;

use crate::aof::AofEntry;
use crate::frame::Frame;
use crate::context::Context;
use anyhow::Result;
use bytes::Bytes;
use smallvec::SmallVec;
use tracing::info;
// 导出解析辅助函数，供各个命令模块使用
pub(crate) use parse::{extract_bytes, extract_i64, extract_string};

pub(crate) trait CommandExecute {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>);
}


pub enum Command {
    Ping(ping::Ping),
    Set(set::Set),
    Get(get::Get),
    Del(del::Del),
    Exist(exists::Exists),
    Ttl(ttl::Ttl),
    Expire(expire::Expire),
    Keys(keys::Keys),
    Incr(incr::Incr),
    Decr(decr::Decr),
    IncrBy(incrby::IncrBy),
    DecrBy(decrby::DecrBy),
    Publish(publish::Publish),
    Subscribe(subscribe::Subscribe),
    Unsubscribe(unsubscribe::Unsubscribe),
    BgRewriteAof(bgrewriteaof::BgRewriteAof),
    Unknown(String),
}

impl Command {
    pub fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        match self {
            Command::Ping(cmd) => cmd.execute(ctx),
            Command::Set(cmd) => cmd.execute(ctx),
            Command::Get(cmd) => cmd.execute(ctx),
            Command::Del(cmd) => cmd.execute(ctx),
            Command::Exist(cmd) => cmd.execute(ctx),
            Command::Ttl(cmd) => cmd.execute(ctx),
            Command::Expire(cmd) => cmd.execute(ctx),
            Command::Incr(cmd) => cmd.execute(ctx),
            Command::Decr(cmd) => cmd.execute(ctx),
            Command::IncrBy(cmd) => cmd.execute(ctx),
            Command::DecrBy(cmd) => cmd.execute(ctx),
            Command::Publish(cmd)  => cmd.execute(ctx),
            Command::BgRewriteAof(cmd) => cmd.execute(ctx),
            Command::Unknown(cmd) => (Frame::Error(Bytes::from(format!("Command failed, unknown command:{:?}", cmd))), None),
            _ => (Frame::Error(Bytes::from("ERR command not implemented".to_string())), None),
        }
    }
}



impl Command {
    
    pub fn from_frame(frame: Frame) -> Result<Command> {
        // 1. 检查是否是 Array
        // 2. 提取命令名（第一个元素）
        // 3. 根据命令名分发到对应模块
        // 4. 返回 Command 枚举
        let (buf, len, arg) = Command::parse_array(frame)?;
        let cmd_name = &buf[..len];
        match &cmd_name[..] {
            b"PING" => Ok(Command::Ping(ping::Ping::parse(&arg)?)),
            b"SET" => Ok(Command::Set(set::Set::parse(&arg)?)),
            b"GET" => Ok(Command::Get(get::Get::parse(&arg)?)),
            b"DEL" => Ok(Command::Del(del::Del::parse(&arg)?)),
            b"EXIST" => Ok(Command::Exist(exists::Exists::parse(&arg)?)),
            b"TTL" | b"PTTL" => Ok(Command::Ttl(ttl::Ttl::parse(&cmd_name, &arg)?)),
            b"EXPIRE" | b"PEXPIRE" => Ok(Command::Expire(expire::Expire::parse(&cmd_name, &arg)?)),
            b"KEYS" => Ok(Command::Keys(keys::Keys::parse(&arg)?)),
            b"PUBLISH" => Ok(Command::Publish(publish::Publish::parse(&arg)?)),
            b"SUBSCRIBE" => Ok(Command::Subscribe(subscribe::Subscribe::parse(&arg)?)),
            b"UNSUBSCRIBE" => Ok(Command::Unsubscribe(unsubscribe::Unsubscribe::parse(&arg)?)),
            b"BGREWRITEAOF" => Ok(Command::BgRewriteAof(bgrewriteaof::BgRewriteAof::parse(&arg)?)),
            b"INCR" => Ok(Command::Incr(incr::Incr::parse(&arg)?)),
            b"DECR" => Ok(Command::Decr(decr::Decr::parse(&arg)?)),
            b"INCRBY" => Ok(Command::IncrBy(incrby::IncrBy::parse(&arg)?)),
            b"DECRBY" => Ok(Command::DecrBy(decrby::DecrBy::parse(&arg)?)),
            _ => {
                let cmd_name_string = str::from_utf8(cmd_name)?;
                info!("unknown command: {}", cmd_name_string);
                Ok(Command::Unknown(cmd_name_string.to_string()))
            }
        }
    }

    fn parse_array(frame: Frame) -> Result<([u8; 16], usize, SmallVec<[Frame; 5]>)> {
        match frame {
            Frame::Array(arr) => {
                if arr.is_empty() {
                    return Err(anyhow::anyhow!("ERR empty command"));
                }

                // 移除并获取第一个元素
                let mut iter = arr.into_iter();
                let cmd_frame = iter.next().expect("checked non-empty");

                let bytes = match cmd_frame {
                    Frame::BulkString(bytes) => {
                        bytes
                    }
                    _ => return Err(anyhow::anyhow!("ERR invalid command name"))
                };

                // 栈上 buffer，16 字节足够所有 Redis 命令
                let mut buf = [0u8; 16];
                let len = bytes.len().min(16);

                for i in 0..len {
                    buf[i] = bytes[i].to_ascii_uppercase();
                }
                Ok((buf, len, iter.collect::<SmallVec<[Frame; 5]>>()))
            }
            _ => Err(anyhow::anyhow!("ERR protocol error: expected array"))
        }
    }
}
