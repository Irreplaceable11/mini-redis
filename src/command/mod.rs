pub mod parse;
pub mod string;
pub mod list;
pub mod key;
pub mod pubsub;
pub mod server;
pub mod hash;

use crate::aof::AofEntry;
use crate::frame::Frame;
use crate::context::Context;
use anyhow::Result;
use bytes::Bytes;
use smallvec::SmallVec;
use tracing::info;

// 导出解析辅助函数，供各个命令模块使用
pub(crate) use parse::{extract_bytes, extract_i64, extract_f64, extract_string};

// re-export 子模块中的类型，保持外部引用兼容
pub use string::{set, get, incr, decr, incrby, decrby, incrbyfloat};
pub use list::{push, pushx, pop, bpop, llen, lrange, lindex, lset, lrem, linsert, lpos, ltrim, lmove};
pub use key::{del, exists, expire, ttl, keys, scan, type_cmd};
pub use pubsub::{publish, subscribe, unsubscribe};
pub use server::{ping, select, dbsize, info as info_cmd, config, command_cmd, client, hello, bgrewriteaof};
pub use hash::{hset, hget};

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
    IncrByFloat(incrbyfloat::IncrByFloat),
    Push(push::Push),
    Pop(pop::Pop),
    Llen(llen::Llen),
    Lrange(lrange::Lrange),
    Lindex(lindex::Lindex),
    Lrem(lrem::Lrem),
    Lset(lset::Lset),
    Linsert(linsert::Linsert),
    Pushx(pushx::Pushx),
    Lpos(lpos::Lpos),
    Ltrim(ltrim::Ltrim),
    Lmove(lmove::Lmove),
    Hset(hset::Hset),
    Hget(hget::Hget),
    Publish(publish::Publish),
    Subscribe(subscribe::Subscribe),
    Unsubscribe(unsubscribe::Unsubscribe),
    BgRewriteAof(bgrewriteaof::BgRewriteAof),
    Scan(scan::Scan),
    Info(info_cmd::Info),
    DbSize(dbsize::DbSize),
    CommandCmd(command_cmd::CommandCmd),
    Config(config::Config),
    Type(type_cmd::TypeCmd),
    Select(select::Select),
    Client(client::Client),
    Hello(hello::Hello),
    BPop(bpop::BPop),
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
            Command::IncrByFloat(cmd) => cmd.execute(ctx),
            Command::Push(cmd) => cmd.execute(ctx),
            Command::Pop(cmd) => cmd.execute(ctx),
            Command::Llen(cmd) => cmd.execute(ctx),
            Command::Lrange(cmd) => cmd.execute(ctx),
            Command::Lindex(cmd) => cmd.execute(ctx),
            Command::Lrem(cmd) => cmd.execute(ctx),
            Command::Lset(cmd) => cmd.execute(ctx),
            Command::Linsert(cmd) => cmd.execute(ctx),
            Command::Pushx(cmd) => cmd.execute(ctx),
            Command::Lpos(cmd) => cmd.execute(ctx),
            Command::Ltrim(cmd) => cmd.execute(ctx),
            Command::Lmove(cmd) => cmd.execute(ctx),
            Command::Hset(cmd) => cmd.execute(ctx),
            Command::Hget(cmd) => cmd.execute(ctx),
            Command::Publish(cmd) => cmd.execute(ctx),
            Command::BgRewriteAof(cmd) => cmd.execute(ctx),
            Command::Info(cmd) => cmd.execute(ctx),
            Command::DbSize(cmd) => cmd.execute(ctx),
            Command::CommandCmd(cmd) => cmd.execute(ctx),
            Command::Config(cmd) => cmd.execute(ctx),
            Command::Type(cmd) => cmd.execute(ctx),
            Command::Select(cmd) => cmd.execute(ctx),
            Command::Client(cmd) => cmd.execute(ctx),
            Command::Hello(cmd) => cmd.execute(ctx),
            Command::BPop(_) => (Frame::Error(Bytes::from("ERR bpop should be handled async")), None),
            Command::Unknown(cmd) => (Frame::Error(Bytes::from(format!("Command failed, unknown command:{:?}", cmd))), None),
            _ => (Frame::Error(Bytes::from("ERR command not implemented".to_string())), None),
        }
    }
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let (buf, len, arg) = Command::parse_array(frame)?;
        let cmd_name = &buf[..len];
        match &cmd_name[..] {
            b"PING" => Ok(Command::Ping(ping::Ping::parse(&arg)?)),
            b"SET" => Ok(Command::Set(set::Set::parse(&arg)?)),
            b"GET" => Ok(Command::Get(get::Get::parse(&arg)?)),
            b"DEL" => Ok(Command::Del(del::Del::parse(&arg)?)),
            b"EXISTS" => Ok(Command::Exist(exists::Exists::parse(&arg)?)),
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
            b"INCRBYFLOAT" => Ok(Command::IncrByFloat(incrbyfloat::IncrByFloat::parse(&arg)?)),
            b"LPUSH" => Ok(Command::Push(push::Push::parse(&arg, true)?)),
            b"RPUSH" => Ok(Command::Push(push::Push::parse(&arg, false)?)),
            b"LPOP" => Ok(Command::Pop(pop::Pop::parse(&arg, true)?)),
            b"RPOP" => Ok(Command::Pop(pop::Pop::parse(&arg, false)?)),
            b"LLEN" => Ok(Command::Llen(llen::Llen::parse(&arg)?)),
            b"LRANGE" => Ok(Command::Lrange(lrange::Lrange::parse(&arg)?)),
            b"LINDEX" => Ok(Command::Lindex(lindex::Lindex::parse(&arg)?)),
            b"LREM" => Ok(Command::Lrem(lrem::Lrem::parse(&arg)?)),
            b"LSET" => Ok(Command::Lset(lset::Lset::parse(&arg)?)),
            b"LINSERT" => Ok(Command::Linsert(linsert::Linsert::parse(&arg)?)),
            b"LPUSHX" => Ok(Command::Pushx(pushx::Pushx::parse(&arg, true)?)),
            b"RPUSHX" => Ok(Command::Pushx(pushx::Pushx::parse(&arg, false)?)),
            b"LPOS" => Ok(Command::Lpos(lpos::Lpos::parse(&arg)?)),
            b"LTRIM" => Ok(Command::Ltrim(ltrim::Ltrim::parse(&arg)?)),
            b"LMOVE" => Ok(Command::Lmove(lmove::Lmove::parse(&arg)?)),
            b"HSET" => Ok(Command::Hset(hset::Hset::parse(&arg)?)),
            b"HGET" => Ok(Command::Hget(hget::Hget::parse(&arg)?)),
            b"BLPOP" => Ok(Command::BPop(bpop::BPop::parse(&arg, true)?)),
            b"BRPOP" => Ok(Command::BPop(bpop::BPop::parse(&arg, false)?)),
            b"SCAN" => Ok(Command::Scan(scan::Scan::parse(&arg)?)),
            b"INFO" => Ok(Command::Info(info_cmd::Info::parse(&arg)?)),
            b"DBSIZE" => Ok(Command::DbSize(dbsize::DbSize::parse(&arg)?)),
            b"COMMAND" => Ok(Command::CommandCmd(command_cmd::CommandCmd::parse(&arg)?)),
            b"CONFIG" => Ok(Command::Config(config::Config::parse(&arg)?)),
            b"TYPE" => Ok(Command::Type(type_cmd::TypeCmd::parse(&arg)?)),
            b"SELECT" => Ok(Command::Select(select::Select::parse(&arg)?)),
            b"CLIENT" => Ok(Command::Client(client::Client::parse(&arg)?)),
            b"HELLO" => Ok(Command::Hello(hello::Hello::parse(&arg)?)),
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

                let mut iter = arr.into_iter();
                let cmd_frame = iter.next().expect("checked non-empty");

                let bytes = match cmd_frame {
                    Frame::BulkString(bytes) => bytes,
                    _ => return Err(anyhow::anyhow!("ERR invalid command name"))
                };

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
