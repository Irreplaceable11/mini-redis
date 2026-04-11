use crate::command::extract_string;
use crate::context::Context;
use crate::{command::CommandExecute, frame::Frame};
use anyhow::{anyhow, Result};
use bytes::Bytes;

pub struct Ping {
    pub msg: Option<String>,
}


impl Ping {

    pub fn new(msg: Option<String>) -> Ping {
        Ping { msg }
    }


    // ping 命令行为
    // PING → 返回 "PONG"
    // PING "hello" → 返回 "hello"

    pub fn parse(args: &[Frame]) -> Result<Ping> {
        // args 是除了命令名之外的参数
        // PING 可以有 0 或 1 个参数
        match args.len() {
            // PING
            0 => Ok(Ping::new(None)),
            // PING "hello"
            1 => {
                let msg = extract_string(&args[0])?;
                Ok(Ping::new(Some(msg)))
            }
            _ => Err(anyhow!("ERR wrong number of arguments for 'ping' command")),
        }
    }

}

impl CommandExecute for Ping {
    fn execute(self, _ctx: &Context) -> (Frame, Option<crate::aof::AofEntry>) {
         match self.msg {
            None => (Frame::SimpleString(Bytes::from_static(b"PONG")), None),
            Some(msg) => (Frame::BulkString(msg.into()), None),
        }
    }
}