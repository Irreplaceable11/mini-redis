use crate::frame::Frame;
use crate::command::extract_string;
use anyhow::{anyhow, Result};

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

    pub fn into_frame(self) -> Frame {
        match self.msg {
            None => Frame::SimpleString("PONG".to_string()),
            Some(msg) => Frame::BulkString(msg.into())
        }
    }
}