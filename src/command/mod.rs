mod ping;
mod set;
mod parse;
mod get;

use crate::frame::Frame;
use anyhow::Result;
use tracing::info;
// 导出解析辅助函数，供各个命令模块使用
pub(crate) use parse::{extract_string, extract_bytes, extract_u32, extract_i64, extract_usize};
use crate::db::Db;

pub(crate) trait CommandExecute {
    fn execute(self, db: &Db) -> Frame;
}


pub enum Command {
    // TODO 增加 DEL EXISTS TTL PTTL EXPIRE KEYS 
    Ping(ping::Ping),
    Set(set::Set),
    Get(get::Get),
    Unknown(String),
}

impl Command {
    pub fn execute(self, db: &Db) -> Frame {
        match self {
            Command::Ping(cmd) => cmd.execute(db),
            Command::Set(cmd) => cmd.execute(db),
            Command::Get(cmd) => cmd.execute(db),
            Command::Unknown(cmd) => Frame::Error(format!("unknown command:{:?}", cmd)),
        }
    }
}



impl Command {
    
    pub fn from_frame(frame: Frame) -> Result<Command> {
        // 1. 检查是否是 Array
        // 2. 提取命令名（第一个元素）
        // 3. 根据命令名分发到对应模块
        // 4. 返回 Command 枚举
        let (cmd_name, arg) = Command::parse_array(frame)?;

        match cmd_name.as_str() {
            "PING" => Ok(Command::Ping(ping::Ping::parse(&arg)?)),
            "SET" => Ok(Command::Set(set::Set::parse(&arg)?)),
            "GET" => Ok(Command::Get(get::Get::parse(&arg)?)),
            _ => {
                info!("unknown command: {}", cmd_name);
                Ok(Command::Unknown(cmd_name))
            }
        }
    }

    fn parse_array(frame: Frame) -> Result<(String, Vec<Frame>)> {
        match frame {
            Frame::Array(mut arr) => {
                if arr.is_empty() {
                    return Err(anyhow::anyhow!("ERR empty command"));
                }

                // 移除并获取第一个元素
                let cmd_frame = arr.remove(0);

                let cmd_name = match cmd_frame {
                    Frame::BulkString(bytes) => {
                        String::from_utf8(bytes)?.to_uppercase()
                    }
                    _ => return Err(anyhow::anyhow!("ERR invalid command name"))
                };

                Ok((cmd_name, arr))  // 返回命令名和剩余参数
            }
            _ => Err(anyhow::anyhow!("ERR protocol error: expected array"))
        }
    }
}
