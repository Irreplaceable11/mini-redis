use crate::command::{extract_string, CommandExecute};
use crate::context::Context;
use crate::frame::Frame;
use anyhow::{anyhow, Result};

pub struct Ttl {
    pub key: String,

    pub ttl_type: TtlType
}
#[derive(PartialEq)]
pub enum TtlType {
    Seconds,
    Milliseconds,
}

impl Ttl {
    pub fn new(key: String, ttl_type: TtlType) -> Self {
        Ttl { key , ttl_type}
    }

    pub fn parse(cmd_name: &[u8], args: &[Frame]) -> Result<Ttl> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'ttl or pttl'"));
        }
        match &cmd_name[..] {
            b"TTL" => Ok(Ttl::new(extract_string(&args[0])?, TtlType::Seconds)),
            b"PTTL" => Ok(Ttl::new(extract_string(&args[0])?, TtlType::Milliseconds)),
            _ => Err(anyhow!("ERR unknown TTL command")),
        }

    }
}


impl CommandExecute for Ttl {
    fn execute(self, ctx: &mut Context) -> Frame {
        let remaining = ctx.db().ttl(&self.key, self.ttl_type == TtlType::Milliseconds);
        Frame::Integer(remaining)
    }
}