
use crate::frame::Frame;
use anyhow::{anyhow, Result};
use crate::command::{extract_string, CommandExecute};
use crate::context::Context;

pub struct Exists {

    keys: Vec<String>
}

impl Exists {

    pub fn new(keys: Vec<String>) ->  Exists{
        Exists { keys }
    }

    pub fn parse(args: &[Frame]) -> Result<Exists> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'exists'"));
        }
        let keys = args
            .into_iter()
            .map(extract_string)
            .collect::<Result<Vec<String>>>()?;
        Ok(Exists { keys })
    }
    
}

impl CommandExecute for Exists {
    fn execute(self, ctx: &Context) -> Frame {
        let res = ctx.db().exists(self.keys);
        Frame::Integer(res as i64)
    }
}