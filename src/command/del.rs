use crate::command::{CommandExecute, extract_string};
use crate::context::Context;
use crate::frame::Frame;
use anyhow::{Result, anyhow};

pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub fn new(keys: Vec<String>) -> Del {
        Del { keys }
    }

    pub fn parse(args: &[Frame]) -> Result<Del> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'del'"));
        }
        let keys: Vec<String> = args
            .iter()
            .map(extract_string)
            .collect::<Result<Vec<String>>>()?;

        Ok(Del::new(keys))
    }
}

impl CommandExecute for Del {
    fn execute(self, ctx: &Context) -> Frame {
        let res = ctx.db().del(self.keys);
        Frame::Integer(res as i64)
    }
}
