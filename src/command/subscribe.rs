use crate::command::extract_string;
use crate::frame::Frame;
use anyhow::{anyhow, Result};

pub struct Subscribe {
    channels: Vec<String>
}

impl Subscribe {
    pub fn new(channels: Vec<String>) -> Self {
        Subscribe { channels }
    }
    
    pub fn channels(&self) -> &Vec<String> {
        &self.channels
    }

    pub fn parse(args: &[Frame]) -> Result<Subscribe> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'subscribe'"))
        }
        let vec = args.iter()
            .map(|arg| extract_string(arg))
            .collect::<Result<Vec<String>>>()?;
        Ok(Subscribe::new(vec))
    }
}
