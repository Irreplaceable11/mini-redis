use anyhow::anyhow;
use crate::command::extract_string;
use crate::frame::Frame;
use anyhow::Result;

pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Unsubscribe {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }
    
    
    pub fn parse(args: &[Frame]) -> Result<Unsubscribe> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'subscribe'"))
        }
        let vec = args.iter()
            .map(|arg| extract_string(arg))
            .collect::<Result<Vec<String>>>()?;
        Ok(Unsubscribe::new(vec))
    }
    
    pub fn channels(&self) -> &Vec<String> {
        &self.channels
    }
}