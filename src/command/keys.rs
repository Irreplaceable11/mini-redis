use crate::command::CommandExecute;
use crate::command::extract_string;
use crate::db::Db;
use crate::frame::Frame;
use anyhow::Result;
use anyhow::anyhow;
use bytes::Bytes;

pub struct Keys {
    pub key_pattern: String,
}

impl Keys {
    pub fn new(key_pattern: String) -> Self {
        Keys { key_pattern }
    }

    pub fn parse(args: &[Frame]) -> Result<Keys> {
        if args.is_empty() {
            return Err(anyhow!("wrong arg number for command 'keys'"));
        }
        let key_pattern = extract_string(&args[0])?;
        Ok(Keys::new(key_pattern))
    }
}

impl CommandExecute for Keys {
    fn execute(self, db: &Db) -> Frame {
        let vec = db.keys(&self.key_pattern);
        let mut results = Vec::with_capacity(vec.len());
        for ele in vec {
            results.push(Frame::BulkString(Bytes::from(ele.to_string())));
        }
        Frame::Array(results)
    }
}
