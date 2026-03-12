use crate::command::{extract_string, CommandExecute};
use crate::db::Db;
use crate::frame::Frame;
use anyhow::Result;

pub struct Get {
    pub key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Get { key }
    }

    pub fn parse(args: &[Frame]) -> Result<Get> {
        let key = extract_string(&args[0]);
        Ok(Get::new(key?))
    }
}

impl CommandExecute for Get {
    fn execute(self, db: &Db) -> Frame {
        match db.get(&self.key) {
            Some(value) => Frame::BulkString(value),
            None => Frame::Null,
        }

    }
}