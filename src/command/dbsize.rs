use crate::aof::AofEntry;
use crate::command::CommandExecute;
use crate::context::Context;
use crate::frame::Frame;

pub struct DbSize;

impl DbSize {
    pub fn parse(_args: &[Frame]) -> anyhow::Result<DbSize> {
        Ok(DbSize)
    }
}

impl CommandExecute for DbSize {
    fn execute(self, ctx: &Context) -> (Frame, Option<AofEntry>) {
        let mut count: i64 = 0;
        for shard in ctx.db().shards.iter() {
            count += shard.len() as i64;
        }
        (Frame::Integer(count), None)
    }
}
