use std::sync::Arc;
use crate::db::ShardedDb;

pub struct Context {
    db: Arc<ShardedDb>,
}

impl Context {
    pub fn new(db: Arc<ShardedDb>) -> Self {
        Context { db }
    }

    pub fn db(&self) -> &ShardedDb {
        &self.db
    }
}
