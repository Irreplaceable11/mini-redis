use crate::db::Db;
use crate::pubsub::PubSub;

pub struct Context {
    db: Db,
    pub_sub: PubSub
}

impl Context {
    pub fn new(db: Db, pub_sub: PubSub) -> Self {
        Context {db, pub_sub }
    }
    
    pub fn db(&self) -> &Db {
        &self.db
    }
    
    pub fn pub_sub(&self) -> &PubSub {
        &self.pub_sub
    }
}
