use crate::db::Db;
// use crate::pubsub::PubSub;

pub struct Context {
    db: Db,
    // pub_sub: PubSub
}

impl Context {
    pub fn new(db: Db) -> Self {
        Context {db }
    }
    
    pub fn db(&mut self) -> &mut Db {
        &mut self.db
    }
    
    // pub fn pub_sub(&self) -> &PubSub {
    //     &self.pub_sub
    // }
}
