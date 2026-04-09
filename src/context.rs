
use crate::pubsub::PubSub;
use std::sync::Arc;
use crate::db::Db;

pub struct Context {
    db: Arc<Db>,
    pub_sub: PubSub,
}

impl Context {
    pub fn new(db: Db, pub_sub: PubSub) -> Self {
        Context {
            db: Arc::new(db),
            pub_sub,
        }
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    pub fn pub_sub(&self) -> &PubSub {
        &self.pub_sub
    }
}
