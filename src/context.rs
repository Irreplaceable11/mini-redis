use crate::db_opt::DbOpt;
use crate::pubsub::PubSub;
use std::sync::Arc;

pub struct Context {
    db: Arc<DbOpt>,
    pub_sub: PubSub,
}

impl Context {
    pub fn new(db: DbOpt, pub_sub: PubSub) -> Self {
        Context {
            db: Arc::new(db),
            pub_sub,
        }
    }

    pub fn db(&self) -> &Arc<DbOpt> {
        &self.db
    }

    pub fn pub_sub(&self) -> &PubSub {
        &self.pub_sub
    }
}
