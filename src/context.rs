use std::sync::Arc;
use crate::aof::AofEntry;
use crate::db::Db;
use crate::pubsub::PubSub;
use anyhow::Result;
use tokio::sync::mpsc::Sender;


pub struct Context {
    db: Arc<Db>,
    pub_sub: PubSub,
    aof_sender: Option<Sender<Vec<AofEntry>>>,
}

impl Context {
    pub fn new(db: Db, pub_sub: PubSub, aof_sender: Option<Sender<Vec<AofEntry>>>) -> Self {
        Context {
            db: Arc::new(db),
            pub_sub,
            aof_sender,
        }
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    pub fn pub_sub(&self) -> &PubSub {
        &self.pub_sub
    }

    /// 批量发送 AofEntry 到 AOF 写入通道
    pub async fn aof_send_batch(&self, entries: Vec<AofEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if let Some(sender) = &self.aof_sender {
            sender.send(entries).await?;
        }
        Ok(())
    }
}
