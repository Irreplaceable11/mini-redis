use crate::aof::{AofEntry, RewriteState};
use crate::db::Db;
use crate::pubsub::PubSub;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::Sender as WatchSender;

pub struct Context {
    db: Arc<Db>,
    pub_sub: PubSub,
    aof_sender: Option<Sender<Vec<AofEntry>>>,
    rewrite_sender: Option<WatchSender<RewriteState>>,
}

impl Context {
    pub fn new(
        db: Db,
        pub_sub: PubSub,
        aof_sender: Option<Sender<Vec<AofEntry>>>,
        rewrite_sender: Option<WatchSender<RewriteState>>,
    ) -> Self {
        Context {
            db: Arc::new(db),
            pub_sub,
            aof_sender,
            rewrite_sender,
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

    pub fn send_rewrite_state(&self, state: RewriteState) -> Result<()> {
        if let Some(sender) = &self.rewrite_sender {
            sender.send(state)?;
        }
        Ok(())
    }
}
