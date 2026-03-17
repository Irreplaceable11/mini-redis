use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast::{Receiver, Sender};

pub struct PubSub {
    shards: DashMap<String, Sender<Bytes>>,
}

impl PubSub {
    pub fn new() -> PubSub {
        PubSub { shards: DashMap::new() }
    }


    pub fn subscribe(&self , channel_name: &str) -> Receiver<Bytes> {
        self.shards
        .entry(channel_name.to_string())
        .or_insert_with(|| {
            let (tx, _rx) = tokio::sync::broadcast::channel::<Bytes>(1024);
            tx
        })
        .subscribe()
    }

    pub fn unsubscribe(&self, channel_name: &str, receiver: Receiver<Bytes>)  {
        drop(receiver);
        if let Some(tx) = self.shards.get(channel_name) {
            // 只有当确定没有活跃的 Receiver 时才删除
            if tx.receiver_count() == 0 {
                drop(tx);
                self.shards.remove(channel_name);
            }
        }

    }

    pub fn publish(&self, channel_name: &str, data: Bytes) -> Result<usize> {
        if let Some(entry) = self.shards.get(channel_name) {
            let tx = entry.value();
            if tx.receiver_count() == 0 {
                drop(entry); // 必须先释放读锁，否则 remove 会死锁
                self.shards.remove(channel_name);
                return Ok(0);
            }
            return Ok(tx.send(data).map_err(|_| anyhow::anyhow!("Send failed"))?);
        }
        Ok(0)
    }

    pub fn get_channel_count(&self, channel_name: &str) -> usize {
        if let Some(tx) = self.shards.get(channel_name) {
            return tx.receiver_count();
        }
        0
    }


}