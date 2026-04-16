use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::info;

use super::Db;

const CLEANUP_BATCH_SIZE: usize = 256;

impl Db {
    pub fn del(&self, keys: Vec<Bytes>) -> usize {
        let mut count = 0;
        for key in &keys {
            let idx = self.shard_index(key);
            match self.shards[idx].remove(key) {
                Some(entry) => {
                    count += 1;
                    let key = entry.0;
                    let val = entry.1;
                    if let Some(instant) = val.ttl {
                        self.expiry_indices[idx].lock().unwrap().remove(&(instant, key));
                    }
                }
                None => {}
            }
        }
        count
    }

    pub fn exists(&self, keys: Vec<Bytes>) -> usize {
        let mut count = 0;
        for key in &keys {
            let idx = self.shard_index(key);
            if let Some(entry) = self.shards[idx].get(key) {
                if !entry.is_expired() {
                    count += 1;
                }
            }
        }
        count
    }

    pub fn ttl(&self, key: &Bytes, return_millis: bool) -> i64 {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];
        if let Some(entry) = shard.get(key) {
            return if let Some(ttl) = entry.ttl {
                if !entry.is_expired() {
                    let remaining = ttl.saturating_duration_since(Instant::now());
                    return if return_millis {
                        remaining.as_millis() as i64
                    } else {
                        remaining.as_secs() as i64
                    };
                }
                drop(entry);
                shard.remove(key);
                -2
            } else {
                -1
            }
        }
        -2
    }

    pub fn expire(&self, key: &Bytes, expire_at: Option<Instant>) -> u8 {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];
        // 没有expire_at代表删除
        let mut guard = self.expiry_indices[idx].lock().unwrap();
        match expire_at {
            None => {
                match shard.remove(key) {
                    Some(entry) => {
                        let key = entry.0;
                        let val = entry.1;
                        if let Some(instant) = val.ttl {
                            guard.remove(&(instant, key));
                        }
                        1
                    }
                    None => 0
                }
            }
            Some(new_ttl) => {
                if let Some(mut entry) = shard.get_mut(key) {
                    if entry.is_expired() {
                        drop(entry);
                        if let Some(entry) = shard.remove(key) {
                            let key = entry.0;
                            let val = entry.1;
                            if let Some(instant) = val.ttl {
                                guard.remove(&(instant, key));
                            }
                        }
                        return 0;
                    }
                    if let Some(instant) = entry.ttl {
                        guard.remove(&(instant, key.clone()));
                    }
                    entry.ttl = Some(new_ttl);
                    guard.insert((new_ttl, key.clone()), ());
                    1
                } else {
                    0
                }
            }
        }
    }

    pub fn clean_up(&self) {
        let start = self
            .next_shard_index
            .fetch_add(CLEANUP_BATCH_SIZE, Ordering::Relaxed);
        let total = self.shards.len();
        let now = Instant::now();
        let mut total_cleaned = 0usize;
        for i in 0..CLEANUP_BATCH_SIZE {
            let shard_idx = (start + i) % total;
            let mut guard = self.expiry_indices[shard_idx].lock().unwrap();
            // split_off 返回 >= 边界的部分，留下 < 边界的部分
            let remaining = guard.split_off(&(now, Bytes::new()));
            // guard 里现在是所有 < now 的条目（即过期的）
            let expired = std::mem::replace(&mut *guard, remaining);
            drop(guard);

            total_cleaned += expired.len();
            for ((_, key), _) in expired {
                self.shards[shard_idx].remove(&key);
            }
        }
        let shard_start = start % total;
        let shard_end = (start + CLEANUP_BATCH_SIZE - 1) % total;
        info!(cleaned = total_cleaned, shards = %format!("{}-{}", shard_start, shard_end), "过期 key 清理完成");
    }
}
