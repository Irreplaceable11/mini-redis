use bytes::Bytes;
use dashmap::DashMap;
use fast_glob::glob_match;
use rayon::prelude::*;

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub struct Entry {
    value: Bytes,
    ttl: Option<Instant>,
}

impl Entry {
    pub fn new(value: Bytes, ttl: Option<Instant>) -> Entry {
        Entry { value, ttl }
    }

    /// 判断是否已过期：没有设置 ttl 的永不过期
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.ttl.map_or(false, |t| Instant::now() > t)
    }
}

pub struct Db {
    shards: Vec<DashMap<Bytes, Entry>>,
    shard_count: usize,
    next_shard_index: AtomicUsize,
}

const CLEANUP_BATCH_SIZE: usize = 64;

impl Db {
    pub fn new() -> Db {
        let shard_count = 1024;
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(DashMap::new());
        }
        Db {
            shards,
            shard_count,
            next_shard_index: AtomicUsize::new(0),
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];
        if let Some(entry) = shard.get(key) {
            if !entry.is_expired() {
                return Some(entry.value.clone());
            }
            drop(entry);
            shard.remove(key);
        }
        None
    }

    pub fn set(
        &self,
        key: Bytes,
        value: Bytes,
        ttl: Option<Instant>,
        nx: bool,
        xx: bool,
    ) -> Option<()> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];
        if !nx && !xx {
            shard.insert(key, Entry::new(value, ttl));
            return Some(());
        }
       
        // NX/XX 需要先检查是否存在
        let existing = shard.get(&key);
        let exists = existing.as_ref().map_or(false, |e| !e.is_expired());
        drop(existing);

        if nx && exists {
            return None;
        }
        if xx && !exists {
            return None;
        }

        shard.insert(key, Entry::new(value, ttl));
        Some(())
    }

    pub fn del(&self, keys: Vec<Bytes>) -> usize {
        let mut count = 0;
        for key in &keys {
            let idx = self.shard_index(key);
            if self.shards[idx].remove(key).is_some() {
                count += 1;
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
            if let Some(ttl) = entry.ttl {
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
                return -2;
            } else {
                return -1;
            }
        }
        -2
    }

    pub fn expire(&self, key: &Bytes, expire_at: Option<Instant>) -> u8 {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];

        match expire_at {
            None => {
                if shard.remove(key).is_some() { 1 } else { 0 }
            }
            Some(new_ttl) => {
                if let Some(mut entry) = shard.get_mut(key) {
                    if entry.is_expired() {
                        drop(entry);
                        shard.remove(key);
                        return 0;
                    }
                    entry.ttl = Some(new_ttl);
                    1
                } else {
                    0
                }
            }
        }
    }

    pub async fn keys(self: &Arc<Db>, pattern: &str) -> Vec<Bytes> {
        let db = self.clone();
        let pattern = pattern.to_string();

        tokio::task::spawn_blocking(move || {
            db.shards
                .par_iter()
                .flat_map(|shard| {
                    let key_list: Vec<Bytes> = shard.iter().map(|r| r.key().clone()).collect();
                    key_list
                        .into_iter()
                        .filter(|k| {
                            std::str::from_utf8(k).map_or(false, |s| glob_match(&pattern, s))
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .await
        .unwrap_or_default()
    }

    pub fn clean_up(&self) {
        let start = self
            .next_shard_index
            .fetch_add(CLEANUP_BATCH_SIZE, Ordering::Relaxed);
        let total = self.shards.len();

        for i in 0..CLEANUP_BATCH_SIZE {
            let shard_idx = (start + i) % total;
            self.shards[shard_idx].retain(|_, v| !v.is_expired());
        }
    }

    fn shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize & (self.shard_count - 1)
    }
}
