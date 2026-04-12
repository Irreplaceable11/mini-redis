use bytes::Bytes;
use dashmap::DashMap;
use dashmap::Entry as DashEntry;
use fast_glob::glob_match;
use rayon::prelude::*;

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

const CLEANUP_BATCH_SIZE: usize = 256;

impl Db {
    pub fn new() -> Db {
        let shard_count = 2048;
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(DashMap::with_capacity(2048));
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

        // 无条件写入，快速路径
        if !nx && !xx {
            shard.insert(key, Entry::new(value, ttl));
            return Some(());
        }

        // 使用 entry API 保证原子性，避免 get→check→drop→insert 的 TOCTOU 竞态
        match shard.entry(key) {
            DashEntry::Occupied(mut occupied) => {
                let exists = !occupied.get().is_expired();
                if nx && exists {
                    // NX: key 存在且未过期，不写入
                    return None;
                }
                if xx && !exists {
                    // XX: key 已过期（等同于不存在），不写入，顺便清理
                    occupied.remove();
                    return None;
                }
                // NX 且已过期 → 视为不存在，允许写入
                // XX 且未过期 → key 存在，允许写入
                occupied.insert(Entry::new(value, ttl));
                Some(())
            }
            DashEntry::Vacant(vacant) => {
                if xx {
                    // XX: key 不存在，不写入
                    return None;
                }
                // NX: key 不存在，写入
                vacant.insert(Entry::new(value, ttl));
                Some(())
            }
        }
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
                if shard.remove(key).is_some() {
                    1
                } else {
                    0
                }
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

    pub fn for_each_entry<F>(&self,mut f: F) -> ControlFlow<anyhow::Error>
    where
        F: FnMut(&Bytes, &Bytes, Option<Instant>, bool) -> ControlFlow<anyhow::Error>,
    {
        for shard in self.shards.iter() {
            for ele in shard.iter() {
                let flow = f(ele.key(), &ele.value().value, ele.value().ttl, ele.value().is_expired());
                if flow.is_break() {
                    return flow;
                }
            }
        }
        ControlFlow::Continue(())
    }
}
