use bytes::Bytes;
use fast_glob::glob_match;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
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
}

pub struct Db {
    shards: Vec<Arc<RwLock<HashMap<Arc<str>, Entry>>>>,
    shard_count: usize,
    next_shard_index: AtomicUsize,
}
// 清理分片的数量
const CLEANUP_BATCH_SIZE: usize = 16;

impl Db {
    pub fn new() -> Db {
        let shard_count = 256;
        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            let map = HashMap::new();
            shards.push(Arc::new(RwLock::new(map)));
        }
        Db {
            shards,
            shard_count,
            next_shard_index: AtomicUsize::new(0),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let instant: Instant;
        {
            let guard = self.shards[idx].read();
            if let Some(entry) = guard.get(key) {
                if let Some(ttl) = entry.ttl {
                    instant = Instant::now();
                    if instant <= ttl {
                        return Some(entry.value.clone());
                    }
                } else {
                    return Some(entry.value.clone());
                }
            } else {
                return None;
            }
        }

        self.double_check_remove(key, idx)
    }

    pub fn set(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Instant>,
        nx: bool,
        xx: bool,
    ) -> Option<()> {
        let idx = self.shard_index(&key);

        let mut guard = self.shards[idx].write();
        let existing_entry = guard.get(key);

        let is_expired =
            existing_entry.map_or(true, |e| e.ttl.map_or(false, |t| Instant::now() > t));

        let exists = existing_entry.is_some() && !is_expired;
        if nx && exists {
            return None;
        }
        if xx && !exists {
            return None;
        }
        let entry_new = Entry::new(value, ttl);
        guard.insert(Arc::from(key), entry_new);
        Some(())
    }

    pub fn del(&self, keys: Vec<String>) -> usize {
        let mut del_result = 0;

        let mut keys_map: HashMap<usize, Vec<String>> = HashMap::new();

        for key in keys {
            let i = self.shard_index(key.as_str());
            keys_map.entry(i).or_insert_with(Vec::new).push(key);
        }

        for (idx, key_vec) in keys_map {
            let mut guard = self.shards[idx].write();
            for k in key_vec {
                if guard.remove(k.as_str()).is_some() {
                    del_result += 1;
                }
            }
        }
        del_result
    }

    pub fn exists(&self, keys: Vec<String>) -> usize {
        let mut exists_result = 0;

        let mut keys_map: HashMap<usize, Vec<String>> = HashMap::new();
        for key in keys {
            let i = self.shard_index(key.as_str());
            keys_map.entry(i).or_insert_with(Vec::new).push(key);
        }
        for (idx, key_vec) in keys_map {
            let guard = self.shards[idx].read();
            for k in key_vec {
                if guard
                    .get(k.as_str())
                    .map_or(false, |v| v.ttl.map_or(true, |t| Instant::now() < t))
                {
                    exists_result += 1;
                }
            }
        }
        exists_result
    }

    pub fn ttl(&self, key: &str, return_millis: bool) -> i64 {
        let idx = self.shard_index(key);
        let mut need_cleanup = false;
        {
            let guard = self.shards[idx].read();
            if let Some(entry) = guard.get(key) {
                if let Some(ttl) = entry.ttl {
                    if Instant::now() < ttl {
                        //存在未过期
                        let remaining = ttl.saturating_duration_since(Instant::now());
                        return if return_millis {
                            remaining.as_millis() as i64
                        } else {
                            remaining.as_secs() as i64
                        };
                    }
                    need_cleanup = true;
                } else {
                    //存在并永不过期
                    return -1;
                }
            }
        }
        if need_cleanup {
            self.double_check_remove(key, idx);
        }
        // 不存在或者过期
        -2
    }

    pub fn expire(&self, key: &str, expire_at: Option<Instant>) -> u8 {
        let idx = self.shard_index(key);
        let mut guard = self.shards[idx].write();

        match expire_at {
            // 如果没有过期时间，代表可能是想删除（或者你可以根据需求改为持久化）
            None => {
                if guard.remove(key).is_some() {
                    1
                } else {
                    0
                }
            }
            Some(new_ttl) => {
                if let Some(entry) = guard.get_mut(key) {
                    // 检查当前是否已经过期（惰性删除逻辑）
                    if let Some(old_ttl) = entry.ttl {
                        if Instant::now() > old_ttl {
                            guard.remove(key);
                            return 0; // 已经过期了，视作不存在
                        }
                    }

                    // 原地修改 TTL，不需要 clone value
                    entry.ttl = Some(new_ttl);
                    1
                } else {
                    0 // Key 不存在
                }
            }
        }
    }

    pub fn keys(&self, key: &str) -> Vec<Arc<str>> {
        self.shards
            .par_iter()
            .flat_map(|shard| {
                let key_list: Vec<_> = { 
                    shard.read().keys().cloned().collect() 
                };
                // 先收集key引用在做匹配，减少锁持有时间

                key_list
                    .iter()
                    .filter(|k| glob_match(key, k.as_ref()))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn clean_up(&self) {
        // 使用基于轮询的过期key清理方式
        // 使用随机的缺点是内存不连续，有可能某个分片倒霉一直抽不到
        let instant = Instant::now();

        let start = self
            .next_shard_index
            .fetch_add(CLEANUP_BATCH_SIZE, Ordering::Relaxed);
        let total = self.shards.len();

        for i in 0..CLEANUP_BATCH_SIZE {
            let shard_idx = (start + i) % total;
            if let Some(mut guard) = self.shards[shard_idx].try_write() {
                guard.retain(|_, v| match v.ttl {
                    None => true,
                    Some(expiry) => instant < expiry,
                });
            }
        }
    }

    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();

        key.hash(&mut hasher);

        let hash = hasher.finish();
        hash as usize & (self.shard_count - 1)
    }

    fn double_check_remove(&self, key: &str, idx: usize) -> Option<Bytes> {
        let mut guard = self.shards[idx].write();
        if let Some(entry) = guard.get(key) {
            if let Some(ttl) = entry.ttl {
                if Instant::now() > ttl {
                    guard.remove(key);
                    None
                } else {
                    Some(entry.value.clone())
                }
            } else {
                Some(entry.value.clone())
            }
        } else {
            None
        }
    }
}
