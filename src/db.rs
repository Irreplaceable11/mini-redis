use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use bytes::Bytes;
use parking_lot::RwLock;
use tracing::info;

pub struct Entry {
    value: Bytes,
    ttl: Option<Instant>
}

impl Entry {
    pub fn new(value: Bytes, ttl: Option<Instant>) -> Entry {
        Entry { value, ttl }
    }
}

pub struct Db {
    shards: Vec<Arc<RwLock<HashMap<String, Entry>>>>,
    shard_count: usize,
    next_shard_index: AtomicUsize
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
            next_shard_index: AtomicUsize::new(0)
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

        let mut write_guard = self.shards[idx].write();

        if let Some(entry) = write_guard.get(key) {
            if let Some(ttl) = entry.ttl {
                return if instant > ttl {
                    write_guard.remove(key);
                    None
                } else {
                    Some(entry.value.clone())
                };
            }
        }

        None
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<Instant>, nx: bool, xx: bool) -> Option<()> {
        let idx = self.shard_index(&key);

        let mut guard = self.shards[idx].write();
        let existing_entry = guard.get(&key);

        let is_expired = existing_entry.map_or(true, |e| {
           e.ttl.map_or(false, |t| Instant::now() > t)
        });

        let exists = existing_entry.is_some() && !is_expired;
        if nx && exists {
            return None;
        }
        if xx && !exists {
            return None;
        }
        let entry_new = Entry::new(value, ttl);
        guard.insert(key, entry_new);
        Some(())
    }

    pub fn del(&self, keys: Vec<String>) -> usize {
        let mut del_result = 0;

        let mut keys_map: HashMap<usize, Vec<String>>  = HashMap::new();

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

        let mut keys_map: HashMap<usize, Vec<String>>  = HashMap::new();
        for key in keys {
            let i = self.shard_index(key.as_str());
            keys_map.entry(i).or_insert_with(Vec::new).push(key);
        }
        for (idx, key_vec) in keys_map {
            let guard = self.shards[idx].read();
            for k in key_vec {
                if guard.contains_key(k.as_str()) {
                    exists_result += 1;
                }
            }
        }
        exists_result
    }

    pub fn clean_up(&self) {
        // 使用基于轮询的过期key清理方式
        // 使用随机的缺点是内存不连续，有可能某个分片倒霉一直抽不到
        let instant = Instant::now();

        let start = self.next_shard_index.fetch_add(CLEANUP_BATCH_SIZE, Ordering::Relaxed);
        let total = self.shards.len();

        for i in 0..CLEANUP_BATCH_SIZE {
            let shard_idx = (start + i) % total;
            if let Some(mut guard) = self.shards[shard_idx].try_write() {
                guard.retain(|_, v| {
                    match v.ttl {
                        None => true,
                        Some(expiry) => instant < expiry,
                    }
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
}
