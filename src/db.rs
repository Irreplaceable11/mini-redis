use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;
use parking_lot::RwLock;
use tracing::info;

pub struct Entry {
    value: Vec<u8>,
    ttl: Option<Instant>
}

impl Entry {
    pub fn new(value: Vec<u8>, ttl: Option<Instant>) -> Entry {
        Entry { value, ttl }
    }
}

pub struct Db {
    shards: Vec<Arc<RwLock<HashMap<String, Entry>>>>,
    shard_count: usize,
}

impl Db {
    pub fn new() -> Db {
        let shard_count = 32;
        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            let map = HashMap::new();
            shards.push(Arc::new(RwLock::new(map)));
        }
        Db {
            shards,
            shard_count,
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let idx = self.shard_index(key);

        {
            let guard = self.shards[idx].read();
            if let Some(entry) = guard.get(key) {
                if let Some(ttl) = entry.ttl {
                    if Instant::now() <= ttl {
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
                return if Instant::now() > ttl {
                    write_guard.remove(key);
                    None
                } else {
                    Some(entry.value.clone())
                };
            }
        }

        None
    }

    pub fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Instant>, nx: bool, xx: bool) -> Option<()> {
        let idx = self.shard_index(key);

        let mut guard = self.shards[idx].write();
        let existing_entry = guard.get(key);

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
        guard.insert(key.to_string(), entry_new);
        Some(())
    }

    pub fn del(&self, keys: Vec<String>) -> usize {
        let mut del_result = 0;

        for key in keys {
            let key_str = key.as_str();
            let i = self.shard_index(key_str);
            let mut g = self.shards[i].write();
            if g.remove(key_str).is_some() {
                del_result += 1;
            }
        }
        del_result
    }

    pub fn clean_up(&self) {
        for (i, shard) in self.shards.iter().enumerate() {
            if let Some(mut guard) = shard.try_write() {
                guard.retain(|_, v| {
                    match v.ttl {
                        None => true,
                        Some(expiry) => Instant::now() < expiry,
                    }
                });
            } else {
                info!("sharding {} is busy", i);
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
