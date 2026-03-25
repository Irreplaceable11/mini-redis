use ahash::{AHasher, RandomState};
use bytes::Bytes;
use fast_glob::glob_match;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry as HashEntry;
use std::hash::{Hash, Hasher};
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

    #[inline]
    pub fn is_expired(&self) -> bool {
        self.ttl.map_or(false, |t| Instant::now() > t)
    }
}

pub struct DbOpt {
    shards: Vec<Mutex<HashMap<Bytes, Entry, RandomState>>>,
    shard_count: usize,
    next_shard_index: AtomicUsize,
}

const CLEANUP_BATCH_SIZE: usize = 256;

impl DbOpt {
    pub fn new() -> DbOpt {
        let shard_count = 2048;
        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            shards.push(Mutex::new(HashMap::with_capacity_and_hasher(
                1600,
                RandomState::new(),
            )));
        }

        DbOpt {
            shards,
            shard_count,
            next_shard_index: AtomicUsize::new(0),
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].lock();

        match shard.get(key) {
            Some(entry) if !entry.is_expired() => Some(entry.value.clone()),
            Some(_) => {
                shard.remove(key);
                None
            }
            None => None,
        }
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
        let mut shard = self.shards[idx].lock();

        if !nx && !xx {
            shard.insert(key, Entry::new(value, ttl));
            return Some(());
        }

        match shard.entry(key) {
            HashEntry::Occupied(mut occupied) => {
                let expired = occupied.get().is_expired();

                if nx && !expired {
                    return None;
                }

                if xx && expired {
                    occupied.remove();
                    return None;
                }

                occupied.insert(Entry::new(value, ttl));
                Some(())
            }
            HashEntry::Vacant(vacant) => {
                if xx {
                    return None;
                }

                vacant.insert(Entry::new(value, ttl));
                Some(())
            }
        }
    }

    pub fn del(&self, keys: Vec<Bytes>) -> usize {
        let mut count = 0;

        for key in &keys {
            let idx = self.shard_index(key);
            let mut shard = self.shards[idx].lock();

            if shard.remove(key).is_some() {
                count += 1;
            }
        }

        count
    }

    pub fn exists(&self, keys: Vec<Bytes>) -> usize {
        let mut count = 0;

        for key in &keys {
            let idx = self.shard_index(key);
            let mut shard = self.shards[idx].lock();

            match shard.get(key) {
                Some(entry) if !entry.is_expired() => {
                    count += 1;
                }
                Some(_) => {
                    shard.remove(key);
                }
                None => {}
            }
        }

        count
    }

    pub fn ttl(&self, key: &Bytes, return_millis: bool) -> i64 {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].lock();

        match shard.get(key) {
            Some(entry) => match entry.ttl {
                Some(ttl) if !entry.is_expired() => {
                    let remaining = ttl.saturating_duration_since(Instant::now());
                    if return_millis {
                        remaining.as_millis() as i64
                    } else {
                        remaining.as_secs() as i64
                    }
                }
                Some(_) => {
                    shard.remove(key);
                    -2
                }
                None => -1,
            },
            None => -2,
        }
    }

    pub fn expire(&self, key: &Bytes, expire_at: Option<Instant>) -> u8 {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].lock();

        match expire_at {
            None => {
                if shard.remove(key).is_some() {
                    1
                } else {
                    0
                }
            }
            Some(new_ttl) => match shard.get_mut(key) {
                Some(entry) if !entry.is_expired() => {
                    entry.ttl = Some(new_ttl);
                    1
                }
                Some(_) => {
                    shard.remove(key);
                    0
                }
                None => 0,
            },
        }
    }

    pub async fn keys(self: &Arc<DbOpt>, pattern: &str) -> Vec<Bytes> {
        let db = self.clone();
        let pattern = pattern.to_string();

        tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();

            for shard in &db.shards {
                let keys: Vec<Bytes> = {
                    let guard = shard.lock();
                    guard.keys().cloned().collect()
                };

                results.extend(
                    keys.into_iter().filter(|k| {
                        std::str::from_utf8(k).map_or(false, |s| glob_match(&pattern, s))
                    }),
                );
            }

            results
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
            let mut shard = self.shards[shard_idx].lock();
            shard.retain(|_, v| !v.is_expired());
        }
    }

    #[inline]
    fn shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize & (self.shard_count - 1)
    }
}
