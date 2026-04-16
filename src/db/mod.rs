mod string;
mod expiry;

use std::collections::BTreeMap;
use bytes::Bytes;
use dashmap::DashMap;

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

pub struct Entry {
    pub(crate) value: Bytes,
    pub(crate) ttl: Option<Instant>,
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
    pub(crate) shards: Vec<DashMap<Bytes, Entry>>,
    /// 选择 BTreeMap<(Instant, Bytes), ()> 多个 key 可能有相同的过期时间
    pub(crate) expiry_indices: Vec<Mutex<BTreeMap<(Instant, Bytes), ()>>>,
    pub(crate) shard_count: usize,
    pub(crate) next_shard_index: AtomicUsize,
}

impl Db {
    pub fn new() -> Db {
        let shard_count = 2048;
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(DashMap::with_capacity(2048));
        }
        let mut expiry_indices = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            expiry_indices.push(Mutex::new(BTreeMap::new()));
        }
        Db {
            shards,
            expiry_indices,
            shard_count,
            next_shard_index: AtomicUsize::new(0),
        }
    }

    pub(crate) fn shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize & (self.shard_count - 1)
    }

    pub fn for_each_entry<F>(&self, mut f: F) -> ControlFlow<anyhow::Error>
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
