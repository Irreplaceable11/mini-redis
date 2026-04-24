mod string;
mod expiry;
mod list;
mod hash;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::oneshot;
use std::collections::{BTreeMap, VecDeque};

use ahash::{HashMap, RandomState};
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::Mutex;
use std::time::Instant;

pub struct Entry {
    pub(crate) value: EntryValue,
    pub(crate) ttl: Option<Instant>,
}


impl EntryValue {
    /// 尝试获取 String 类型的引用
    pub fn as_string(&self) -> Result<&Bytes, &'static str> {
        match self {
            EntryValue::String(b) => Ok(b),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    /// 尝试获取 List 类型的引用
    pub fn as_list(&self) -> Result<&VecDeque<Bytes>, &'static str> {
        match self {
            EntryValue::List(l) => Ok(l),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    /// 尝试获取 List 类型的可变引用
    pub fn as_list_mut(&mut self) -> Result<&mut VecDeque<Bytes>, &'static str> {
        match self {
            EntryValue::List(l) => Ok(l),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }
    
    pub fn as_hash(&self) -> Result<&HashMap<Bytes, Bytes>, &'static str> {
        match self {
            EntryValue::Hash(l) => Ok(l),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }
    pub fn as_hash_mut(&mut self) -> Result<&mut HashMap<Bytes, Bytes>, &'static str> {
        match self {
            EntryValue::Hash(l) => Ok(l),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }
    
}

#[derive(Clone)]
pub enum EntryValue {
    String(Bytes),

    List(VecDeque<Bytes>),

    Hash(HashMap<Bytes, Bytes>)
}


impl From<Bytes> for EntryValue {
    fn from(value: Bytes) -> Self {
       EntryValue::String(value)
    }
}

impl From<VecDeque<Bytes>> for EntryValue {
    fn from(value: VecDeque<Bytes>) -> Self {
        EntryValue::List(value)
    }
}

impl From<HashMap<Bytes, Bytes>> for EntryValue {
    fn from(value: HashMap<Bytes, Bytes>) -> Self {
        EntryValue::Hash(value)
    }
}

impl Entry {
    pub fn new<T>(value: T, ttl: Option<Instant>) -> Entry
    where T: Into<EntryValue> {
        Entry { value: value.into(), ttl }
    }

    /// 判断是否已过期：没有设置 ttl 的永不过期
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.ttl.map_or(false, |t| Instant::now() > t)
    }
}

pub struct Db {
    hash_builder: RandomState,
    pub(crate) shards: Vec<DashMap<Bytes, Entry>>,
    /// 选择 BTreeMap<(Instant, Bytes), ()> 多个 key 可能有相同的过期时间
    pub(crate) expiry_indices: Vec<Mutex<BTreeMap<(Instant, Bytes), ()>>>,
    pub(crate) shard_count: usize,
    pub(crate) waiters: DashMap<Bytes, VecDeque<oneshot::Sender<(Bytes, Bytes)>>>
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
            hash_builder: RandomState::new(),
            shards,
            expiry_indices,
            shard_count,
            waiters: DashMap::with_capacity(2048)
        }
    }

    pub(crate) fn shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize & (self.shard_count - 1)
    }

    /// 根据 key 获取对应的分片引用
    #[inline]
    pub(crate) fn shard(&self, key: &Bytes) -> &DashMap<Bytes, Entry> {
        &self.shards[self.shard_index(key)]
    }

    pub fn for_each_entry<F>(&self, mut f: F) -> ControlFlow<anyhow::Error>
    where
        F: FnMut(&Bytes, &EntryValue, Option<Instant>, bool) -> ControlFlow<anyhow::Error>,
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
