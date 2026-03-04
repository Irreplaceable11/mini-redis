use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::info;

pub struct Entry {
    value: Vec<u8>,
    // 选择Instant是因为更好计算过期时间
    ttl: Option<Instant>
}

impl Entry {
    pub fn new(value: Vec<u8>, ttl: Option<Instant>) -> Entry {
        Entry { value, ttl }
    }
}

pub struct Db {
    shards: Vec<Arc<RwLock<HashMap<String, Entry>>>>,
    // 2的幂次
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

        let guard = self.shards[idx].read().expect("read lock poisoned");
        let key_string = key.to_string();

        let entry = guard.get(&key_string)?;// 如果不存在直接返回 None

        let is_expire = match entry.ttl {
            Some(ttl) => Instant::now().gt(&ttl),
            // 代表永不过期
            None => false
        };
        if !is_expire {
            return Some(entry.value.clone());
        }
        //如果过期 剔除
        drop(guard);
        let mut write_guard = self.shards[idx].write().expect("write lock poisoned");
        let entry = match write_guard.get(&key_string) {
            Some(entry) => entry,
            None => return None
        };
        match entry.ttl {
            Some(ttl) => {
                // 过期返回 None
                if Instant::now() > ttl {
                    write_guard.remove(&key_string);
                    None
                } else {
                    //未过期返回值
                    Some(entry.value.clone())
                }
            },
            // 永不过期 返回值
            None => Some(entry.value.clone())
        }

    }

    pub fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Instant>, nx: bool, xx: bool) -> Option<()> {
        let idx = self.shard_index(key);
        let key_string = key.to_string();

        //如果nx xx有一个为true 先获取写锁 判断key是否存在

        let key_exist = {
            if nx || xx {
                let guard = self.shards[idx].read().expect("read lock poisoned");
                match guard.get(&key_string) {
                    Some(entry) => {
                        if let Some(expiry) = entry.ttl {
                            if Instant::now() > expiry {
                                false //过期
                            } else {
                                true //未过期
                            }
                        } else {
                            true //永不过期
                        }
                    },
                    None => false //key 不存在
                }
            } else {
                false //nx xx 都为false 不重要
            }
        };
        // 如果nx 为true key已存在
        let mut guard = self.shards[idx].write().expect("write lock poisoned");
        let entry_new = Entry::new(value, ttl);
        // 只有当 Key 不存在时，才执行设置操作
        if nx && key_exist{
            return None;
        }
        // 只有当 Key 已存在时，才执行设置操作
        if xx && !key_exist {
            return None;
        }

        guard.insert(key_string, entry_new);
        Some(())

    }

    pub fn clean_up(&self) {
        for (i, shard) in self.shards.iter().enumerate() {
            match shard.try_write() {
                Ok(mut guard) => {
                    guard.retain(|_, v| {
                        match v.ttl {
                            None => return true,
                            // 保留未过期
                            Some(expiry) => Instant::now() < expiry,
                        }
                    });
                }
                Err(_) => {
                    info!("sharding {} is busy", i);
                }
            }
        }
    }

    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();

        key.hash(&mut hasher);

        let hash = hasher.finish();
        hash as usize & (self.shard_count -1)
    }


}