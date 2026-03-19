use bytes::Bytes;
use fast_glob::glob_match;
use std::sync::Arc;
use std::time::Instant;

use scc::HashMap as SccHashMap;

struct Entry {
    value: Bytes,
    ttl: Option<Instant>,
}

impl Entry {
    fn new(value: Bytes, ttl: Option<Instant>) -> Entry {
        Entry { value, ttl }
    }

    fn is_expired(&self) -> bool {
        self.ttl.map_or(false, |t| Instant::now() > t)
    }
}

/// 基于 scc::HashMap 的无锁并发数据库
/// 读操作完全 lock-free（基于 epoch-based reclamation）
/// 写操作使用细粒度锁，不会阻塞读
pub struct ShardedDb {
    data: SccHashMap<String, Entry>,
}

impl ShardedDb {
    pub fn new() -> Arc<Self> {
        Arc::new(ShardedDb {
            data: SccHashMap::new(),
        })
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        // read 是 lock-free 的，不会阻塞其他操作
        let result = self.data.read(key, |_, entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        });

        match result {
            Some(Some(value)) => Some(value),
            Some(None) => {
                // 过期了，清理
                let _ = self.data.remove(key);
                None
            }
            None => None, // key 不存在
        }
    }

    pub fn set(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Instant>,
        nx: bool,
        xx: bool,
    ) -> Option<()> {
        if nx {
            // NX: 只在 key 不存在时设置
            // 先检查是否存在且未过期
            let exists = self.data.read(key, |_, entry| !entry.is_expired()).unwrap_or(false);
            if exists {
                return None;
            }
            // 尝试插入，如果被其他线程抢先插入了就失败
            let entry = Entry::new(value, ttl);
            match self.data.insert(key.to_string(), entry) {
                Ok(()) => Some(()),
                Err(_) => None, // 已存在
            }
        } else if xx {
            // XX: 只在 key 存在时设置
            let updated = self.data.update(key, |_, entry| {
                if entry.is_expired() {
                    return false;
                }
                true
            });
            if updated.is_some_and(|existed| existed) {
                // key 存在且未过期，覆盖写入
                let _ = self.data.insert(key.to_string(), Entry::new(value, ttl));
                Some(())
            } else {
                None
            }
        } else {
            // 普通 SET：直接 upsert
            let entry = Entry::new(value, ttl);
            let _ = self.data.insert(key.to_string(), entry);
            Some(())
        }
    }

    pub fn del(&self, keys: Vec<String>) -> usize {
        let mut count = 0;
        for k in &keys {
            if self.data.remove(k).is_some() {
                count += 1;
            }
        }
        count
    }

    pub fn exists(&self, keys: Vec<String>) -> usize {
        let mut count = 0;
        for k in &keys {
            let exists = self.data.read(k, |_, entry| !entry.is_expired()).unwrap_or(false);
            if exists {
                count += 1;
            }
        }
        count
    }

    pub fn ttl(&self, key: &str, return_millis: bool) -> i64 {
        let result = self.data.read(key, |_, entry| {
            if let Some(ttl) = entry.ttl {
                if Instant::now() < ttl {
                    let remaining = ttl.saturating_duration_since(Instant::now());
                    if return_millis {
                        remaining.as_millis() as i64
                    } else {
                        remaining.as_secs() as i64
                    }
                } else {
                    -3 // 标记为过期，需要清理
                }
            } else {
                -1 // 存在且永不过期
            }
        });

        match result {
            Some(-3) => {
                let _ = self.data.remove(key);
                -2
            }
            Some(v) => v,
            None => -2, // 不存在
        }
    }

    pub fn expire(&self, key: &str, expire_at: Option<Instant>) -> u8 {
        match expire_at {
            None => {
                if self.data.remove(key).is_some() { 1 } else { 0 }
            }
            Some(new_ttl) => {
                let updated = self.data.update(key, |_, entry| {
                    if entry.is_expired() {
                        return false; // 过期了，视为不存在
                    }
                    entry.ttl = Some(new_ttl);
                    true
                });
                match updated {
                    Some(true) => 1,
                    Some(false) => {
                        // 过期了，清理
                        let _ = self.data.remove(key);
                        0
                    }
                    None => 0, // 不存在
                }
            }
        }
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let mut result = Vec::new();
        self.data.scan(|k, entry| {
            if !entry.is_expired() && glob_match(pattern, k) {
                result.push(k.clone());
            }
        });
        result
    }
}
