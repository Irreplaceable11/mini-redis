use bytes::Bytes;
use fast_glob::glob_match;
use std::collections::{hash_map, HashMap};
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
    data: HashMap<String, Entry>
}


impl Db {
    pub fn new() -> Db {
        Db {
            data: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<Bytes> {
        let instant: Instant;
        {
            if let Some(entry) = self.data.get(key) {
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

        self.remove_if_expired(key)
    }

    pub fn set(
        &mut self,
        key: &str,
        value: Bytes,
        ttl: Option<Instant>,
        nx: bool,
        xx: bool,
    ) -> Option<()> {

        let existing_entry = self.data.get(key);

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
        self.data.insert(key.to_string(), entry_new);
        Some(())
    }

    pub fn del(&mut self, keys: Vec<String>) -> usize {
        let mut del_result = 0;

        for k in keys {
            if self.data.remove(k.as_str()).is_some() {
                del_result += 1
            }
        }
        del_result
    }

    pub fn exists(&mut self, keys: Vec<String>) -> usize {
        let mut exists_result = 0;

        for k in keys {
            if self.data.get(k.as_str()).map_or(false, |v| v.ttl.map_or(true, |t| Instant::now() < t)) {
            exists_result += 1
            }
        }

        exists_result
    }

    pub fn ttl(&mut self, key: &str, return_millis: bool) -> i64 {
        let mut need_cleanup = false;
        {
            if let Some(entry) = self.data.get(key) {
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
            self.remove_if_expired(key);
        }
        // 不存在或者过期
        -2
    }

    /// 设置或移除 key 的过期时间
    /// 
    /// - expire_at: None -> 删除 key (类似 DEL)
    /// - expire_at: Some(instant) -> 设置新的过期时间 (类似 EXPIREAT)
    /// 
    /// 返回值:
    /// - 1: 操作成功 (key 存在并被更新/删除)
    /// - 0: 操作失败 (key 不存在或已过期)
    pub fn expire(&mut self, key: &str, expire_at: Option<Instant>) -> u8 {

        match expire_at {
            // 如果没有过期时间，代表可能是想删除（或者你可以根据需求改为持久化）
            None => {
                if self.data.remove(key).is_some() {
                    1
                } else {
                    0
                }
            }
            Some(new_ttl) => {
                match self.data.entry(key.to_string()) {
                    hash_map::Entry::Vacant(_) => 0,
                    hash_map::Entry::Occupied(mut entry) => {
                        let value = entry.get_mut();
                        if let Some(old_ttl) = value.ttl {
                            if Instant::now() > old_ttl {
                                entry.remove();
                                return 0; // 已经过期了，视作不存在
                            }
                        }

                        // 原地修改 TTL，不需要 clone value
                        value.ttl = Some(new_ttl);
                        1
                    }
                }
            }
        }
    }


    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.data.keys()
            .filter(|k| glob_match(pattern, k))
            .cloned()
            .collect()
    }

    pub fn clean_up(&mut self) {
        let instant = Instant::now();

        self.data.retain(|_, v| v.ttl.map_or(true, |t| instant < t));
    }


    fn remove_if_expired(&mut self, key: &str) -> Option<Bytes> {
        if let Some(entry) = self.data.get(key) {
            if let Some(ttl) = entry.ttl {
                if Instant::now() > ttl {
                    self.data.remove(key);
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
