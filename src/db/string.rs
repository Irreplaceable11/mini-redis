use bytes::Bytes;
use dashmap::Entry as DashEntry;
use fast_glob::glob_match;
use rayon::prelude::*;
use std::sync::Arc;
use std::time::Instant;

use super::{Db, Entry, EntryValue};

const WRONGTYPE_ERR: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

impl Db {
    pub fn get(&self, key: &Bytes) -> Result<Option<Bytes>, &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];
        if let Some(entry) = shard.get(key) {
            if !entry.is_expired() {
                return entry.value.as_string().map(|b| Some(b.clone()));
            }
            drop(entry);
            shard.remove(key);
        }
        Ok(None)
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
        let mut guard = self.expiry_indices[idx].lock().unwrap();
        let clone_key = key.clone();
        // 无条件写入，快速路径
        if !nx && !xx {

            let old_value = shard.insert(key.clone(), Entry::new(value, ttl));

            //清理旧的索引条目
            if let Some(old_value) = old_value {
                if let Some(ttl) = old_value.ttl {
                    guard.remove(&(ttl, key.clone()));
                }
            }
            if let Some(instant) = ttl {
                guard.insert((instant, key.clone()), ());
            }

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
                    let entry = occupied.remove();
                    if let Some(ttl) = entry.ttl {
                        guard.remove(&(ttl, clone_key));
                    }
                    return None;
                }
                // NX 且已过期 → 视为不存在，允许写入
                // XX 且未过期 → key 存在，允许写入
                let old_val = occupied.insert(Entry::new(value, ttl));
                if let Some(old_ttl) = old_val.ttl {
                    guard.remove(&(old_ttl, clone_key.clone()));
                }
                if let Some(instant) = ttl {
                    guard.insert((instant, clone_key), ());
                }
                Some(())
            }
            DashEntry::Vacant(vacant) => {
                if xx {
                    // XX: key 不存在，不写入
                    return None;
                }
                // NX: key 不存在，写入
                vacant.insert(Entry::new(value, ttl));
                if let Some(instant) = ttl {
                    guard.insert((instant, clone_key), ());
                }
                Some(())
            }
        }
    }

    /// INCR / DECR / INCRBY / DECRBY 通用方法
    pub fn incr_by(&self, key: Bytes, delta: i64) -> Result<i64, &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];

        let mut result: Result<i64, &'static str> = Ok(0);
        let mut guard = self.expiry_indices[idx].lock().unwrap();

        shard.entry(key.clone())
            .and_modify(|entry| {
                if entry.is_expired() {
                    result = Ok(delta);
                    let mut buffer = itoa::Buffer::new();
                    let printed = buffer.format(delta);
                    entry.value = EntryValue::String(Bytes::copy_from_slice(printed.as_bytes()));
                    if let Some(ttl) = entry.ttl {
                        guard.remove(&(ttl, key.clone()));
                    }
                    entry.ttl = None;
                    return;
                }

                let bytes = match entry.value.as_string() {
                    Ok(b) => b,
                    Err(e) => { result = Err(e); return; }
                };

                match atoi::atoi::<i64>(bytes) {
                    Some(old_val) => match old_val.checked_add(delta) {
                        Some(new_val) => {
                            result = Ok(new_val);
                            let mut buffer = itoa::Buffer::new();
                            let printed = buffer.format(new_val);
                            entry.value = EntryValue::String(Bytes::copy_from_slice(printed.as_bytes()));
                        }
                        None => {
                            result = Err("ERR increment or decrement would overflow");
                        }
                    },
                    None => {
                        result = Err("ERR value is not an integer or out of range");
                    }
                }
            })
            .or_insert_with(|| {
                result = Ok(delta);
                let mut buffer = itoa::Buffer::new();
                let printed = buffer.format(delta);
                Entry::new(Bytes::copy_from_slice(printed.as_bytes()), None)
            });

        result
    }

    pub fn incr_by_float(&self, key: Bytes, delta: f64) -> Result<Bytes, &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];

        let mut result: Result<Bytes, &'static str> = Ok(Bytes::new());
        let mut buffer = [b'0'; lexical_core::BUFFER_SIZE];

        let mut guard = self.expiry_indices[idx].lock().unwrap();

        shard.entry(key.clone())
            .and_modify(|entry| {
                if entry.is_expired() {
                    let s = lexical_core::write(delta, &mut buffer);
                    let val = Bytes::copy_from_slice(s);
                    entry.value = EntryValue::String(val.clone());
                    if let Some(ttl) = entry.ttl {
                        guard.remove(&(ttl, key.clone()));
                    }
                    entry.ttl = None;
                    result = Ok(val);
                    return;
                }

                let bytes = match entry.value.as_string() {
                    Ok(b) => b,
                    Err(e) => { result = Err(e); return; }
                };

                match lexical_core::parse::<f64>(bytes) {
                    Ok(old_val) => {
                        let sum = old_val + delta;
                        if sum.is_nan() || sum.is_infinite() {
                            result = Err("ERR increment would produce NaN or Infinity");
                            return;
                        }
                        let s = lexical_core::write(sum, &mut buffer);
                        let val = Bytes::copy_from_slice(s);
                        entry.value = EntryValue::String(val.clone());
                        result = Ok(val);
                    }
                    Err(_) => {
                        result = Err("ERR value is not a valid float");
                    }
                }
            })
            .or_insert_with(|| {
                let s = lexical_core::write(delta, &mut buffer);
                let val = Bytes::copy_from_slice(s);
                result = Ok(val.clone());
                Entry::new(val, None)
            });

        result
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
}
