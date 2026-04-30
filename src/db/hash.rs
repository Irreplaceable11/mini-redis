use super::{Db, Entry, EntryValue};
use ahash::{HashMap, HashMapExt};
use anyhow::Result;
use bytes::Bytes;
use fast_glob::glob_match;

impl Db {
    pub fn hset(&self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<i64, &'static str> {
        let shard = self.shard(&key);

        let mut result: std::result::Result<i64, &'static str> = Ok(0);
        shard.entry(key)
            .and_modify(|entry| {
                if entry.is_expired() {
                    let map: HashMap<Bytes, Bytes> = fields.clone().into_iter().collect();
                    result = Ok(map.len() as i64);
                    entry.value = EntryValue::Hash(map);
                    entry.ttl = None;
                    return;
                }
                match entry.value.as_hash_mut() {
                    Ok(hash) => {
                        let mut added : i64 = 0;
                        for (k, v) in &fields {
                            let res = hash.insert(k.clone(), v.clone());
                            if res.is_none() {
                                added  += 1
                            }
                        }
                        result = Ok(added );
                    }
                    Err(e) => result = Err(e),
                }
            })
            .or_insert_with(|| {
                let map: HashMap<Bytes, Bytes> = fields.into_iter().collect();
                result = Ok(map.len() as i64);
                Entry::new(map, None)
            });
        result
    }

    pub fn hget(&self, key: &Bytes, field: &Bytes) -> Result<Option<Bytes>, &'static str> {
        let shard = self.shard(key);

        match shard.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(None);
                }
                let hash = entry.value.as_hash()?;
                Ok(hash.get(field).cloned())
            }
            None => Ok(None)
        }
    }

    pub fn hscan(&self, key: &Bytes, pattern: Option<&str>, mut cursor: u64, count: Option<u64>) -> Result<(u64, Vec<(Bytes, Bytes)>), &'static str> {
        let shard = self.shard(&key);

        let count = count.unwrap_or(10);
        let pattern = pattern.unwrap_or("*");
        let mut res = Vec::new();

        match shard.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok((0, res));
                }
                let hash = entry.value.as_hash()?;
                
                let vec = hash.iter()
                    .map(|(k, v)| (k, v))
                    .collect::<Vec<(&Bytes, &Bytes)>>();
                for (k, v) in vec.into_iter().skip(cursor as usize) {
                    cursor += 1;
                    if std::str::from_utf8(k).map_or(false, |s| glob_match(&pattern, s)) {
                        res.push((k.clone(), v.clone()))
                    }
                    if cursor == hash.len() as u64 {
                        cursor = 0;
                        break;
                    }
                    if res.len() >= count as usize {
                        break;
                    }
                }
            }
            None => {}
        }
        Ok((cursor, res))
    }


    pub fn hmget(&self, key: &Bytes, fields: Vec<&Bytes>) -> Result<Vec<Option<Bytes>>, &'static str> {
        let shard = self.shard(key);

        match shard.get(key) {
            None => Ok(vec![None; fields.len()]),
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(vec![None; fields.len()])
                }
                let hash = entry.value.as_hash()?;
                Ok(fields.into_iter().map(|f| hash.get(f).cloned()).collect())
            }
        }
    }


    pub fn hsetnx(&self, key: &Bytes, field: Bytes, value: Bytes) -> Result<i64, &'static str> {

        let shard = self.shard(key);
        let mut result: std::result::Result<i64, &'static str> = Ok(0);

        shard.entry(key.clone())
            .and_modify(|entry| {
                if entry.is_expired() {
                    let mut map = HashMap::new();
                    map.insert(field.clone(), value.clone());
                    result = Ok(1);
                    entry.value = EntryValue::Hash(map);
                    entry.ttl = None;
                    return;
                }
                match entry.value.as_hash_mut() {
                    Ok(hash) => {
                        if hash.contains_key(&field) {
                            result = Ok(0);
                        } else {
                            hash.insert(field.clone(), value.clone());
                            result = Ok(1);
                        }
                    }
                    Err(e) => result = Err(e)
                }

            })
            .or_insert_with(|| {
                let mut map = HashMap::new();
                map.insert(field, value);
                result = Ok(1);
                Entry::new(map, None)
            });

        result
    }

    pub fn hdel(&self, key: &Bytes, fields: Vec<Bytes>) -> Result<i64, &'static str> {
        let shard = self.shard(key);

        match shard.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                let hash = entry.value.as_hash_mut()?;
                let mut removed = 0i64;
                for f in &fields {
                    if hash.remove(f).is_some() {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            None => Ok(0),
        }
    }

    pub fn hexists(&self, key: &Bytes, field: &Bytes) -> Result<i64, &'static str> {
        let shard = self.shard(key);

        match shard.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                let hash = entry.value.as_hash()?;
                Ok(if hash.contains_key(field) { 1 } else { 0 })
            }
            None => Ok(0),
        }
    }

    pub fn hlen(&self, key: &Bytes) -> Result<i64, &'static str> {
        let shard = self.shard(key);

        match shard.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                let hash = entry.value.as_hash()?;
                Ok(hash.len() as i64)
            }
            None => Ok(0),
        }
    }

    pub fn hgetall(&self, key: &Bytes) -> Result<Vec<(Bytes, Bytes)>, &'static str> {
        let shard = self.shard(key);

        match shard.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(Vec::new());
                }
                let hash = entry.value.as_hash()?;
                Ok(hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            None => Ok(Vec::new()),
        }
    }

    /// HKEYS / HVALS 共用：is_keys=true 返回 field 名，false 返回 value
    pub fn hkeys_or_vals(&self, key: &Bytes, is_keys: bool) -> Result<Vec<Bytes>, &'static str> {
        let shard = self.shard(key);

        match shard.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(Vec::new());
                }
                let hash = entry.value.as_hash()?;
                if is_keys {
                    Ok(hash.keys().cloned().collect())
                } else {
                    Ok(hash.values().cloned().collect())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn hincrby(&self, key: Bytes, field: Bytes, delta: i64) -> Result<i64, &'static str> {
        let shard = self.shard(&key);
        let mut result: Result<i64, &'static str> = Ok(0);

        shard.entry(key)
            .and_modify(|entry| {
                if entry.is_expired() {
                    let mut map = HashMap::new();
                    let mut buf = itoa::Buffer::new();
                    let printed = buf.format(delta);
                    map.insert(field.clone(), Bytes::copy_from_slice(printed.as_bytes()));
                    entry.value = EntryValue::Hash(map);
                    entry.ttl = None;
                    result = Ok(delta);
                    return;
                }
                match entry.value.as_hash_mut() {
                    Ok(hash) => {
                        let old_val = match hash.get(&field) {
                            Some(v) => match atoi::atoi::<i64>(v) {
                                Some(n) => n,
                                None => { result = Err("ERR hash value is not an integer"); return; }
                            },
                            None => 0,
                        };
                        match old_val.checked_add(delta) {
                            Some(new_val) => {
                                let mut buf = itoa::Buffer::new();
                                let printed = buf.format(new_val);
                                hash.insert(field.clone(), Bytes::copy_from_slice(printed.as_bytes()));
                                result = Ok(new_val);
                            }
                            None => { result = Err("ERR increment or decrement would overflow"); }
                        }
                    }
                    Err(e) => result = Err(e),
                }
            })
            .or_insert_with(|| {
                let mut map = HashMap::new();
                let mut buf = itoa::Buffer::new();
                let printed = buf.format(delta);
                map.insert(field, Bytes::copy_from_slice(printed.as_bytes()));
                result = Ok(delta);
                Entry::new(map, None)
            });

        result
    }

    pub fn hincrbyfloat(&self, key: Bytes, field: Bytes, delta: f64) -> Result<Bytes, &'static str> {
        let shard = self.shard(&key);
        let mut result: Result<Bytes, &'static str> = Ok(Bytes::new());

        shard.entry(key)
            .and_modify(|entry| {
                if entry.is_expired() {
                    let mut map = HashMap::new();
                    let mut buffer = [b'0'; lexical_core::BUFFER_SIZE];
                    let s = lexical_core::write(delta, &mut buffer);
                    let val = Bytes::copy_from_slice(s);
                    map.insert(field.clone(), val.clone());
                    entry.value = EntryValue::Hash(map);
                    entry.ttl = None;
                    result = Ok(val);
                    return;
                }
                match entry.value.as_hash_mut() {
                    Ok(hash) => {
                        let old_val = match hash.get(&field) {
                            Some(v) => match lexical_core::parse::<f64>(v) {
                                Ok(n) => n,
                                Err(_) => { result = Err("ERR hash value is not a valid float"); return; }
                            },
                            None => 0.0,
                        };
                        let sum = old_val + delta;
                        if sum.is_nan() || sum.is_infinite() {
                            result = Err("ERR increment would produce NaN or Infinity");
                            return;
                        }
                        let mut buffer = [b'0'; lexical_core::BUFFER_SIZE];
                        let s = lexical_core::write(sum, &mut buffer);
                        let val = Bytes::copy_from_slice(s);
                        hash.insert(field.clone(), val.clone());
                        result = Ok(val);
                    }
                    Err(e) => result = Err(e),
                }
            })
            .or_insert_with(|| {
                let mut map = HashMap::new();
                let mut buffer = [b'0'; lexical_core::BUFFER_SIZE];
                let s = lexical_core::write(delta, &mut buffer);
                let val = Bytes::copy_from_slice(s);
                map.insert(field, val.clone());
                result = Ok(val);
                Entry::new(map, None)
            });

        result
    }
}