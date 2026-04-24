use super::{Db, Entry};
use ahash::{HashMap};
use anyhow::Result;
use bytes::Bytes;

impl Db {
    pub fn hset(&self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<i64, &'static str> {
        let shard = self.shard(&key);

        let mut result: std::result::Result<i64, &'static str> = Ok(0);
        shard.entry(key)
            .and_modify(|entry| {
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
}