use std::collections::VecDeque;

use crate::db::{Db, Entry};
use anyhow::Result;
use bytes::Bytes;


impl Db {
    pub fn push(&self, key: Bytes, values: Vec<Bytes>, is_left: bool) -> Result<i64, &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];

        let mut result: std::result::Result<i64, &'static str> = Ok(0);

        shard.entry(key.clone())
            .and_modify(|entry| {
                match entry.value.as_list_mut() {
                    Ok(list) => {
                        for ele in &values {
                            if is_left {
                                list.push_front(ele.clone());
                            } else {
                                list.push_back(ele.clone());
                            }

                        }
                        result = Ok(list.len() as i64);
                    }
                    Err(e) => {result = Err(e)}
                    
                }
            })
            .or_insert_with(|| {
                let mut new_vec_deque = VecDeque::new();
                 for ele in values {
                     if is_left {
                         new_vec_deque.push_front(ele);
                     } else {
                         new_vec_deque.push_back(ele);
                     }
                }
                result = Ok(new_vec_deque.len() as i64);
                Entry::new(new_vec_deque, None)
            });
        result    
    }
}