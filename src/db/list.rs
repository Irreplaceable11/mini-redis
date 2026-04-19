use std::{cmp::max, collections::VecDeque};
use std::cmp::min;
use crate::db::{Db, Entry};
use anyhow::Result;
use bytes::Bytes;

impl Db {
    pub fn push(&self, key: Bytes, values: Vec<Bytes>, is_left: bool) -> Result<i64, &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];

        let mut result: std::result::Result<i64, &'static str> = Ok(0);
 
        shard
            .entry(key)
            .and_modify(|entry| match entry.value.as_list_mut() {
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
                Err(e) => result = Err(e),
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

    pub fn pop(&self, key: &Bytes, is_left: bool) -> Result<Option<Bytes>, &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];

        let mut result: std::result::Result<Option<Bytes>, &'static str> = Ok(None);

        match shard.get_mut(key) {
            Some(mut entry) => match entry.value.as_list_mut() {
                Ok(list) => {
                    if is_left {
                        if let Some(val) = list.pop_front() {
                            result = Ok(Some(val.clone()));
                        }
                    } else {
                        if let Some(val) = list.pop_back() {
                            result = Ok(Some(val.clone()));
                        }
                    }
                }
                Err(e) => result = Err(e),
            },
            None => {}
        }
        result
    }

    pub fn len(&self, key: &Bytes) -> Result<i64, &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];
        shard
            .get(key)
            .map(|entry| entry.value.as_list().map(|list| list.len() as i64))
            .unwrap_or(Ok(0))
    }

    pub fn range(&self, key: &Bytes, start: isize, end: isize) -> Result<Vec<Bytes>, &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];

        match shard.get(key) {
            Some(entry) => match entry.value.as_list() {
                Ok(list) => {
                    // 负数索引转换后，如果结果仍然是负数，就裁剪到 0
                    let len = list.len() as isize;
                    // 在 isize 层面做 max，然后再转 usize
                    let start_idx =
                        max(0isize, if start < 0 { start + len } else { start }) as usize;
                    let mut end_idx = max(0isize, if end < 0 { end + len } else { end }) as usize;
                   
                    if end_idx > (len - 1) as usize {
                        end_idx = (len - 1) as usize
                    }    
                    if start_idx > end_idx {
                       return Ok(Vec::new())
                    }
                   
                    
                    Ok(list
                        .range(start_idx..=end_idx)
                        .cloned()
                        .collect::<Vec<Bytes>>())
                }
                Err(e) =>  Err(e),
            },
            None => {Ok(Vec::new())}
        }
    }


    pub fn index(&self, key: &Bytes, index: isize) -> Result<Option<Bytes>, &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];

        match shard.get(key) {
            Some(entry) => {
                match entry.value.as_list() {
                    Ok(list) => {
                        let len = list.len() as isize;
                        let final_index = if index < 0 { index + len} else { index };
                        if final_index > (len - 1) || final_index < 0 {
                            return Ok(None)
                        }
                        Ok(list.get(final_index as usize).cloned())
                    }
                    Err(e) => Err(e)
                }
            }
            None => Ok(None)
        }

    }

    pub fn lset(&self, key: &Bytes, index: isize, value: Bytes) -> Result<(), &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];

        let mut result: std::result::Result<(), &'static str> = Ok(());

        match shard.get_mut(key) {
            Some(mut entry) => {
                match entry.value.as_list_mut() {
                    Ok(list) => {
                        let len = list.len() as isize;
                        let final_index = if index < 0 { index + len} else { index };
                        if final_index > (len - 1) || final_index < 0 {
                            result = Err("ERR index out of range");
                            return result;
                        }
                        list[final_index as usize] = value;
                    }
                    Err(e) => result = Err(e),
                }
            }
            None => result = Err("ERR no such key")
        }
        result
    }

    pub fn lrem(&self, key: &Bytes, count: isize, value: Bytes) -> Result<i64, &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];

        let mut result: std::result::Result<i64, &'static str> = Ok(0);
        let mut del_count = 0;
        
        match shard.get_mut(key) {
            Some(mut entry) => {
                match entry.value.as_list_mut() {
                    Ok(list) => {
                        if count < 0 {
                            //从尾部删除

                            let limit = min(list.len(), (-count) as usize);

                            if limit == 0 {
                                return Ok(0);
                            }

                            // 标记要删除的位置
                            let mut to_delete = vec![false; list.len()];
                            let mut found = 0;

                            // 从后往前找
                            for (idx, item) in list.iter().enumerate().rev() {
                                if *item == value {
                                    to_delete[idx] = true;
                                    found += 1;
                                    if found >= limit {
                                        break;
                                    }
                                }
                            }

                            // 如果一个都没找到，直接返回
                            if found == 0 {
                                return Ok(0);
                            }

                            // 重建
                            let mut new = VecDeque::with_capacity(list.len() - found);

                            for (idx, item) in list.drain(..).enumerate() {
                                if !to_delete[idx] {
                                    new.push_back(item);
                                }
                            }

                            *list = new;
                            del_count = found;
                        } else {
                            let limit = if count == 0 { usize::MAX } else { count as usize };
                            list.retain(|item| {
                                if *item == value && del_count < limit {
                                    del_count += 1;
                                    false
                                } else {
                                    true
                                }
                            });
                        }
                        result = Ok(del_count as i64);
                    }
                    Err(e) => result = Err(e.into()),
                }
            }
            None => {}
        }
        result
    }
    //TODO LINSERT key BEFORE|AFTER pivot value
}
