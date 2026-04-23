use crate::db::{Db, Entry};
use anyhow::Result;
use bytes::Bytes;
use futures::future::select_all;
use std::cmp::min;
use std::time::Duration;
use std::{cmp::max, collections::VecDeque};
use tokio::sync::oneshot;

impl Db {
    pub fn push(&self, key: Bytes, values: Vec<Bytes>, is_left: bool) -> Result<i64, &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];

        let mut result: std::result::Result<i64, &'static str> = Ok(0);
 
        shard
            .entry(key.clone())
            .and_modify(|entry| match entry.value.as_list_mut() {
                Ok(list) => {
                    for ele in &values {
                        if self.try_notify_waiter(&key, ele) {
                            continue;
                        }
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
                    if self.try_notify_waiter(&key, &ele) {
                            continue;
                    }
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

    pub fn pushx(&self, key: &Bytes, values: Vec<Bytes>, is_left: bool) -> Result<i64, &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];
        match shard.get_mut(key) {
            Some(mut entry) => {
                entry.value.as_list_mut()
                    .map(|list| {
                        for val in values {
                            if is_left {
                                list.push_front(val);
                            } else {
                                list.push_back(val);
                            }
                        }
                        list.len() as i64
                    })
            }
            None => Ok(0)
        }
    }

    /// LPOS 命令的实现：在列表中查找元素的索引位置
    ///
    /// # 参数
    /// - `key`: 列表的 key
    /// - `value`: 要查找的目标元素
    /// - `rank`: 指定返回第几个匹配项，正数从左往右，负数从右往左，默认为 1。不能为 0
    /// - `count`: 返回匹配项的数量，0 表示返回全部匹配，默认为 1
    /// - `max_len`: 最多扫描的元素数量，0 表示不限制，默认为 0
    ///
    /// # 返回值
    /// - key 不存在时返回空 Vec
    /// - 命令层根据调用时是否传入了 count 来决定返回 Integer/Nil 还是 Array
    pub fn pos(&self, key: &Bytes, value: Bytes, rank: Option<i64>, count: Option<u64>, max_len: Option<u64>) -> Result<Vec<i64>, &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];
        
        match shard.get(key) {
            Some(entry) => {
                match entry.value.as_list() {
                    Ok(list) => {
                        let mut vec = Vec::new();
                        let rank = rank.unwrap_or(1);
                        let max_len = max_len.unwrap_or(0);
                        let actual_count = count.unwrap_or(1);
                        if rank ==  0 {
                            return Err("RANK can't be zero");
                        }
                        let mut matched_count = 0;
                        let mut scanned = 0;
                        if rank > 0 {
                            for (idx,ele) in list.iter().enumerate() {
                                if max_len > 0 && scanned >= max_len {
                                    break;
                                }
                                if *ele == value {
                                    matched_count += 1;
                                    if matched_count >= rank {
                                        vec.push(idx as i64);
                                        if actual_count > 0 && actual_count >= vec.len() as u64 {
                                            break;
                                        }
                                    }
                                }
                                scanned += 1
                            }
                        } else {
                            for (idx,ele) in list.iter().rev().enumerate() {
                                if max_len > 0 && scanned >= max_len {
                                    break;
                                }
                                if *ele == value {
                                    matched_count += 1;
                                    if matched_count >= rank.unsigned_abs() as i64 {
                                        vec.push((list.len() - 1 - idx) as i64);
                                        if actual_count > 0 && actual_count >= vec.len() as u64 {
                                            break;
                                        }
                                    }
                                }
                                scanned += 1
                            }
                        }
                        Ok(vec)
                    }
                    Err(e) => Err(e),
                }
            }
            None => {
                let v: Vec<i64> = Vec::new();
                Ok(v)
            }
        }
    }

    pub fn trim(&self, key: &Bytes, start: i64, stop: i64) -> Result<(), &'static str> {
        let idx = self.shard_index(&key);
        let shard = &self.shards[idx];

        match shard.get_mut(key) {
            Some(mut entry) => {
                match entry.value.as_list_mut() {
                    Ok(list) => {
                        if list.is_empty() {
                            return Ok(());
                        }
                        let len = list.len() as i64;
                        let start_idx =
                            max(0, if start < 0 { start + len } else { start }) as usize;
                        let end_idx = max(0, if stop < 0 { stop + len } else { stop }) as usize;
                        if start_idx > (len - 1) as usize || start_idx > end_idx {
                            list.clear();
                            return Ok(())
                        }
                        for _ in 0..start_idx {
                            list.pop_front();
                        }

                        let keep = end_idx - start_idx + 1;

                        while list.len() > keep {
                            list.pop_back();
                        }
                        Ok(())
                    }
                    Err(e) => Err(e)
                }
            }
            None => Ok(())
        }
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

    pub fn linsert(&self, key: &Bytes, is_before: bool, pivot: &Bytes, value: Bytes) -> Result<i64, &'static str> {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];
        
        match shard.get_mut(key) {
            Some(mut entry) => {
                match entry.value.as_list_mut(){
                    Ok(list) => {
                        if let Some(index) = list.iter().position(|item| item == pivot) {
                            if is_before {
                                list.insert(index, value);
                            } else {
                                list.insert(index + 1, value);
                            }
                           Ok(list.len() as i64)
                        } else {
                            //pivot 不存在 -1
                           Ok(-1)
                        }
                    }
                    Err(e) =>  Err(e),
                }

            }
            None =>  Ok(0)
        }
    }

    pub async fn bpop(&self, keys: Vec<Bytes>, timeout: u64, is_left: bool) -> Result<Vec<Bytes>, &'static str> {

        for key in &keys {
            let idx = self.shard_index(key);
            let shard = &self.shards[idx];
            match shard.get_mut(key) {
                Some(mut entry) => {
                    match entry.value.as_list_mut() {
                        Ok(list) => {
                            let pop_val = if is_left {list.pop_front()} else {list.pop_back()};
                            if pop_val.is_some() {
                                return Ok(vec![key.clone(), pop_val.unwrap_or(Bytes::new())])
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                None => {}
            }
        }
        // 为每个 key 注册 waiter，收集成 future 列表
        let futures: Vec<_> = keys.iter().map(|key| {
            self.register_waiter(key)
        }).collect();

        if futures.is_empty() {
            return Ok(vec![]);
        }
 
        // 所有 key 同时等待，整体共享一个 timeout
        match tokio::time::timeout(Duration::from_secs(timeout), select_all(futures)).await {
            Ok((Ok((key, value)), _index, _remaining)) => {
                // 某个 key 有数据了，返回 [key, value]
                Ok(vec![key, value])
            }
            Ok((Err(_), _, _)) => {
                // sender 被 drop 了，没拿到数据
                Ok(vec![])
            }
            Err(_) => {
                // 超时，所有 key 都没数据
                Ok(vec![])
            }
        }
    }

    pub fn lmove(&self, source: &Bytes, destination: &Bytes, source_left: bool, dest_left: bool) -> Result<Option<Bytes>, &'static str> {

        let src_outer = self.shard_index(source);
        let dst_outer = self.shard_index(destination);
        let src_map = &self.shards[src_outer];
        let dst_map = &self.shards[dst_outer];

        let mut result: std::result::Result<Option<Bytes>, &'static str> = Ok(None);

        if source == destination {
            // 情况1：同 key，get_mut 一次搞定
            match src_map.get_mut(source) {
                None => return Ok(None),
                Some(mut entry) => {
                    let list = entry.value.as_list_mut()?;
                    let val = if source_left { list.pop_front() } else { list.pop_back() };
                    if let Some(v) = &val {
                        if dest_left { list.push_front(v.clone()); } else { list.push_back(v.clone()); }
                    }
                    return Ok(val)
                }
            }
        } else if src_outer == dst_outer {
            // 情况2：同外层 shard，同一个 DashMap
            let src_inner = src_map.determine_map(source);
            let dst_inner = src_map.determine_map(destination);
            let shards = src_map.shards();

            if src_inner == dst_inner {
                // 一把写锁
                let mut shard = shards[src_inner].write();
                let src_hash = src_map.hash_usize(&source);
                let dst_hash = src_map.hash_usize(&destination);

                // 先从 source pop
                let popped = shard
                    .get_mut(src_hash as u64, |(k, _)| k == source)
                    .and_then(|(_, v)| {
                        match v.get_mut().value.as_list_mut() {
                            Ok(list) => {
                                if source_left { list.pop_front() } else { list.pop_back() }
                            }
                            Err(e) => {
                                result = Err(e);
                                None
                            }
                        }
                    });

                if result.is_err() {
                    return result;
                }

                let Some(value) = popped else {
                    return Ok(None);
                };

                // 再 push 到 destination
                match shard.get_mut(dst_hash as u64, |(k, _)| k == destination) {
                    Some((_, v)) => {
                        match v.get_mut().value.as_list_mut() {
                            Ok(list) => {
                                if dest_left { list.push_front(value.clone()); } else { list.push_back(value.clone()); }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    None => {
                        // destination 不存在，创建新列表
                        let mut deque = VecDeque::new();
                        if dest_left { deque.push_front(value.clone()); } else { deque.push_back(value.clone()); }
                        let entry = Entry::new(deque, None);
                        let hash = dst_hash as u64;
                        shard.insert(hash, (destination.clone(), dashmap::SharedValue::new(entry)), |(k, _)| src_map.hash_usize(k) as u64);
                    }
                }
                return Ok(Some(value));
            } else {
                let (first_idx, second_idx) = if src_inner < dst_inner {
                    (src_inner, dst_inner)
                } else {
                    (dst_inner, src_inner)
                };
                let mut first_lock = shards[first_idx].write();
                let mut second_lock = shards[second_idx].write();

                // 根据 src_inner 和 dst_inner 的大小关系，确定哪个 lock 对应 source
                let (src_shard, dst_shard) = if src_inner < dst_inner {
                    (&mut first_lock, &mut second_lock)
                } else {
                    (&mut second_lock, &mut first_lock)
                };
                let src_hash = src_map.hash_usize(&source);
                let dst_hash = src_map.hash_usize(&destination);


                let popped = src_shard
                    .get_mut(src_hash as u64, |(k, _)| k == source)
                    .and_then(|(_, v)| {
                        match v.get_mut().value.as_list_mut() {
                            Ok(list) => {
                                if source_left { list.pop_front() } else { list.pop_back() }
                            }
                            Err(e) => {
                                result = Err(e);
                                None
                            }
                        }
                    });

                if result.is_err() {
                    return result;
                }

                let Some(value) = popped else {
                    return Ok(None);
                };

                match dst_shard.get_mut(dst_hash as u64, |(k, _)| k == destination) {
                    Some((_, v)) => {
                        match v.get_mut().value.as_list_mut() {
                            Ok(list) => {
                                if dest_left { list.push_front(value.clone()); } else { list.push_back(value.clone()); }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    None => {
                        // destination 不存在，创建新列表
                        let mut deque = VecDeque::new();
                        if dest_left { deque.push_front(value.clone()); } else { deque.push_back(value.clone()); }
                        let entry = Entry::new(deque, None);
                        let hash = dst_hash as u64;
                        dst_shard.insert(hash, (destination.clone(), dashmap::SharedValue::new(entry)), |(k, _)| src_map.hash_usize(k) as u64);
                    }
                }
                return Ok(Some(value));
            }
        } else {
            // 情况3：不同外层 shard，按顺序拿锁
            let src_inner = src_map.determine_map(source);
            let dst_inner = dst_map.determine_map(destination);
            let mut src_guard = src_map.shards()[src_inner].write();
            let mut dst_guard = dst_map.shards()[dst_inner].write();

            let src_hash = src_map.hash_usize(&source);
            let dst_hash = dst_map.hash_usize(&destination);
            let popped = src_guard
                    .get_mut(src_hash as u64, |(k, _)| k == source)
                    .and_then(|(_, v)| {
                        match v.get_mut().value.as_list_mut() {
                            Ok(list) => {
                                if source_left { list.pop_front() } else { list.pop_back() }
                            }
                            Err(e) => {
                                result = Err(e);
                                None
                            }
                        }
                    });

            if result.is_err() {
                return result;
            }

            let Some(value) = popped else {
                return Ok(None);
            };

            match dst_guard.get_mut(dst_hash as u64, |(k, _)| k == destination) {
                Some((_, v)) => {
                    match v.get_mut().value.as_list_mut() {
                        Ok(list) => {
                            if dest_left { list.push_front(value.clone()); } else { list.push_back(value.clone()); }
                        }
                        Err(e) => return Err(e),
                    }
                }
                None => {
                    // destination 不存在，创建新列表
                    let mut deque = VecDeque::new();
                    if dest_left { deque.push_front(value.clone()); } else { deque.push_back(value.clone()); }
                    let entry = Entry::new(deque, None);
                    let hash = dst_hash as u64;
                    dst_guard.insert(hash, (destination.clone(), dashmap::SharedValue::new(entry)), |(k, _)| dst_map.hash_usize(k) as u64);
                }
            }
            return Ok(Some(value));
        }
    }

    /// 注册一个 waiter，返回 receiver
    fn register_waiter(&self, key: &Bytes) -> oneshot::Receiver<(Bytes, Bytes)> {
        let (tx, rx) = oneshot::channel();
        let mut entry = self.waiters
            .entry(key.clone())
            .or_insert_with(VecDeque::new);

        entry.push_front(tx);
        rx
    }

    /// LPUSH 时调用：检查有没有 waiter，有就直接发数据，返回 true；没有返回 false
    fn try_notify_waiter(&self, key: &Bytes, value: &Bytes) -> bool {
        self.waiters.get_mut(key)
            .map_or(false, |mut waiters| {
                waiters.pop_back().map_or(false, |tx| {
                    tx.send((key.clone(), value.clone())).is_ok()
                })
            })
    }
}
