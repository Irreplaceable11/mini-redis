use bytes::Bytes;
use rand::RngExt;
use std::time::Instant;
use tracing::info;

use super::Db;


/// 随机采样清理参数
const SAMPLE_PER_SHARD: usize = 20;       // 每个 shard 采样数
const EXPIRE_RATIO_THRESHOLD: f64 = 0.25; // 过期比例阈值，超过则继续清理
const MAX_CLEANUP_ROUNDS: usize = 16;     // 单次 clean_up 最大轮数，防止长时间阻塞

impl Db {
    pub fn del(&self, keys: Vec<Bytes>) -> usize {
        let mut count = 0;
        for key in &keys {
            let idx = self.shard_index(key);
            match self.shards[idx].remove(key) {
                Some(entry) => {
                    count += 1;
                    let key = entry.0;
                    let val = entry.1;
                    if let Some(instant) = val.ttl {
                        self.expiry_indices[idx].lock().unwrap().remove(&(instant, key));
                    }
                }
                None => {}
            }
        }
        count
    }

    pub fn exists(&self, keys: Vec<Bytes>) -> usize {
        let mut count = 0;
        for key in &keys {
            if let Some(entry) = self.shard(key).get(key) {
                if !entry.is_expired() {
                    count += 1;
                }
            }
        }
        count
    }

    pub fn ttl(&self, key: &Bytes, return_millis: bool) -> i64 {
        let shard = self.shard(key);
        if let Some(entry) = shard.get(key) {
            return if let Some(ttl) = entry.ttl {
                if !entry.is_expired() {
                    let remaining = ttl.saturating_duration_since(Instant::now());
                    return if return_millis {
                        remaining.as_millis() as i64
                    } else {
                        remaining.as_secs() as i64
                    };
                }
                drop(entry);
                shard.remove(key);
                -2
            } else {
                -1
            }
        }
        -2
    }

    pub fn expire(&self, key: &Bytes, expire_at: Option<Instant>) -> u8 {
        let idx = self.shard_index(key);
        let shard = &self.shards[idx];
        // 没有expire_at代表删除
        let mut guard = self.expiry_indices[idx].lock().unwrap();
        match expire_at {
            None => {
                match shard.remove(key) {
                    Some(entry) => {
                        let key = entry.0;
                        let val = entry.1;
                        if let Some(instant) = val.ttl {
                            guard.remove(&(instant, key));
                        }
                        1
                    }
                    None => 0
                }
            }
            Some(new_ttl) => {
                if let Some(mut entry) = shard.get_mut(key) {
                    if entry.is_expired() {
                        drop(entry);
                        if let Some(entry) = shard.remove(key) {
                            let key = entry.0;
                            let val = entry.1;
                            if let Some(instant) = val.ttl {
                                guard.remove(&(instant, key));
                            }
                        }
                        return 0;
                    }
                    if let Some(instant) = entry.ttl {
                        guard.remove(&(instant, key.clone()));
                    }
                    entry.ttl = Some(new_ttl);
                    guard.insert((new_ttl, key.clone()), ());
                    1
                } else {
                    0
                }
            }
        }
    }

    /// [新方案] 随机采样 + 自适应循环清理
    /// 类似 Redis 的 activeExpireCycle：
    /// 1. 随机选 shard，从中采样若干 key
    /// 2. 删除已过期的
    /// 3. 如果过期比例 > 25%，继续下一轮
    /// 4. 达到最大轮数或过期比例低于阈值时停止
    pub fn clean_up(&self) {
        let total_shards = self.shards.len();
        let mut rng = rand::rng();
        let mut total_sampled = 0usize;
        let mut total_cleaned = 0usize;
        let now = Instant::now();

        for _round in 0..MAX_CLEANUP_ROUNDS {
            let mut round_sampled = 0usize;
            let mut round_expired = 0usize;

            // 每轮随机选几个 shard 进行采样
            for _ in 0..4 {
                let shard_idx = rng.random_range(0..total_shards);
                let shard = &self.shards[shard_idx];

                // 从 shard 中采样有 TTL 的 key
                let mut sampled = 0;
                let mut expired_keys: Vec<Bytes> = Vec::new();

                for entry in shard.iter() {
                    if sampled >= SAMPLE_PER_SHARD {
                        break;
                    }
                    if let Some(ttl) = entry.ttl {
                        sampled += 1;
                        if now > ttl {
                            expired_keys.push(entry.key().clone());
                        }
                    }
                }

                round_sampled += sampled;
                round_expired += expired_keys.len();

                // 删除过期 key
                for key in expired_keys {
                    shard.remove(&key);
                }
            }

            total_sampled += round_sampled;
            total_cleaned += round_expired;

            // 如果采样数为 0（没有带 TTL 的 key）或过期比例低于阈值，停止
            if round_sampled == 0 {
                break;
            }
            let ratio = round_expired as f64 / round_sampled as f64;
            if ratio < EXPIRE_RATIO_THRESHOLD {
                break;
            }
        }

        if total_cleaned > 0 {
            info!(cleaned = total_cleaned, sampled = total_sampled, "随机采样过期清理完成");
        }
    }
}
