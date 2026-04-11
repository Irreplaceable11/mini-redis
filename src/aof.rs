use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info};

use crate::context::Context;

#[derive(Clone, Copy)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AofEntry {
    // key, value, expire_at_ms(绝对时间戳), nx, xx
    Set(Bytes, Bytes, Option<i64>, bool, bool),
    // vec<key>
    Del(Vec<Bytes>),
    // key, expire_at_ms(绝对时间戳)
    Expire(Bytes, Option<i64>),
}

impl AofEntry {
    /// 从 Set 命令的字段构造 AofEntry
    /// 将相对过期时间(Option<Instant>)转为绝对时间戳(毫秒)存储
    pub fn from_set(
        key: Bytes,
        value: Bytes,
        instant: Option<Instant>,
        nx: bool,
        xx: bool,
    ) -> Self {
        AofEntry::Set(key, value, Self::instant_to_ms(instant), nx, xx)
    }

    /// 从 Expire 命令的字段构造 AofEntry
    pub fn from_expire(key: Bytes, instant: Option<Instant>) -> Self {
        AofEntry::Expire(key, Self::instant_to_ms(instant))
    }

    /// 将 Option<Instant> 转换为绝对时间戳(毫秒)
    /// 利用 Instant::now() 与 Utc::now() 的差值桥接单调时钟和墙钟
    fn instant_to_ms(instant: Option<Instant>) -> Option<i64> {
        instant.map(|exp| {
            let remain = exp.saturating_duration_since(Instant::now());
            Utc::now().timestamp_millis() + remain.as_millis() as i64
        })
    }
}

pub struct Aof {
    pub sender: Sender<Vec<AofEntry>>,

    fsync_policy: FsyncPolicy,

    path: PathBuf,
}

impl Aof {
    pub fn new(fsync_policy: FsyncPolicy, path: PathBuf) -> Result<(Aof, Receiver<Vec<AofEntry>>)> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(&parent)?;
            }
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(4096);
        let aof = Self {
            fsync_policy,
            path,
            sender,
        };
        Ok((aof, receiver))
    }

    pub async fn start_aof_writer(self, mut receiver: Receiver<Vec<AofEntry>>) -> Result<()> {
        let mut buffer = vec![];
        let file = std::fs::File::options()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut writer = BufWriter::new(file);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut pre_allocation: Vec<u8> = Vec::with_capacity(512);
        loop {
            tokio::select! {
                count = receiver.recv_many(&mut buffer, 4096) => {
                    if count == 0 {
                        return Ok(())
                    }
                    for batch in buffer.drain(..) {
                        for msg in batch {
                            pre_allocation.clear();
                            match bincode::serialize_into(&mut pre_allocation, &msg) {
                                Ok(_) => {
                                    let len = pre_allocation.len() as u32;
                                    let len_bytes = len.to_be_bytes();
                                    writer.write_all(&len_bytes)?;
                                    writer.write_all(&pre_allocation)?;
                                }
                                Err(e) => {
                                    error!("Aof Serialization Error: {}", e);
                                }
                            }
                        }
                    }
                    writer.flush()?;
                    match self.fsync_policy {
                        FsyncPolicy::Always => {
                            writer.get_ref().sync_all()?;
                        }
                        FsyncPolicy::No => {}
                        _ => {}
                    }
                }
                _ = interval.tick() => {
                    if matches!(self.fsync_policy, FsyncPolicy::EverySec) {
                        writer.flush()?;
                        writer.get_ref().sync_all()?;
                    }
                }
            }
        }
    }

    pub fn replay(&self, ctx: Arc<Context>) -> Result<()> {
        if !self.path.exists() {
            info!("AOF 文件不存在，跳过重放");
            return Ok(());
        }
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);

        let mut len_buf = [0u8; 4]; // 用来存放长度前缀的缓冲区

        loop {
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {} // 成功读到长度头
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // 读到文件末尾，正常退出循环
                    info!("AOF 文件读取完毕");
                    return Ok(());
                }
                Err(e) => {
                    error!("read aof file error:{}", e);
                    return Ok(());
                } // 其他 IO 错误
            };

            let data_len = u32::from_be_bytes(len_buf) as usize;

            // 创建一个刚好能装下数据的 Vec
            let mut data_buf = vec![0u8; data_len];

            reader.read_exact(&mut data_buf)?;

            //  反序列化并处理
            match bincode::deserialize::<AofEntry>(&data_buf) {
                Ok(entry) => {
                    debug!("读取到 Entry: {:?}", entry);
                    match entry {
                        AofEntry::Set(k, v, expire_at_ms, nx, xx) => {
                            let (is_expired, instant) = Self::to_instant(expire_at_ms);
                            if !is_expired {
                                ctx.db().set(k, v, instant, nx, xx);
                            }
                        }
                        AofEntry::Del(k) => {
                            ctx.db().del(k);
                        }
                        AofEntry::Expire(k, expire_at_ms) => {
                            let (_, instant) = Self::to_instant(expire_at_ms);
                            ctx.db().expire(&k, instant);
                        }
                    }
                }
                Err(e) => {
                    error!("反序列化失败: {}", e);
                }
            };
        }
    }

    /// 将绝对时间戳(毫秒)转换为 Instant，同时判断是否已过期
    fn to_instant(expire_at_ms: Option<i64>) -> (bool, Option<Instant>) {
        let mut is_expired = false;
        let instant = match expire_at_ms {
            Some(timestamp) => {
                let now = Utc::now().timestamp_millis();
                let remain = timestamp - now;
                if remain > 0 {
                    let duration = Duration::from_millis(remain as u64);
                    Some(Instant::now() + duration)
                } else {
                    is_expired = true;
                    None
                }
            }
            None => None,
        };
        (is_expired, instant)
    }
}
