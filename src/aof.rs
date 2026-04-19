use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::watch::{Receiver as WatchReceiver, Sender as WatchSender};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info};

use crate::context::Context;
use crate::db::EntryValue;

#[derive(Clone, Copy)]
pub enum RewriteState {
    Normal,    // 正常模式，不做额外操作
    Rewriting, // rewrite 进行中，需要双写增量文件
}

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

    Push(Bytes, Vec<Bytes>, bool),

    // key, count, value
    Pop(Bytes, bool),

    // key, count, value
    Lrem(Bytes, isize, Bytes),

    // key, index, value
    Lset(Bytes, isize, Bytes),
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
const AOF_REWRITE_THRESHOLD: u64 =  10 * 1024 * 1024;
pub struct Aof {
    pub sender: Sender<Vec<AofEntry>>,

    fsync_policy: FsyncPolicy,

    path: PathBuf,

    aof_current_size: u64,

    rewrite_state: WatchReceiver<RewriteState>
}

impl Aof {
    pub fn new(fsync_policy: FsyncPolicy, path: PathBuf) -> Result<(Aof, Receiver<Vec<AofEntry>>, WatchSender<RewriteState>)> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(&parent)?;
            }
        }
        let file_len = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(4096);
        let (tx, rx) = tokio::sync::watch::channel(RewriteState::Normal);
        let aof = Self {
            fsync_policy,
            path,
            sender,
            aof_current_size: file_len,
            rewrite_state: rx
        };
        Ok((aof, receiver, tx))
    }

    pub async fn start_aof_writer(mut self, mut receiver: Receiver<Vec<AofEntry>>, ctx: Arc<Context>) -> Result<()> {
        let mut buffer = vec![];
        let file = File::options()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut aof_writer = BufWriter::new(file);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut pre_allocation: Vec<u8> = Vec::with_capacity(512);

        let mut incr_writer: Option<BufWriter<File>> = None;

        let mut snapshot_handle: Option<tokio::task::JoinHandle<Result<()>>> = None;
        let incr_aof_path = self.path.parent().unwrap().join("listendb.aof.incr");
        let temp_aof_path = self.path.parent().unwrap().join("rewrite-temp.aof");

        let mut is_rewriting = false;
        loop {
            tokio::select! {
                count = receiver.recv_many(&mut buffer, 4096) => {
                    if count == 0 {
                        return Ok(())
                    }
                    for batch in buffer.drain(..) {
                        for msg in batch {
                            let write_len = Self::serialize_to_file(&mut pre_allocation, &msg, &mut aof_writer)?;
                            self.aof_current_size += write_len;
                            // 如果正在 rewrite，同时写增量文件
                            if let Some(ref mut incr_writer) = incr_writer {
                                is_rewriting = true;
                                Self::serialize_to_file(&mut pre_allocation, &msg, incr_writer)?;
                            }
                        }
                    }
                    aof_writer.flush()?;
                    if self.aof_current_size >= AOF_REWRITE_THRESHOLD && !is_rewriting {
                        info!("AOF rewrite triggered, current size: {} bytes", self.aof_current_size);
                        ctx.send_rewrite_state(RewriteState::Rewriting)?;
                    }
                    match self.fsync_policy {
                        FsyncPolicy::Always => {
                            // 高性能选择：fdatasync
                            // 仅确保数据和检索数据必需的元数据（如大小）落地
                            aof_writer.get_ref().sync_data()?;
                        }
                        FsyncPolicy::No => {}
                        _ => {}
                    }
                }
                _ = interval.tick() => {
                    if matches!(self.fsync_policy, FsyncPolicy::EverySec) {
                        aof_writer.flush()?;
                        aof_writer.get_ref().sync_data()?;
                    }
                }
                Ok(()) = self.rewrite_state.changed() => {
                    // 拿到最新的值
                    let state = *self.rewrite_state.borrow();
                    match state {
                        RewriteState::Rewriting => {
                            info!("AOF rewrite state: Rewriting, starting snapshot and incremental writer");
                            let temp_path_clone = temp_aof_path.clone();
                            let incr_path_clone = incr_aof_path.clone();
                            let incr_file = File::options()
                                .create(true)
                                .append(true)
                                .open(&incr_path_clone)?;
                            incr_writer = Some(BufWriter::new(incr_file));

                            let ctx_clone = ctx.clone();
                            let join_handler = tokio::task::spawn_blocking(move || {

                                Self::snapshot_to_file(&temp_path_clone, ctx_clone)
                            });
                            snapshot_handle = Some(join_handler);
                        }
                        RewriteState::Normal => {is_rewriting = false;}
                    }
                }
                result = async { snapshot_handle.as_mut().unwrap().await }, if snapshot_handle.is_some() => {
                    snapshot_handle = None; // 用完清掉
                    match result {
                        Ok(Ok(())) => {
                            info!("AOF snapshot completed, starting finalize");
                            // snapshot 成功，做收尾：追加增量文件、rename 等
                            if let Some(mut incr_writer) = incr_writer.take() {
                                // flush 刷盘
                                incr_writer.flush()?;
                                incr_writer.get_ref().sync_data()?;

                                //读取增量 写入rewrite-temp.aof
                                let mut reader =  BufReader::new(File::open(&incr_aof_path)?);

                                let mut temp_writer = BufWriter::new(
                                    File::options().append(true).open(&temp_aof_path)?
                                );

                                std::io::copy(&mut reader, &mut temp_writer)?;
                                temp_writer.flush()?;
                                //原子替换
                                fs::rename(&temp_aof_path, &self.path)?;
                                //重新打开主aof的writer
                                let new_aof_writer = File::options().create(true).append(true).open(&self.path)?;
                                aof_writer = BufWriter::new(new_aof_writer);
                                //更新aof_current_size
                                self.aof_current_size = fs::metadata(&self.path)?.len();

                                //清理增量文件
                                fs::remove_file(&incr_aof_path)?;

                                let _ = ctx.send_rewrite_state(RewriteState::Normal)?;
                                info!("AOF rewrite finished, new size: {} bytes", self.aof_current_size);
                            }
                        }
                        Ok(Err(e)) => {
                            error!("snapshot failed: {}", e);
                        }
                        Err(e) => {
                            error!("spawn_blocking panicked: {}", e);
                        }
                    }
                }
            }
        }
    }


    fn serialize_to_file(pre_allocation: &mut Vec<u8>, msg: &AofEntry, writer: &mut BufWriter<File>) -> Result<u64> {
        pre_allocation.clear();
        match bincode::serialize_into(&mut *pre_allocation, msg) {
            Ok(_) => {
                let len = pre_allocation.len() as u32;
                let len_bytes = len.to_be_bytes();
                writer.write_all(&len_bytes)?;
                writer.write_all(&pre_allocation)?;
                Ok((4 + pre_allocation.len()) as u64)
            }
            Err(e) => {
                error!("Aof Serialization Error: {}", e);
                Err(e.into())
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
                            let (is_expired, instant) = Self::trans_to_instant(expire_at_ms);
                            if !is_expired {
                                ctx.db().set(k, v, instant, nx, xx);
                            }
                        }
                        AofEntry::Del(k) => {
                            ctx.db().del(k);
                        }
                        AofEntry::Expire(k, expire_at_ms) => {
                            let (_, instant) = Self::trans_to_instant(expire_at_ms);
                            ctx.db().expire(&k, instant);
                        }
                        AofEntry::Push(k, values, is_left) => {
                            let _ = ctx.db().push(k, values, is_left);
                        }
                        AofEntry::Pop(k, is_left) => {
                            let _ = ctx.db().pop(&k, is_left);
                        }
                        AofEntry::Lrem(k, count, value) => {
                            let _ = ctx.db().lrem(&k, count, value);
                        }
                        AofEntry::Lset(k, index, value) => {
                            let _ = ctx.db().lset(&k, index, value);
                        }
                    }
                }
                Err(e) => {
                    error!("反序列化失败: {}", e);
                }
            };
        }
    }

    fn snapshot_to_file(path: &PathBuf, ctx: Arc<Context>) -> Result<()> {
        let file = File::options().create(true).append(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let mut pre_allocation: Vec<u8> = Vec::with_capacity(512);
        let res = ctx.db().for_each_entry(
            |key: &Bytes, value: &EntryValue, expire_at: Option<Instant>, is_expired: bool| {
                let result: Result<()> = (|| -> Result<()> {
                    if !is_expired {
                        let expire_at_ms = AofEntry::instant_to_ms(expire_at);
                        pre_allocation.clear();
                        let entry = match value {
                            EntryValue::String(bytes) => {
                                AofEntry::Set(key.clone(), bytes.clone(), expire_at_ms, false, false);
                            }
                            EntryValue::List(list) => {
                                let values: Vec<Bytes> = list.iter().cloned().collect();
                                /*
                                    假设内存里是[1,2,3]
                                    RPUSH key 1 2 3
                                    → push_back(1): [1]
                                    → push_back(2): [1, 2]
                                    → push_back(3): [1, 2, 3]
                                    LPUSH key 1 2 3
                                    → push_front(1): [1]
                                    → push_front(2): [2, 1]
                                    → push_front(3): [3, 2, 1]
                                    lpush是头插法, 队列先进先出，使用lpush插入时新元素会跑到最里面，所以使用rpush还原
                                */
                                AofEntry::Push(key.clone(), values, false);
                            }
                        };
                        bincode::serialize_into(&mut pre_allocation, &entry)?;
                        let len = pre_allocation.len() as u32;
                        let len_bytes = len.to_be_bytes();
                        writer.write_all(&len_bytes)?;
                        writer.write_all(&pre_allocation)?;
                    }
                    Ok(())
                })();
                match result {
                    Ok(_) => ControlFlow::Continue(()),
                    Err(e) => ControlFlow::Break(e)
                }
            },
        );
        if let ControlFlow::Break(err) = res {
            return Err(err);
        }
        writer.flush()?;
        writer.get_ref().sync_data()?;
        Ok(())
    }

    /// 将绝对时间戳(毫秒)转换为 Instant，同时判断是否已过期
    fn trans_to_instant(expire_at_ms: Option<i64>) -> (bool, Option<Instant>) {
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
