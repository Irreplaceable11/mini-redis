use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration};
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
    // key value ttl nx xx
    Set(Bytes, Bytes, Option<i64>, bool, bool),
    // vec<key>
    Del(Vec<Bytes>),
    // key ttl
    Expire(Bytes, Option<i64>),
}

pub struct Aof {
    sender: Sender<AofEntry>,

    fsync_policy: FsyncPolicy,

    path: PathBuf,
}

impl Aof {
    pub fn new(fsync_policy: FsyncPolicy, path: PathBuf) -> Result<(Aof, Receiver<AofEntry>)> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(&parent)?;
            }
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(4090);
        let aof = Self {
            fsync_policy,
            path,
            sender,
        };
        Ok((aof, receiver))
    }

    pub async fn send(&self, entry: AofEntry) -> Result<()> {
        self.sender.send(entry).await?;
        Ok(())
    }

    pub async fn start_aof_writer(
        self,
        mut receiver: Receiver<AofEntry>,
    ) -> Result<()> {
        let mut buffer = vec![];
        let file = std::fs::File::options()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut writer = BufWriter::new(file);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                count = receiver.recv_many(&mut buffer, 100) => {
                    if count == 0 {
                        return Ok(())
                    }
                    for msg in buffer.drain(..) {
                        match bincode::serialize(&msg) {
                            Ok(bytes) => {
                                let len = bytes.len() as u32;
                                let len_bytes = len.to_be_bytes();
                                writer.write_all(&len_bytes)?;
                                writer.write_all(&bytes)?;
                            }
                            Err(e) => {
                                error!("Aof Serialization Error: {}", e);
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

    pub fn replay(self, ctx: Context) -> Result<()> {
        let file = File::open(self.path)?;
        let mut reader = BufReader::new(file);

        let mut len_buf = [0u8; 4]; // 用来存放长度前缀的缓冲区

        loop {
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}, // 成功读到长度头
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // 读到文件末尾，正常退出循环
                    info!("AOF 文件读取完毕");
                    return Ok(())
                }
                Err(e) => error!("read aof file error:{}", e), // 其他 IO 错误
            };

            let data_len = u32::from_be_bytes(len_buf) as usize;

            // 创建一个刚好能装下数据的 Vec
            let mut data_buf = vec![0u8; data_len];

            reader.read_exact(&mut data_buf)?;

            // 6. 反序列化并处理
            match bincode::deserialize::<AofEntry>(&data_buf) {
                Ok(entry) => {
                    debug!("读取到 Entry: {:?}", entry);
                    match entry {
                        AofEntry::Set(k, v, ttl, nx, xx) => {
                            let instant = match ttl {
                                Some(timestamp) => {
                                    
                                }
                                None => None
                            };
                        }
                        AofEntry::Del(k) => {

                        }
                        AofEntry::Expire(k, ttl) => {

                        }
                    }
                }
                Err(e) => {
                    error!("反序列化失败: {}", e);
                }
            };
        }
    }
}
