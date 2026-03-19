use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use crossbeam_channel::{Receiver, Sender, unbounded};
use monoio::io::AsyncReadRent;
use monoio::net::UnixStream;

use crate::context::Context;
use crate::frame::Frame;
use crate::notifier::Notifier;

// ============ 消息类型定义 ============

/// 跨核心请求：包含要执行的命令帧 + 回传结果的 channel
pub struct RemoteRequest {
    pub frame: Frame,                       // 客户端发来的命令帧
    pub reply_tx: Sender<Frame>,            // 把结果发回请求方
    pub sender_core: usize,                 // 请求方的核心 ID，用于 notify 唤醒
}

/// 跨核心响应：从 reply_rx 收到的结果
pub struct RemoteResponse {
    pub reply_rx: Receiver<Frame>,
}

// ============ 核心间通信总线 ============

/// CoreBus: 每个核心持有一份，包含：
/// - my_core_id: 本核心编号
/// - core_count: 总核心数
/// - notifier: 用于唤醒目标核心
/// - senders[i]: 往核心 i 发请求的 sender
/// - my_receiver: 本核心的请求接收端
pub struct CoreBus {
    pub my_core_id: usize,
    pub core_count: usize,
    pub notifier: Notifier,
    senders: Arc<Vec<Sender<RemoteRequest>>>,   // senders[i] = 往核心 i 发请求
    my_receiver: Receiver<RemoteRequest>,        // 本核心的请求接收端
}

/// 创建所有核心的通信总线
/// 返回 Vec<(Sender, Receiver)>，index = core_id
pub fn create_core_channels(core_count: usize) -> Vec<(Sender<RemoteRequest>, Receiver<RemoteRequest>)> {
    (0..core_count)
        .map(|_| unbounded::<RemoteRequest>())
        .collect()
}

impl CoreBus {
    pub fn new(
        my_core_id: usize,
        core_count: usize,
        notifier: Notifier,
        senders: Arc<Vec<Sender<RemoteRequest>>>,
        my_receiver: Receiver<RemoteRequest>,
    ) -> Self {
        CoreBus {
            my_core_id,
            core_count,
            notifier,
            senders,
            my_receiver,
        }
    }

    /// 计算 key 应该路由到哪个核心
    pub fn route_key(&self, key: &str) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.core_count
    }

    /// 判断 key 是否在本核心
    pub fn is_local(&self, key: &str) -> bool {
        self.route_key(key) == self.my_core_id
    }

    /// 发送远程请求到目标核心，返回一个 Receiver 用于等待结果
    pub fn send_remote(&self, target_core: usize, frame: Frame) -> RemoteResponse {
        let (reply_tx, reply_rx) = unbounded::<Frame>();

        let request = RemoteRequest {
            frame,
            reply_tx,
            sender_core: self.my_core_id,
        };

        // 发送到目标核心的队列
        self.senders[target_core]
            .send(request)
            .expect("Failed to send remote request");

        // 唤醒目标核心的事件循环
        self.notifier.notify(target_core);

        RemoteResponse { reply_rx }
    }

    /// 从本核心的队列中取出所有待处理的远程请求，执行并回复
    pub fn process_incoming(&self, ctx: &mut Context) {
        while let Ok(request) = self.my_receiver.try_recv() {
            // 解析并执行命令
            let response = match crate::command::Command::from_frame(request.frame) {
                Ok(cmd) => {
                    if let crate::command::Command::Keys(keys) = cmd {
                        keys.execute(ctx)
                    } else {
                        crate::command::Command::execute(cmd, ctx)
                    }
                }
                Err(e) => Frame::Error(e.to_string()),
            };

            // 把结果发回请求方
            let _ = request.reply_tx.send(response);

            // 唤醒请求方核心
            self.notifier.notify(request.sender_core);
        }
    }
}

// ============ 事件循环监听器 ============

/// 在 monoio 事件循环中监听 unix socket 通知，收到通知后处理远程请求
/// 这个函数会被 monoio::spawn 到每个核心上
pub async fn notify_listener(
    mut read_stream: UnixStream,
    bus: Rc<CoreBus>,
    ctx: Rc<RefCell<Context>>,
) {
    let mut buf = vec![0u8; 64]; // 读缓冲区，eventfd/unix socket 通知数据不重要

    loop {
        // 异步等待通知（monoio 的 AsyncReadRent，底层走 io_uring）
        let (result, returned_buf) = read_stream.read(buf).await;
        buf = returned_buf;

        match result {
            Ok(0) => {
                // 写端关闭了，不应该发生
                tracing::error!("notify unix socket EOF on core {}", bus.my_core_id);
                break;
            }
            Ok(_n) => {
                // 收到通知，处理所有待处理的远程请求
                bus.process_incoming(&mut ctx.borrow_mut());
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // 非阻塞模式下没数据，继续等
                continue;
            }
            Err(e) => {
                tracing::error!("notify read error on core {}: {:?}", bus.my_core_id, e);
                break;
            }
        }
    }
}
