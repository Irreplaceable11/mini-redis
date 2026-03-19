use std::os::unix::net::UnixStream as StdUnixStream;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;

/// 每个核心的通知通道：write_fd 用于跨线程写 1 字节唤醒，read_half 留给 monoio async read
pub struct NotifyPair {
    pub read_half: StdUnixStream,  // 留给本核心，转成 monoio UnixStream 做 async read
    pub write_fd: RawFd,           // 共享给所有核心，用 libc::write 发通知
}

/// 创建 N 个核心的通知通道
/// 返回 (read_halves, write_fds)
/// - read_halves[i]: 核心 i 拿走，转成 monoio UnixStream
/// - write_fds: Arc 共享给所有核心，write_fds[i] 就是往核心 i 发通知的 fd
pub fn create_notify_pairs(core_count: usize) -> (Vec<StdUnixStream>, Arc<Vec<RawFd>>) {
    let mut read_halves = Vec::with_capacity(core_count);
    let mut write_fds = Vec::with_capacity(core_count);

    for _ in 0..core_count {
        let (read_half, write_half) = StdUnixStream::pair()
            .expect("Failed to create UnixStream pair");

        // 写端设为非阻塞，避免跨线程写时阻塞
        write_half.set_nonblocking(true).expect("Failed to set nonblocking");
        // 读端也设为非阻塞，monoio 需要
        read_half.set_nonblocking(true).expect("Failed to set nonblocking");

        // 保存写端的 raw fd，注意：我们需要 leak write_half 防止它被 drop 关闭 fd
        let write_fd = write_half.as_raw_fd();
        std::mem::forget(write_half); // 故意泄漏，fd 生命周期跟进程一样长

        write_fds.push(write_fd);
        read_halves.push(read_half);
    }

    (read_halves, Arc::new(write_fds))
}

/// Notifier: 任何核心都可以用它来通知目标核心
/// 通过往目标核心的 unix socket 写端写 1 字节来唤醒
#[derive(Clone)]
pub struct Notifier {
    write_fds: Arc<Vec<RawFd>>,
}

impl Notifier {
    pub fn new(write_fds: Arc<Vec<RawFd>>) -> Self {
        Notifier { write_fds }
    }

    /// 通知目标核心：往它的 unix socket 写 1 字节
    /// 这是线程安全的（fd 上的 write 是原子的对于小数据量）
    pub fn notify(&self, target_core: usize) {
        if target_core >= self.write_fds.len() {
            return;
        }
        let fd = self.write_fds[target_core];
        let buf: [u8; 1] = [1];
        unsafe {
            libc::write(fd, buf.as_ptr() as *const _, 1);
        }
    }

    pub fn core_count(&self) -> usize {
        self.write_fds.len()
    }
}
