use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

use anyhow::Result;
use monoio::net::TcpListener;
use monoio::net::UnixStream;
use monoio::time::TimeDriver;
use monoio::{IoUringDriver, Runtime, RuntimeBuilder};
use socket2::{Domain, Socket, Type};
use time::format_description;
use time::macros::offset;
use tracing_subscriber::fmt;

use mini_redis::context::Context;
use mini_redis::db::Db;
use mini_redis::dispatcher::{self, CoreBus, create_core_channels};
use mini_redis::handler::handle_connection;
use mini_redis::notifier::{self, Notifier};

fn main() -> Result<()> {
    init_log();

    let core_ids = core_affinity::get_core_ids().expect("Unable to get core IDs");
    let core_count = core_ids.len();

    // 1. 创建通知通道：每个核心一对 unix socket
    let (read_halves, write_fds) = notifier::create_notify_pairs(core_count);
    let notifier = Notifier::new(write_fds);

    // 2. 创建核心间消息通道：每个核心一个 crossbeam mpsc
    let channels = create_core_channels(core_count);
    let senders: Arc<Vec<_>> = Arc::new(channels.iter().map(|(s, _)| s.clone()).collect());

    // 3. 把 read_half 和 receiver 按核心分配
    //    用 Option 包装以便在循环中 take 出来
    let mut per_core: Vec<Option<_>> = read_halves
        .into_iter()
        .zip(channels.into_iter().map(|(_, r)| r))
        .map(|(rh, rx)| Some((rh, rx)))
        .collect();

    let mut handles = Vec::new();

    for (core_idx, core_id) in core_ids.into_iter().enumerate() {
        let notifier = notifier.clone();
        let senders = senders.clone();
        let (read_half, my_receiver) = per_core[core_idx].take().unwrap();

        let handle = thread::spawn(move || {
            // 绑定当前线程到特定核心
            core_affinity::set_for_current(core_id);
            println!("Thread started on core {:?} (index {})", core_id, core_idx);

            let mut runtime = create_runtime().unwrap();

            runtime.block_on(async {
                let addr: SocketAddr = "0.0.0.0:6377".parse().unwrap();
                let listener = create_reuse_port_listener(addr).expect("Failed to bind");

                // 创建本核心的 Context（Db）
                let ctx = Rc::new(RefCell::new(Context::new(Db::new())));

                // 创建本核心的 CoreBus
                let bus = Rc::new(CoreBus::new(
                    core_idx,
                    core_count,
                    notifier,
                    senders,
                    my_receiver,
                ));

                // 把 std UnixStream 读端转成 monoio UnixStream
                let notify_stream = UnixStream::from_std(read_half)
                    .expect("Failed to convert UnixStream");

                // 启动通知监听 task：异步等待其他核心的唤醒信号
                monoio::spawn(dispatcher::notify_listener(
                    notify_stream,
                    bus.clone(),
                    ctx.clone(),
                ));

                // 主循环：accept 连接
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let ctx_clone = ctx.clone();
                    let bus_clone = bus.clone();
                    monoio::spawn(async move {
                        handle_connection(stream, ctx_clone, bus_clone).await
                    });
                }
            });
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

fn create_runtime() -> Result<Runtime<TimeDriver<IoUringDriver>>> {
    let rt: Runtime<_> = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(32768)
        .enable_timer()
        .build()?;
    Ok(rt)
}

fn create_reuse_port_listener(addr: SocketAddr) -> Result<TcpListener> {
    let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let socket = Socket::new(domain, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.set_tcp_nodelay(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let std_listener: std::net::TcpListener = socket.into();
    Ok(TcpListener::from_std(std_listener)?)
}

fn init_log() {
    let timer_format = format_description::parse(
        "[year]-[month padding:zero]-[day padding:zero] [hour]:[minute]:[second]",
    )
    .expect("时间格式字符串无效");
    let timer = fmt::time::OffsetTime::new(offset!(+8), timer_format);
    tracing_subscriber::fmt().with_timer(timer).init();
}
