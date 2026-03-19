use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use anyhow::Result;
use monoio::net::TcpListener;
use monoio::time::TimeDriver;
use monoio::{IoUringDriver, Runtime, RuntimeBuilder};
use socket2::{Domain, Socket, Type};
use time::format_description;
use time::macros::offset;
use tracing_subscriber::fmt;

use mini_redis::context::Context;
use mini_redis::db::ShardedDb;
use mini_redis::handler::handle_connection;

fn main() -> Result<()> {
    init_log();

    let core_ids = core_affinity::get_core_ids().expect("Unable to get core IDs");
    let _core_count = core_ids.len();

    // 所有核心共享一个分片数据库
    let shared_db = ShardedDb::new();

    let mut handles = Vec::new();

    for (core_idx, core_id) in core_ids.into_iter().enumerate() {
        let db = shared_db.clone();

        let handle = thread::spawn(move || {
            core_affinity::set_for_current(core_id);
            println!("Thread started on core {:?} (index {})", core_id, core_idx);

            let mut runtime = create_runtime().unwrap();

            runtime.block_on(async {
                let addr: SocketAddr = "0.0.0.0:6377".parse().unwrap();
                let listener = create_reuse_port_listener(addr).expect("Failed to bind");

                // 每个核心持有同一个 Arc<ShardedDb> 的引用
                let ctx = Arc::new(Context::new(db));

                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let ctx_clone = ctx.clone();
                    monoio::spawn(async move {
                        if let Err(e) = handle_connection(stream, ctx_clone, core_idx).await {
                            tracing::error!("connection error on core {}: {:?}", core_idx, e);
                        }
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
