use anyhow::Result;
use mini_redis::aof::{Aof, AofEntry, FsyncPolicy, RewriteState};
use mini_redis::db::Db;
use std::env;
use std::sync::Arc;
use time::format_description;
use time::macros::offset;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch::Sender as WatchSender;
use tokio::time::{Duration, interval};
use tracing::{info, info_span, Instrument};
use tracing_subscriber::{EnvFilter, fmt};

use mini_redis::context::Context;
use mini_redis::handler::handle_connection;
use mini_redis::pubsub::PubSub;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    init_log().await;

    let listener = TcpListener::bind(("0.0.0.0", 6377)).await?;
    info!("mini redis listening on {:?}", listener.local_addr()?);

    let (aof, rx, watch_sender) = init_aof().await?;
    let sender_clone = aof.sender.clone();
    let ctx = Arc::new(Context::new(Db::new(), PubSub::new(), Some(sender_clone), Some(watch_sender)));

    aof.replay(ctx.clone())?;
    let aof_ctx = ctx.clone();
    //aof单独一个线程
    std::thread::Builder::new()
        .name("aof-writer".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create AOF runtime");
            rt.block_on(aof.start_aof_writer(rx, aof_ctx))
        })?;

    // 启动定期清理过期 key 的后台任务
    let cleanup_ctx = ctx.clone();
    tokio::spawn(async move {
        let mut intv = interval(Duration::from_secs(30));
        loop {
            intv.tick().await;
            cleanup_ctx.db().clean_up();
        }
    });

    loop {
        let (socket, addr) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let conn_ctx = ctx.clone();

        let span = info_span!("handle_connection", client_addr = %addr);
        tokio::spawn(
            async move {
                if let Err(e) = handle_connection(socket, conn_ctx).await {
                    info!("connection error: {}", e);
                }
            }
            .instrument(span),
        );
    }
}

async fn init_log() {
    let timer_format = format_description::parse(
        "[year]-[month padding:zero]-[day padding:zero] [hour]:[minute]:[second]",
    )
    .expect("时间格式字符串无效");
    let timer = fmt::time::OffsetTime::new(offset!(+8), timer_format);
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error")),
        )
        .with_timer(timer)
        .init();
}

async fn init_aof() -> Result<(Aof, Receiver<Vec<AofEntry>>, WatchSender<RewriteState>)> {
    let exe_path = env::current_exe()?;
    let exe_dir = match exe_path.parent() {
        Some(dir) => dir,
        None => return Err(anyhow::anyhow!("无法获取可执行文件的目录")),
    };
    let (aof, rx, sender) = Aof::new(FsyncPolicy::EverySec, exe_dir.join("listendb.aof"))?;
    Ok((aof, rx, sender))
}
