use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{Duration, interval};

use mini_redis::context::Context;
use mini_redis::db::Db;
use mini_redis::handler::handle_connection;
use mini_redis::pubsub::PubSub;

#[cfg(debug_assertions)]
use time::format_description;
#[cfg(debug_assertions)]
use time::macros::offset;
#[cfg(debug_assertions)]
use tracing::Instrument;
#[cfg(debug_assertions)]
use tracing::{info, info_span};
#[cfg(debug_assertions)]
use tracing_subscriber::{EnvFilter, fmt};

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
    //编译 RUSTFLAGS="-C target-cpu=native" cargo build --release
    //这样编译器会针对你本机 CPU 的指令集做额外优化
    // redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 64
    // redis基准测试 pipeline模式 get qps:6097561/s set qps:4273504/s (wsl2 ubuntu 24.04)
    // redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get
    // 普通模式 get qps:134770/s  set qps: 135501/s(wsl2 ubuntu 24.04)

    #[cfg(debug_assertions)]
    init_log().await;

    let listener = TcpListener::bind(("0.0.0.0", 6377)).await?;

    #[cfg(debug_assertions)]
    info!("mini redis listening on {:?}", listener.local_addr()?);
    #[cfg(not(debug_assertions))]
    eprintln!("mini redis listening on {:?}", listener.local_addr()?);

    let ctx = Arc::new(Context::new(Db::new(), PubSub::new()));

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

        #[cfg(debug_assertions)]
        {
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

        #[cfg(not(debug_assertions))]
        {
            let _ = addr;
            tokio::spawn(async move {
                let _ = handle_connection(socket, conn_ctx).await;
            });
        }
    }
}

#[cfg(debug_assertions)]
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
