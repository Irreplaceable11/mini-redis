use anyhow::Result;
use std::sync::Arc;
use time::format_description;
use tokio::net::TcpListener;
use tokio::time::{Duration, interval};

use mini_redis::context::Context;
use mini_redis::db::Db;
use mini_redis::handler::handle_connection;
use mini_redis::pubsub::PubSub;
use time::macros::offset;
use tracing::Instrument;
use tracing::{info, info_span};
use tracing_subscriber::{EnvFilter, fmt};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]

async fn main() -> Result<()> {

    // redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t get,set -P 16
    // redis基准测试 pipeline模式 get qps:719424/s set qps:354609/s (windows 11 24h2数据)
    // pipeline模式 get qps:1449275/s set qps:970873/s (wsl2 ubuntu 24.04)
    // redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get
    // 普通模式 get qps:66934/s  set qps: 67069/s(windows 11 24h2数据)
    // 普通模式 get qps:103305/s  set qps: 100200/s (wsl2 ubuntu 24.04)
    init_log().await;
    let listener = TcpListener::bind(("0.0.0.0", 6377)).await?;
    info!("mini redis listening on {:?}", listener.local_addr()?);

    let ctx = Arc::new(Context::new(Db::new(), PubSub::new()));

    // 启动定期清理过期 key 的后台任务
    let cleanup_ctx = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5 * 60)).await;
        let mut intv = interval(Duration::from_secs(30));
        loop {
            intv.tick().await;
            cleanup_ctx.db().clean_up();
        }
    });

    loop {
        let (socket, addr) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let span = info_span!("handle_connection", client_addr = %addr);
        let conn_ctx = ctx.clone();
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
    .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error")))
    .with_timer(timer).init();
}
