use anyhow::Result;
use std::sync::Arc;
use time::format_description;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, interval};

use mini_redis::connection::Connection;
use mini_redis::db::Db;
use mini_redis::frame;
use time::macros::offset;
use tracing::Instrument;
use tracing::{info, info_span};
use tracing_subscriber::fmt;

#[tokio::main]
async fn main() -> Result<()> {
    // redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t get,set -P 16
    // redis基准测试 pipeline模式 get qps:719424/s set qps:354609/s
    // redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get
    // 普通模式 get qps:66934/s  set qps: 67069/s
    init_log().await;
    let listener = TcpListener::bind(("127.0.0.1", 6377)).await?;
    info!("mini redis listening on {:?}", listener.local_addr()?);
    let db_arc = Arc::new(Db::new());
    let arc1 = db_arc.clone();
    tokio::spawn(async move {
        info!("starting db clean up");
        tokio::time::sleep(Duration::from_secs(5 * 60)).await;

        // 2. 创建周期性定时器：每 30 秒一次
        let mut intv = interval(Duration::from_secs(30));
        loop {
            intv.tick().await; // 等待下一个 30 秒时间点

            // 执行清理
            arc1.clean_up();
        }
    });
    loop {
        let (socket, addr) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let span = info_span!("handle_connection", client_addr = %addr);
        let db = db_arc.clone();
        tokio::spawn(
            async move {
                let _ = handle_connection(socket, db).await;
            }
            .instrument(span),
        );
    }
}

pub async fn init_log() {
    // 自定义时间格式：yyyy-MM-dd HH:mm:ss
    let timer_format = format_description::parse(
        "[year]-[month padding:zero]-[day padding:zero] [hour]:[minute]:[second]",
    )
    .expect("时间格式字符串无效");
    let timer = fmt::time::OffsetTime::new(offset!(+8), timer_format);
    tracing_subscriber::fmt().with_timer(timer).init();
}

pub async fn handle_connection(socket: TcpStream, db: Arc<Db>) -> Result<()> {
    use mini_redis::command::Command;

    let peer_addr = socket.peer_addr()?;
    info!("Client connected: {}", peer_addr);

    let mut conn = Connection::new(socket);
    loop {
        let mut frames = Vec::new();
        // 读取第一个frame
        match conn.read_frame().await? {
            Some(frame) => frames.push(frame),
            None => break //说明什么也没读过
        }
        // 再读取stream中所有的frame
        // 对比之前读取一个就处理返回可以提升吞吐量
        loop {
            let frame = match conn.try_read_frame()? {
                Some(frame) => frame,
                None => break,
            };
            frames.push(frame);
        }

        for ele in frames {
            let cmd = Command::from_frame(ele);
            match cmd {
                Ok(cmd) => {
                    let resp = Command::execute(cmd, &db);
                    conn.encode_to_buffer(&resp)?
                }
                Err(err) => {
                    conn.encode_to_buffer(&frame::Frame::Error(err.to_string()))?
                }
            }
        }
        conn.write_and_flush().await?;
       
    }

    info!("Client disconnected: {}", peer_addr);
    Ok(())
}
