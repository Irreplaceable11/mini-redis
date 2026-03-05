pub mod frame;
pub mod connection;
mod command;
mod db;

use std::sync::Arc;
use anyhow::Result;
use time::{format_description};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{interval, Duration};

use tracing::Instrument;
use tracing::{info, info_span};
use tracing_subscriber::fmt;
use time::macros::offset;
use crate::connection::Connection;
use crate::db::Db;

#[tokio::main]
async fn main() -> Result<()> {
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
    use crate::command::Command;
    
    let peer_addr = socket.peer_addr()?;
    info!("Client connected: {}", peer_addr);
    
    let mut conn = Connection::new(socket);

    loop {
        let frame = match conn.read_frame().await? {
            Some(frame) => frame,
            None => break,
        };
        let cmd = Command::from_frame(frame);
        match cmd {
            Ok(cmd) => {
                let resp = Command::execute(cmd, &db);
                conn.write_frame(&resp).await?
            }
            Err(err) => {
                conn.write_frame(&frame::Frame::Error(err.to_string())).await?
            }
        }
      
    }
    
    info!("Client disconnected: {}", peer_addr);
    Ok(())
}
