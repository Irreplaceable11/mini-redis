pub mod frame;
pub mod connection;
mod command;

use anyhow::{anyhow, Result};
use time::{format_description};
use tokio::net::{TcpListener, TcpStream};

use tracing::Instrument;
use tracing::{info, info_span};
use tracing_subscriber::fmt;
use time::macros::offset;
use crate::connection::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    init_log().await;
    let listener = TcpListener::bind(("127.0.0.1", 6377)).await?;
    info!("mini redis listening on {:?}", listener.local_addr()?);

    loop {
        let (socket, addr) = listener.accept().await?;

        let span = info_span!("handle_connection", client_addr = %addr);
        let _ = tokio::spawn(
            async move {
               let _ = handle_connection(socket).await;
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

pub async fn handle_connection(socket: TcpStream) -> Result<()> {
    use crate::command::Command;
    
    let peer_addr = socket.peer_addr()?;
    info!("Client connected: {}", peer_addr);
    
    let mut conn = Connection::new(socket);

    loop {
        let frame = match conn.read_frame().await? {
            Some(frame) => frame,
            None => break,
        };
        
        match Command::from_frame(frame)? {
            Command::Ping(cmd) => {
                let resp = cmd.into_frame();
                conn.write_frame(&resp).await?;
            }
            _ => return Err(anyhow!("Unsupported command"))
        }
    }
    
    info!("Client disconnected: {}", peer_addr);
    Ok(())
}
