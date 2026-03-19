use std::sync::Arc;

use anyhow::Result;
use monoio::net::TcpStream;
use tracing::info;

use crate::command::Command;
use crate::connection::Connection;
use crate::context::Context;
use crate::frame::Frame;

/// 处理一个客户端连接的完整生命周期
pub async fn handle_connection(
    socket: TcpStream,
    context: Arc<Context>,
    core_id: usize,
) -> Result<()> {
    let peer_addr = socket.peer_addr()?;
    info!("Client connected: {} on core {}", peer_addr, core_id);

    let mut conn = Connection::new(socket);
    loop {
        let mut frames = Vec::new();
        match conn.read_frame().await {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => break,
            Err(e) => {
                tracing::error!("read_frame error on core {}: {:?}", core_id, e);
                break;
            }
        }
        // 批量读取缓冲区中已有的 frame
        loop {
            match conn.try_read_frame() {
                Ok(Some(frame)) => frames.push(frame),
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("try_read_frame error on core {}: {:?}", core_id, e);
                    break;
                }
            }
        }

        for frame in frames {
            let response = execute_command(frame, &context);
            conn.encode_to_buffer(&response)?;
        }
        if let Err(e) = conn.write_and_flush().await {
            tracing::error!("write_and_flush error on core {}: {:?}", core_id, e);
            break;
        }
    }

    info!("Client disconnected: {}", peer_addr);
    Ok(())
}

/// 解析并执行命令，直接访问共享 ShardedDb
fn execute_command(frame: Frame, ctx: &Context) -> Frame {
    let cmd = match Command::from_frame(frame) {
        Ok(cmd) => cmd,
        Err(e) => return Frame::Error(e.to_string()),
    };

    if let Command::Keys(keys) = cmd {
        keys.execute(ctx)
    } else {
        Command::execute(cmd, ctx)
    }
}
