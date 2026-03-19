use std::cell::RefCell;
use std::rc::Rc;

use anyhow::Result;
use monoio::net::TcpStream;
use tracing::info;

use crate::command::Command;
use crate::connection::Connection;
use crate::context::Context;
use crate::dispatcher::CoreBus;
use crate::frame::Frame;

/// 处理一个客户端连接的完整生命周期
pub async fn handle_connection(
    socket: TcpStream,
    context: Rc<RefCell<Context>>,
    bus: Rc<CoreBus>,
) -> Result<()> {
    let peer_addr = socket.peer_addr()?;
    info!("Client connected: {} on core {}", peer_addr, bus.my_core_id);

    let mut conn = Connection::new(socket);
    loop {
        let mut frames = Vec::new();
        match conn.read_frame().await? {
            Some(frame) => frames.push(frame),
            None => break,
        }
        // 批量读取缓冲区中已有的 frame
        while let Some(frame) = conn.try_read_frame()? {
            frames.push(frame);
        }

        for frame in frames {
            let response = execute_with_routing(
                frame,
                &context,
                &bus,
            );
            conn.encode_to_buffer(&response)?;
        }
        conn.write_and_flush().await?;
    }

    info!("Client disconnected: {}", peer_addr);
    Ok(())
}

/// 带路由的命令执行：
/// 1. 解析命令，提取 key
/// 2. 根据 key hash 判断目标核心
/// 3. 本地 key -> 直接执行
/// 4. 远程 key -> 转发到目标核心，同步等待结果
fn execute_with_routing(
    frame: Frame,
    ctx: &Rc<RefCell<Context>>,
    bus: &Rc<CoreBus>,
) -> Frame {
    // 先解析命令
    let cmd = match Command::from_frame(frame.clone()) {
        Ok(cmd) => cmd,
        Err(e) => return Frame::Error(e.to_string()),
    };

    // 提取命令的 key（如果有的话）
    let key = cmd.extract_key();

    match key {
        Some(ref k) => {
            let target_core = bus.route_key(k);
            if target_core == bus.my_core_id {
                // 本地执行
                execute_local(cmd, &mut ctx.borrow_mut())
            } else {
                // 远程转发：发送原始 frame 到目标核心，阻塞等待结果
                let resp = bus.send_remote(target_core, frame);
                // 这里用 crossbeam 的 recv() 阻塞等待
                // 因为目标核心处理完会 notify 我们，但我们当前在同步上下文中
                // 对于 mini-redis 学习项目，这个阻塞时间极短（微秒级）
                match resp.reply_rx.recv() {
                    Ok(result) => result,
                    Err(_) => Frame::Error("ERR remote execution failed".to_string()),
                }
            }
        }
        None => {
            // 无 key 的命令（如 PING），本地执行
            execute_local(cmd, &mut ctx.borrow_mut())
        }
    }
}

fn execute_local(cmd: Command, ctx: &mut Context) -> Frame {
    if let Command::Keys(keys) = cmd {
        keys.execute(ctx)
    } else {
        Command::execute(cmd, ctx)
    }
}
