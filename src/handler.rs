use anyhow::{Result, anyhow};
use bytes::Bytes;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};
use tracing::{error, info};

use crate::command::Command;
use crate::connection::Connection;
use crate::context::Context;
use crate::frame::Frame;

/// 处理一个客户端连接的完整生命周期
pub async fn handle_connection(socket: TcpStream, context: Arc<Context>) -> Result<()> {
    let peer_addr = socket.peer_addr()?;
    info!("Client connected: {}", peer_addr);

    let mut conn = Connection::new(socket);
    loop {
        let mut frames = Vec::new();
        match conn.read_frame().await? {
            Some(frame) => frames.push(frame),
            None => break,
        }
        // 批量读取缓冲区中已有的 frame，提升吞吐量
        while let Some(frame) = conn.try_read_frame()? {
            frames.push(frame);
        }
        let mut aof_entry_vec = Vec::with_capacity(frames.len());
        for ele in frames {
            match Command::from_frame(ele) {
                Ok(cmd) => {
                    if let Command::Subscribe(sub) = &cmd {
                        handle_subscribe(context.clone(), &mut conn, sub.channels().clone())
                            .await?;
                        return Ok(());
                    }
                    // Keys 命令需要 async 执行（内部用了 spawn_blocking）
                    if let Command::Keys(keys) = cmd {
                        let resp = keys.execute(&context).await;
                        conn.encode_to_buffer(&resp)?;
                        continue;
                    }
                    // Scan 命令需要 async 执行（内部用了 spawn_blocking）
                    if let Command::Scan(scan) = cmd {
                        let resp = scan.execute(&context).await;
                        conn.encode_to_buffer(&resp)?;
                        continue;
                    }
                    // BPop 命令需要 async 执行（阻塞等待数据）
                    if let Command::BPop(bpop) = cmd {
                        // blpop 可能阻塞，先把之前积攒的 aof 和响应刷出去
                        context.aof_send_batch(std::mem::take(&mut aof_entry_vec)).await?;
                        conn.write_and_flush().await?;
                        let resp = bpop.execute(&context).await;
                        conn.encode_to_buffer(&resp)?;
                        conn.write_and_flush().await?;
                        continue;
                    }
                    let (resp, aof_entry) = Command::execute(cmd, &context);
                    if let Some(entry) = aof_entry {
                        aof_entry_vec.push(entry);
                    }
                    conn.encode_to_buffer(&resp)?;
                }
                Err(err) => {
                    conn.encode_to_buffer(&Frame::Error(Bytes::from(err.to_string())))?;
                }
            }
        }
        context.aof_send_batch(aof_entry_vec).await?;
        conn.write_and_flush().await?;
    }

    info!("Client disconnected: {}", peer_addr);
    Ok(())
}

/// 处理订阅模式：持续监听消息推送和客户端命令
async fn handle_subscribe(
    ctx: Arc<Context>,
    conn: &mut Connection,
    channels: Vec<String>,
) -> Result<()> {
    let mut streams = StreamMap::new();
    for channel in channels {
        subscribe_channel(&ctx, conn, &mut streams, &channel).await?;
    }

    loop {
        tokio::select! {
            Some((channel_name, result)) = streams.next() => {
                match result {
                    Ok(msg) => {
                        let response = Frame::Array(vec![
                            Frame::BulkString(Bytes::from_static(b"message")),
                            Frame::BulkString(Bytes::from(channel_name)),
                            Frame::BulkString(msg),
                        ]);
                        conn.encode_to_buffer(&response)?;
                        conn.write_and_flush().await?;
                    }
                    Err(_) => {
                        // Lagged 错误：消费太慢，丢失了部分消息
                    }
                }
            }
            frame = conn.read_frame() => {
                match frame {
                    Ok(Some(frame)) => {
                        handle_subscribe_command(frame, &ctx, conn, &mut streams).await?;
                        if streams.is_empty() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        error!("connection error in subscribe mode: {}", err);
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

/// 处理订阅模式下收到的客户端命令
async fn handle_subscribe_command(
    frame: Frame,
    ctx: &Arc<Context>,
    conn: &mut Connection,
    streams: &mut StreamMap<String, BroadcastStream<Bytes>>,
) -> Result<()> {
    let command = Command::from_frame(frame)?;
    match &command {
        Command::Subscribe(cmd) => {
            for channel in cmd.channels() {
                subscribe_channel(ctx, conn, streams, channel).await?;
            }
        }
        Command::Unsubscribe(cmd) => {
            for channel in cmd.channels() {
                streams.remove(channel);
                send_ack(conn, channel, ctx, false).await?;
            }
        }
        Command::Ping(_) => {
            let (resp, _) = Command::execute(command, ctx);
            conn.encode_to_buffer(&resp)?;
            conn.write_and_flush().await?;
        }
        _ => {
            return Err(anyhow!(
                "ERR only SUBSCRIBE, UNSUBSCRIBE and PING are allowed in subscribe mode"
            ));
        }
    }
    Ok(())
}

/// 订阅单个频道并发送确认
async fn subscribe_channel(
    ctx: &Arc<Context>,
    conn: &mut Connection,
    streams: &mut StreamMap<String, BroadcastStream<Bytes>>,
    channel: &str,
) -> Result<()> {
    let rx = ctx.pub_sub().subscribe(channel);
    streams.insert(channel.to_string(), BroadcastStream::new(rx));
    send_ack(conn, channel, ctx, true).await
}

/// 发送订阅/取消订阅的确认帧
async fn send_ack(
    conn: &mut Connection,
    channel: &str,
    ctx: &Arc<Context>,
    is_subscribe: bool,
) -> Result<()> {
    let action = if is_subscribe {
        Bytes::from_static(b"subscribe")
    } else {
        Bytes::from_static(b"unsubscribe")
    };
    let response = Frame::Array(vec![
        Frame::BulkString(action),
        Frame::BulkString(Bytes::from(channel.to_string())),
        Frame::Integer(ctx.pub_sub().get_channel_count(channel) as i64),
    ]);
    conn.encode_to_buffer(&response)?;
    conn.write_and_flush().await?;
    Ok(())
}
