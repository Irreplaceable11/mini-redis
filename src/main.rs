use std::net::SocketAddr;
use anyhow::Result;
use monoio::net::TcpListener;
use monoio::RuntimeBuilder;
use socket2::{Domain, Socket, Type};
use time::format_description;
use time::macros::offset;
use tracing::info;
use tracing_subscriber::fmt;


fn main() -> Result<()> {
    init_log();
    RuntimeBuilder::<monoio::FusionDriver>::new()
        .with_entries(32768)
        .enable_timer()
        .build()?
        .block_on(async {
            let tcp_listener = TcpListener::bind("0.0.0.0:6377").unwrap();
            info!("Mini Redis Listening on: {}", tcp_listener.local_addr().unwrap());
            loop {
                let in_coming = tcp_listener.accept().await;
                match in_coming {
                    Ok((socket, addr)) => {
                        println!("accepted a connection from {}", addr);
                    }
                    Err(err) => {}
                }
            }
        });

   Ok(())
}

fn create_reuse_port_listener(addr: SocketAddr) -> Result<TcpListener> {
    // 1. 创建底层 Socket
    let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let socket = Socket::new(domain, Type::STREAM, None)?;

    // 2. 设置关键选项
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?; // 核心：允许端口复用
    socket.set_nonblocking(true)?; // 异步必须是非阻塞

    // 3. 绑定并监听
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    // 4. 转换成 monoio 的 TcpListener
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
