use anyhow::Result;
use monoio::net::TcpListener;
use monoio::RuntimeBuilder;
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

fn init_log() {
    let timer_format = format_description::parse(
        "[year]-[month padding:zero]-[day padding:zero] [hour]:[minute]:[second]",
    )
    .expect("时间格式字符串无效");
    let timer = fmt::time::OffsetTime::new(offset!(+8), timer_format);
    tracing_subscriber::fmt().with_timer(timer).init();
}
