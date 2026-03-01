use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use anyhow::Result;

pub struct Connection {
    stream: TcpStream,

    buffer: BytesMut,
}
impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096)
        }
    }

    pub async fn read_frame(&mut self) -> Result<()>{
        self.stream.read_buf(&mut self.buffer).await?;

        Ok(())
    }
}