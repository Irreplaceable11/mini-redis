use crate::frame::Frame;
use anyhow::Result;
use atoi::atoi;
use bytes::{Buf, Bytes, BytesMut};
use memchr::memmem;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
    write_buffer: BytesMut,
}

const MAX_BULK_LENGTH: usize = 512 * 1024 * 1024;

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(32 * 1024),
            write_buffer: BytesMut::with_capacity(32 * 1024),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            match self.try_read_frame()? {
                Some(frame) => return Ok(Some(frame)),
                None => {
                    let n = self.stream.read_buf(&mut self.buffer).await?;
                    if n == 0 {
                        return if self.buffer.is_empty() {
                            Ok(None)
                        } else {
                            Err(anyhow::anyhow!("Connection closed with incomplete frame"))
                        };
                    }
                }
            }
        }
    }

    pub fn encode_to_buffer(&mut self, frame: &Frame) -> Result<()> {
        frame.encode(&mut self.write_buffer);
        Ok(())
    }

    pub async fn write_and_flush(&mut self) -> Result<()> {
        self.stream.write_all(&self.write_buffer).await?;
        self.write_buffer.clear();
        Ok(())
    }

    pub fn find_crlf(buf: &[u8]) -> Option<usize> {
        memmem::find(buf, b"\r\n").map(|pos| pos + 2)
    }

    // ==================== 第一阶段：检查帧完整性（Cursor，只读不消费） ====================

    /// RESP 协议类型标记 (第一个字节)
    ///
    /// '+' : 简单字符串 (Simple String) -> +OK\r\n
    /// '-' : 错误 (Error)             -> -ERR message\r\n
    /// ':' : 整数 (Integer)           -> :100\r\n
    /// '$' : 批量字符串 (Bulk String) -> $5\r\nhello\r\n (先读长度，再读数据)
    /// '*' : 数组 (Array)             -> *2\r\n... (先读个数，再读元素)
    pub fn try_read_frame(&mut self) -> Result<Option<Frame>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        // ==================== 两阶段方案（已注释） ====================
        //第一阶段：用 Cursor 检查帧是否完整，拿到总长度
        let len = match self.check_frame_complete() {
            Ok(Some(len)) => len,
            Ok(None) => return Ok(None),
            Err(e) if e.to_string().contains("Incomplete") => return Ok(None),
            Err(e) => return Err(e),
        };
        // 第二阶段：从 buffer 中切出这段数据，做真正的解析
        let mut frame_buf = self.buffer.split_to(len).freeze();
        Self::parse_frame_bytes(&mut frame_buf, &mut 0).map(Some)
    }

    fn check_frame_complete(&self) -> Result<Option<usize>> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        self.check_frame_complete_inner(&mut cursor)
    }

    fn check_frame_complete_inner(&self, cursor: &mut Cursor<&[u8]>) -> Result<Option<usize>> {
        let type_byte = self.peek_u8(cursor)?;
        match type_byte {
            b'*' => self.check_array(cursor),
            b'$' => self.check_bulk_string(cursor),
            b'+' | b'-' | b':' => self.check_simple_type(cursor),
            _ => Err(anyhow::anyhow!("Unexpected frame type: {}", type_byte)),
        }
    }

    fn check_simple_type(&self, src: &mut Cursor<&[u8]>) -> Result<Option<usize>> {
        let _type_byte = src.get_u8(); // 消耗 '+' '-' ':'
        let start = src.position() as usize;
        let end = src.get_ref().len();
        match Self::find_crlf(&src.get_ref()[start..end]) {
            Some(len) => {
                src.set_position((start + len) as u64);
                Ok(Some(start + len))
            }
            None => Err(anyhow::anyhow!("Incomplete")),
        }
    }

    fn check_bulk_string(&self, src: &mut Cursor<&[u8]>) -> Result<Option<usize>> {
        let _type_byte = src.get_u8(); // 消耗 '$'

        // 第一步：读长度行
        let line = self.read_line(src)?;
        let length = std::str::from_utf8(&line[..line.len() - 2])?.parse::<isize>()?;

        if length == -1 {
            return Ok(Some(src.position() as usize));
        }

        let data_len = length as usize;
        if data_len > MAX_BULK_LENGTH {
            return Err(anyhow::anyhow!("Bulk string too large"));
        }
        // 第二步：确保数据体 + \r\n 都在缓冲区里
        let total_data_part = data_len + 2;
        if src.remaining() < total_data_part {
            return Err(anyhow::anyhow!("Incomplete"));
        }
        src.advance(total_data_part);
        Ok(Some(src.position() as usize))
    }

    fn check_array(&self, src: &mut Cursor<&[u8]>) -> Result<Option<usize>> {
        let _type_byte = src.get_u8(); // 消耗 '*'
        let line = self.read_line(src)?;
        let length = std::str::from_utf8(&line[..line.len() - 2])?.parse::<isize>()?;

        if length == -1 {
            return Ok(Some(src.position() as usize));
        }
        let array_len = length as usize;
        for _ in 0..array_len {
            self.check_frame_complete_inner(src)?;
        }
        Ok(Some(src.position() as usize))
    }

    // --- Cursor 辅助方法（用于 check 阶段）---

    // 看一眼第一个字节，但不移动指针
    fn peek_u8(&self, src: &mut Cursor<&[u8]>) -> Result<u8> {
        if !src.has_remaining() {
            return Err(anyhow::anyhow!("Incomplete"));
        }
        let b = src.chunk()[0];
        Ok(b)
    }

    // 从 Cursor 中读取一行，直到 \r\n
    fn read_line<'a>(&self, src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
        let start = src.position() as usize;
        let end = src.get_ref().len();
        match Self::find_crlf(&src.get_ref()[start..end]) {
            Some(len) => {
                src.set_position((start + len) as u64);
                Ok(&src.get_ref()[start..start + len])
            }
            None => Err(anyhow::anyhow!("Incomplete")),
        }
    }

    fn parse_frame_bytes(src: &Bytes, pos: &mut usize) -> Result<Frame> {
        let type_byte = src[*pos];
        *pos += 1;
        match type_byte {
            b'$' => Self::parse_bulk_string_bytes(src, pos),
            b'*' => Self::parse_array_bytes(src, pos),
            _ => Err(anyhow::anyhow!("Unexpected frame type: {}", type_byte)),
        }
    }

    fn parse_array_bytes(src: &Bytes, pos: &mut usize) -> Result<Frame> {
        let remaining = &src[*pos..];
        let crlf_pos = Self::find_crlf(remaining).ok_or_else(|| anyhow::anyhow!("Incomplete"))?;
        let length = atoi::<usize>(&remaining[..crlf_pos - 2])
            .ok_or_else(|| anyhow::anyhow!("parse error"))?;
        *pos += crlf_pos;
        let mut frames = Vec::with_capacity(length);
        for _ in 0..length {
            frames.push(Self::parse_frame_bytes(src, pos)?);
        }
        Ok(Frame::Array(frames))
    }

    fn parse_bulk_string_bytes(src: &Bytes, pos: &mut usize) -> Result<Frame> {
        let remaining = &src[*pos..];
        let crlf_pos = Self::find_crlf(remaining).ok_or_else(|| anyhow::anyhow!("Incomplete"))?;

        // 直接从 remaining 里取长度数字（去掉 \r\n）
        let data_len = atoi::<usize>(&remaining[..crlf_pos - 2])
            .ok_or_else(|| anyhow::anyhow!("Incomplete"))?;
        *pos += crlf_pos; // 跳过长度行

        // 零拷贝切出数据体
        let start = *pos;
        let end = start + data_len;
        *pos = end + 2; // 跳过数据 + \r\n
        Ok(Frame::BulkString(src.slice(start..end)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bulk_string_bytes() {
        // "$3\r\nSET\r\n" — 类型字节 '$' 在外面消费，pos 从 1 开始
        let src = Bytes::from("$3\r\nSET\r\n");
        let mut pos = 1; // 跳过 '$'
        let frame = Connection::parse_bulk_string_bytes(&src, &mut pos).unwrap();
        println!("bulk_string: {:?}", frame);
        println!("pos after parse: {}", pos);
    }

    #[test]
    fn test_parse_array_bytes() {
        // "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        let src = Bytes::from("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        let mut pos = 0;
        // parse_array_bytes 需要跳过 '*'，但它内部不消费类型字节
        // 所以需要从 parse_frame_bytes 入口进
        let frame = Connection::parse_frame_bytes(&src, &mut pos).unwrap();
        println!("array: {:?}", frame);
        println!("pos after parse: {}", pos);
    }
}
