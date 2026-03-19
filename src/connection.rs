use crate::frame::Frame;
use anyhow::Result;
use bytes::{Buf, BytesMut};
use memchr::memmem;
use std::io::{Cursor};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

pub struct Connection {
    stream: TcpStream,
    buffer: Option<BytesMut>,
    write_buffer: Option<BytesMut>,
}

const MAX_BULK_LENGTH: usize = 512 * 1024 * 1024;

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: Some(BytesMut::with_capacity(4096)),
            write_buffer: Some(BytesMut::with_capacity(4096)),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            match self.try_read_frame()? {
                Some(frame) => return Ok(Some(frame)),
                None => {
                    let (res, buf) = self.stream.read(self.buffer.take().expect("")).await;
                    self.buffer = Some(buf);
                    if res? == 0 {
                        return if self.buffer.as_ref().unwrap().is_empty() {
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
        frame.encode(&mut self.write_buffer.as_mut().unwrap());
        Ok(())
    }

    pub async fn write_and_flush(&mut self) -> Result<()>  {
        let (_i, buffer) = self.stream.write_all(self.write_buffer.take().unwrap()).await;
        self.write_buffer = Some(buffer);
        if let Some(buffer) = self.write_buffer.as_mut() {
            buffer.clear();
        }
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
        if self.buffer.as_ref().unwrap().is_empty() {
            return Ok(None);
        }

        // 第一阶段：用 Cursor 检查帧是否完整，拿到总长度
        let len = match self.check_frame_complete() {
            Ok(Some(len)) => len,
            Ok(None) => return Ok(None),
            Err(e) if e.to_string().contains("Incomplete") => return Ok(None),
            Err(e) => return Err(e),
        };

        // 第二阶段：从 buffer 中切出这段数据，做真正的解析
        // split_to 把 buffer 一分为二，前半段是独立的 BytesMut
        let mut frame_buf = self.buffer.as_mut().unwrap().split_to(len);
        Self::parse_frame(&mut frame_buf).map(Some)
    }

    fn check_frame_complete(&self) -> Result<Option<usize>> {
        let mut cursor = Cursor::new(&self.buffer.as_ref().unwrap()[..]);
        self.check_frame_complete_inner(&mut cursor)
    }

    fn check_frame_complete_inner(&self, cursor: &mut Cursor<&[u8]>) -> Result<Option<usize>> {
        let type_byte = self.peek_u8(cursor)?;
        match type_byte {
            b'+' | b'-' | b':' => self.check_simple_type(cursor),
            b'$' => self.check_bulk_string(cursor),
            b'*' => self.check_array(cursor),
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

    // ==================== 第二阶段：真正解析（BytesMut，消费式，零拷贝） ====================
    // 此时帧数据已经通过 split_to 切出来了，可以放心地消费

    /// 从 BytesMut 中解析一个 Frame
    /// BytesMut 的 get_u8() 会消费数据（自动 advance），split_to() 零拷贝切割
    fn parse_frame(src: &mut BytesMut) -> Result<Frame> {
        let type_byte = src.get_u8(); // 消费类型字节
        match type_byte {
            b'+' | b'-' | b':' => Self::parse_simple_type(type_byte, src),
            b'$' => Self::parse_bulk_string(src),
            b'*' => Self::parse_array(src),
            _ => Err(anyhow::anyhow!("Unexpected frame type: {}", type_byte)),
        }
    }

    /// 解析简单类型：SimpleString / Error / Integer
    fn parse_simple_type(type_byte: u8, src: &mut BytesMut) -> Result<Frame> {
        // 找到 \r\n 的位置
        let crlf_pos = Self::find_crlf(src)
            .ok_or_else(|| anyhow::anyhow!("Incomplete"))?;

        // split_to 切出这一行（包含 \r\n），src 中剩余后面的数据
        let line = src.split_to(crlf_pos);
        // 去掉末尾的 \r\n
        let content = &line[..line.len() - 2];
        let string = std::str::from_utf8(content)?.to_string();

        match type_byte {
            b'+' => Ok(Frame::SimpleString(string)),
            b'-' => Ok(Frame::Error(string)),
            b':' => Ok(Frame::Integer(string.parse()?)),
            _ => unreachable!(),
        }
    }

    /// 解析 BulkString，这里是零拷贝的关键！
    fn parse_bulk_string(src: &mut BytesMut) -> Result<Frame> {
        // 读长度行
        let crlf_pos = Self::find_crlf(src)
            .ok_or_else(|| anyhow::anyhow!("Incomplete"))?;
        let header = src.split_to(crlf_pos);
        let length = std::str::from_utf8(&header[..header.len() - 2])?.parse::<isize>()?;

        if length == -1 {
            return Ok(Frame::Null);
        }

        let data_len = length as usize;

        // 零拷贝核心：split_to 切出数据部分，freeze 转成不可变的 Bytes
        let data = src.split_to(data_len).freeze();

        // 跳过结尾的 \r\n
        src.advance(2);

        Ok(Frame::BulkString(data))
    }

    /// 解析数组，递归调用 parse_frame
    fn parse_array(src: &mut BytesMut) -> Result<Frame> {
        let crlf_pos = Self::find_crlf(src)
            .ok_or_else(|| anyhow::anyhow!("Incomplete"))?;
        let header = src.split_to(crlf_pos);
        let length = std::str::from_utf8(&header[..header.len() - 2])?.parse::<isize>()?;

        if length == -1 {
            return Ok(Frame::Null);
        }

        let array_len = length as usize;
        let mut frames = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            frames.push(Self::parse_frame(src)?);
        }

        Ok(Frame::Array(frames))
    }
}
