use crate::frame::Frame;
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use memchr::memmem;
use std::io::{Cursor};
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

    /// 测试专用：直接用 buffer 构造，绕过 TcpStream
    #[cfg(test)]
    fn from_buffer(buffer: BytesMut) -> Self {
        use tokio::net::TcpListener;
        // 创建一个假的 TcpStream（测试中不会用到）
        let stream = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let (stream, _) = tokio::join!(
                    async { tokio::net::TcpStream::connect(addr).await.unwrap() },
                    async { listener.accept().await.unwrap().0 }
                );
                stream
            })
        };
        Connection {
            stream,
            buffer,
            write_buffer: BytesMut::new(),
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

    pub async fn write_and_flush(&mut self) -> Result<()>  {
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

        // // 第一阶段：用 Cursor 检查帧是否完整，拿到总长度
        // let len = match self.check_frame_complete() {
        //     Ok(Some(len)) => len,
        //     Ok(None) => return Ok(None),
        //     Err(e) if e.to_string().contains("Incomplete") => return Ok(None),
        //     Err(e) => return Err(e),
        // };

        // // 第二阶段：从 buffer 中切出这段数据，做真正的解析
        // // split_to 把 buffer 一分为二，前半段是独立的 BytesMut
        // let mut frame_buf = self.buffer.split_to(len);
        // Self::parse_frame(&mut frame_buf).map(Some)
        // 一遍扫描：既检查完整性，又直接解析
        let mut pos = 0;
        match self.parse_at(&mut pos) {
            Ok(Some(frame)) => {
                // pos 现在指向帧末尾，一次性推进 buffer
                self.buffer.advance(pos);
                Ok(Some(frame))
            }
            Ok(None) => Ok(None),  // 数据不完整
            Err(e) => Err(e),
        }
    }

    fn parse_at(&mut self, pos: &mut usize) -> Result<Option<Frame>> {
        if *pos >= self.buffer.len() {
            return Ok(None);
        }
        let type_byte = self.buffer[*pos];
        *pos += 1;  // 跳过类型字节
        match type_byte {
            b'+' | b'-' | b':' => self.parse_simple_at(type_byte, pos),
            b'$' => self.parse_bulk_string_at(pos),
            b'*' => self.parse_array_at(pos),
             _ => Err(anyhow::anyhow!("Unexpected frame type: {}", type_byte)),
        }
    }

    fn parse_simple_at(&mut self, type_byte: u8, pos: &mut usize) -> Result<Option<Frame>> {
        match Self::find_crlf(&self.buffer[*pos..]) {
            Some( curr_pos) => {
                let bytes_mut = Bytes::copy_from_slice(&self.buffer[*pos..*pos + curr_pos - 2]);
                *pos += curr_pos;//截取完再移动pos
                match type_byte {
                    b'+' => Ok(Some(Frame::SimpleString(bytes_mut))),
                    b'-' => Ok(Some(Frame::Error(bytes_mut))),
                    b':' => Ok(Some(Frame::Integer(str::from_utf8(&bytes_mut)?.parse()?))),
                    _ => unreachable!(),
                }

            }
            None => Ok(None),
        }
    }

    fn parse_bulk_string_at(&mut self, pos: &mut usize) -> Result<Option<Frame>> {
        // 读长度行，找 \r\n
        let crlf_pos = match Self::find_crlf(&self.buffer[*pos..]) {
            Some(p) => p,
            None => return Ok(None),
        };
        // 提取长度数字（去掉 \r\n）
        let length = std::str::from_utf8(&self.buffer[*pos..*pos + crlf_pos - 2])?.parse::<isize>()?;
        *pos += crlf_pos; // 跳过长度行 + \r\n

        if length == -1 {
            return Ok(Some(Frame::Null));
        }

        let data_len = length as usize;
        // 检查 buffer 里是否有足够的数据体 + \r\n
        if *pos + data_len + 2 > self.buffer.len() {
            return Ok(None);
        }
        // 切出数据体
        let data = Bytes::copy_from_slice(&self.buffer[*pos..*pos + data_len]);
        *pos += data_len + 2; // 跳过数据体 + \r\n
        Ok(Some(Frame::BulkString(data)))
    }
    
    fn parse_array_at(&mut self, pos: &mut usize) -> Result<Option<Frame>> {
        // 读元素个数行，找 \r\n
        let crlf_pos = match Self::find_crlf(&self.buffer[*pos..]) {
            Some(p) => p,
            None => return Ok(None),
        };
        let length = std::str::from_utf8(&self.buffer[*pos..*pos + crlf_pos - 2])?.parse::<isize>()?;
        *pos += crlf_pos; // 跳过个数行 + \r\n

        if length == -1 {
            return Ok(Some(Frame::Null));
        }

        let array_len = length as usize;
        let mut frames = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            // 递归解析每个元素，parse_at 会自动推进 pos
            match self.parse_at(pos)? {
                Some(frame) => frames.push(frame),
                None => return Ok(None), // 某个元素不完整，整个数组也不完整
            }
        }
        Ok(Some(Frame::Array(frames)))
    }

    fn check_frame_complete(&self) -> Result<Option<usize>> {
        let mut cursor = Cursor::new(&self.buffer[..]);
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
        let mut line = src.split_to(crlf_pos);
        // 去掉末尾的 \r\n
        line.truncate(line.len() - 2);

        match type_byte {
            b'+' => Ok(Frame::SimpleString(line.freeze())),
            b'-' => Ok(Frame::Error(line.freeze())),
            b':' => Ok(Frame::Integer(line.get_i64())),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 辅助函数：把字节塞进 Connection 的 buffer，调用 try_read_frame
    fn parse(data: &[u8]) -> Result<Option<Frame>> {
        let buf = BytesMut::from(data);
        let mut conn = Connection::from_buffer(buf);
        conn.try_read_frame()
    }

    // ==================== SimpleString ====================

    #[test]
    fn test_simple_string() {
        let frame = parse(b"+OK\r\n").unwrap().unwrap();
        match frame {
            Frame::SimpleString(s) => assert_eq!(s, &b"OK"[..]),
            _ => panic!("期望 SimpleString，得到 {:?}", frame),
        }
    }

    // ==================== Error ====================

    #[test]
    fn test_error() {
        let frame = parse(b"-ERR unknown\r\n").unwrap().unwrap();
        match frame {
            Frame::Error(s) => assert_eq!(s, &b"ERR unknown"[..]),
            _ => panic!("期望 Error，得到 {:?}", frame),
        }
    }

    // ==================== Integer ====================

    #[test]
    fn test_integer() {
        let frame = parse(b":42\r\n").unwrap().unwrap();
        match frame {
            Frame::Integer(n) => assert_eq!(n, 42),
            _ => panic!("期望 Integer，得到 {:?}", frame),
        }
    }

    // ==================== BulkString ====================

    #[test]
    fn test_bulk_string() {
        let frame = parse(b"$5\r\nhello\r\n").unwrap().unwrap();
        match frame {
            Frame::BulkString(s) => assert_eq!(s, &b"hello"[..]),
            _ => panic!("期望 BulkString，得到 {:?}", frame),
        }
    }

    #[test]
    fn test_bulk_string_null() {
        let frame = parse(b"$-1\r\n").unwrap().unwrap();
        assert!(matches!(frame, Frame::Null));
    }

    #[test]
    fn test_bulk_string_empty() {
        let frame = parse(b"$0\r\n\r\n").unwrap().unwrap();
        match frame {
            Frame::BulkString(s) => assert_eq!(s.len(), 0),
            _ => panic!("期望空 BulkString，得到 {:?}", frame),
        }
    }

    // ==================== Array ====================

    #[test]
    fn test_array_simple() {
        // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let frame = parse(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap().unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(&arr[0], Frame::BulkString(s) if s == &b"foo"[..]));
                assert!(matches!(&arr[1], Frame::BulkString(s) if s == &b"bar"[..]));
            }
            _ => panic!("期望 Array，得到 {:?}", frame),
        }
    }

    #[test]
    fn test_array_mixed_types() {
        // *3\r\n+OK\r\n:100\r\n$5\r\nhello\r\n
        let frame = parse(b"*3\r\n+OK\r\n:100\r\n$5\r\nhello\r\n").unwrap().unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert!(matches!(&arr[0], Frame::SimpleString(s) if s == &b"OK"[..]));
                assert!(matches!(&arr[1], Frame::Integer(100)));
                assert!(matches!(&arr[2], Frame::BulkString(s) if s == &b"hello"[..]));
            }
            _ => panic!("期望 Array，得到 {:?}", frame),
        }
    }

    #[test]
    fn test_array_null() {
        let frame = parse(b"*-1\r\n").unwrap().unwrap();
        assert!(matches!(frame, Frame::Null));
    }

    #[test]
    fn test_nested_array() {
        // *2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n
        let frame = parse(b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n").unwrap().unwrap();
        match frame {
            Frame::Array(outer) => {
                assert_eq!(outer.len(), 2);
                match &outer[0] {
                    Frame::Array(inner) => {
                        assert_eq!(inner.len(), 2);
                        assert!(matches!(&inner[0], Frame::Integer(1)));
                        assert!(matches!(&inner[1], Frame::Integer(2)));
                    }
                    _ => panic!("期望嵌套 Array"),
                }
            }
            _ => panic!("期望 Array，得到 {:?}", frame),
        }
    }

    // ==================== 不完整数据 ====================

    #[test]
    fn test_incomplete_simple_string() {
        // 没有 \r\n 结尾
        assert!(parse(b"+OK").unwrap().is_none());
    }

    #[test]
    fn test_incomplete_bulk_string_no_data() {
        // 有长度行但没有数据体
        assert!(parse(b"$5\r\n").unwrap().is_none());
    }

    #[test]
    fn test_incomplete_bulk_string_partial_data() {
        // 数据体不完整
        assert!(parse(b"$5\r\nhel").unwrap().is_none());
    }

    #[test]
    fn test_incomplete_array() {
        // 声明2个元素但只有1个
        assert!(parse(b"*2\r\n$3\r\nfoo\r\n").unwrap().is_none());
    }

    #[test]
    fn test_empty_buffer() {
        assert!(parse(b"").unwrap().is_none());
    }

    // ==================== buffer 推进验证 ====================

    #[test]
    fn test_buffer_advance_after_parse() {
        // buffer 里放两个帧，解析第一个后 buffer 应该只剩第二个
        let data = b"+OK\r\n+PONG\r\n";
        let buf = BytesMut::from(&data[..]);
        let mut conn = Connection::from_buffer(buf);

        let frame1 = conn.try_read_frame().unwrap().unwrap();
        assert!(matches!(frame1, Frame::SimpleString(s) if s == &b"OK"[..]));

        let frame2 = conn.try_read_frame().unwrap().unwrap();
        assert!(matches!(frame2, Frame::SimpleString(s) if s == &b"PONG"[..]));

        // buffer 应该空了
        assert!(conn.try_read_frame().unwrap().is_none());
    }
}
