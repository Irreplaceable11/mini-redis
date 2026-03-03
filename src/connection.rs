use crate::frame::Frame;
use anyhow::Result;
use bytes::{Buf, BytesMut};
use memchr::memmem;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    stream: TcpStream,

    buffer: BytesMut,
}

const MAX_BULK_LENGTH: usize = 512 * 1024 * 1024;
impl Connection {
    // 初始化connection
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096)
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>>{
        // 循环读取stream数据，直接获取整个frame
        loop {
            match self.try_read_frame()? {
                Some(frame) => return Ok(Some(frame)),
                None => {
                    let n = self.stream.read_buf(&mut self.buffer).await?;
                    // 读不到说明客户端以关闭
                    if n == 0 {
                        // 读取到 0 字节，说明对方关闭了连接
                        return if self.buffer.is_empty() {
                            Ok(None)
                        } else {
                            Err(anyhow::anyhow!("Connection closed with incomplete frame"))
                        }
                    }
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        // 编码到 buffer
        frame.encode(&mut self.buffer);
        // 写出所有数据
        self.stream.write_all(&self.buffer).await?;
        // 清空 buffer，为下次写入做准备
        self.buffer.clear();
        Ok(())
    }

    pub fn find_crlf(buf: &[u8]) -> Option<usize> {
       /* windows(2) 创建一个滑动窗口，每次看2个字节
        比如 b"+OK\r\n" -> 窗口序列: [b'+', b'O'], [b'O', b'K'], [b'K', b'\r'], [b'\r', b'\n']
        buf.windows(2)
            .position(|window| window == b"\r\n")
            .map(|pos| pos + 2)*/
        // 经过比较 选用memmem使用SIMD优化，因为windows会创建大量切片
        // 查找 \r\n 并返回包含 \r\n 的长度
        memmem::find(buf, b"\r\n").map(|pos| pos + 2)
    }

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

        let mut src = Cursor::new(&self.buffer[..]);
        // let type_byte = self.buffer[0];
        // 如果不够解析最小头部，返回 Ok(None)
        match self.parse_frame(&mut src) {
           Ok(frame) => {
               let len = src.position() as usize;
               // 现在才真正地从 buffer 中移除数据
               self.buffer.advance(len);
               Ok(Some(frame))
           },
            Err(err) => {
                // Incomplete表示数据不全
                if err.to_string().contains("Incomplete") {
                    return Ok(None);
                }
                Err(err)
            }
        }
    }

    fn parse_frame(& self, src: &mut Cursor<&[u8]>) -> Result<Frame> {
        let type_byte = self.peek_u8(src)?;
        match type_byte {
            b'+' | b'-' | b':' => self.do_parse_simple_type(src),
            b'$' => self.do_parse_bulk_string(src),
            b'*' => self.do_parse_array(src), // 递归入口
            _ => Err(anyhow::anyhow!("Unexpected frame type: {}", type_byte)),
        }
    }

    // 看一眼第一个字节，但不移动指针
    fn peek_u8(&self, src: &mut Cursor<&[u8]>) -> Result<u8> {
        if !src.has_remaining() {
            return Err(anyhow::anyhow!("Incomplete"));
        }
        let b = src.chunk()[0]; // chunk() 返回当前指针指向的数据
        Ok(b)
    }

    // 从 Cursor 中读取一行，直到 \r\n
    fn read_line<'a>(&self, src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
        // 获取当前位置之后的所有剩余数据
        let start = src.position() as usize;
        let end = src.get_ref().len();

        // 在剩余数据中找 CRLF
        match Self::find_crlf(&src.get_ref()[start..end]) {
            Some(len) => {
                // 找到了！移动 Cursor 的位置
                src.set_position((start + len) as u64);
                // 返回这一行的切片
                Ok(&src.get_ref()[start..start + len])
            }
            None => Err(anyhow::anyhow!("Incomplete")), // 数据还没传完
        }
    }


    fn do_parse_simple_type(& self, src: &mut Cursor<&[u8]>) -> Result<Frame> {
        // 1. 获取当前字节（类型标识），Cursor 后移 1 位
        let type_byte = src.get_u8();

        // 2. 直接读取剩余的一整行（包含 \r\n）
        // 注意：这里的 read_line 是基于当前 Cursor 位置开始读的
        let line = self.read_line(src)?;

        // 3. 这里的 line 是由 read_line 返回的截断切片，已经排除了类型字节
        // 但它结尾还带着 \r\n，所以需要去掉最后两个字节
        if line.len() < 2 {
            return Err(anyhow::anyhow!("Invalid simple type frame"));
        }

        let content = &line[..line.len() - 2];
        let string = std::str::from_utf8(content)?.to_string();

        match type_byte {
            b'+' => Ok(Frame::SimpleString(string)),
            b'-' => Ok(Frame::Error(string)),
            b':' => Ok(Frame::Integer(string.parse()?)),
            _ => unreachable!(), // parse_frame 已经过滤了类型
        }


        // match Self::find_crlf(&self.buffer) {
        //     Some(pos) => {
        //         let type_byte = self.buffer[0];
        //         // 健壮性检查：防止空行或过短帧
        //         // 最小合法帧: "+\r\n" (长度3).
        //         // end_pos 指向 \n 之后，所以 end_pos 至少应该是 3.
        //         if pos < 3 {
        //             return Err(anyhow::anyhow!("Invalid frame: too short"));
        //         }
        //         // 从缓冲区提取数据
        //         let line = self.buffer.split_to(pos);
        //         // 去掉 + \r\n
        //         let content = &line[1..line.len() - 2];
        //         let string = str::from_utf8(content)?.to_string();
        //         match type_byte {
        //             b'+' => Ok(Frame::SimpleString(string)) ,
        //             b'-' => Ok(Frame::Error(string)),
        //             b':' => Ok(Frame::Integer(string.parse()?)),
        //             _ => unreachable!()
        //         }
        //     },
        //     None => {Err(anyhow::anyhow!("Incomplete frame"))}
        // }
    }

    fn do_parse_bulk_string (& self,  src: &mut Cursor<&[u8]>) -> Result<Frame> {
        let _type_byte = src.get_u8(); // 消耗 '$'

        // 第一步：读长度行
        let line = self.read_line(src)?;
        let length = std::str::from_utf8(&line[..line.len() - 2])?.parse::<isize>()?;

        if length == -1 {
            return Ok(Frame::Null);
        }

        let data_len = length as usize;
        if data_len > MAX_BULK_LENGTH {
            return Err(anyhow::anyhow!("Bulk string too large"));
        }

        // 第二步：确保数据体 + \r\n 都在缓冲区里
        let total_data_part = data_len + 2; // 数据加上结尾的 \r\n
        if src.remaining() < total_data_part {
            return Err(anyhow::anyhow!("Incomplete")); // 触发外层回滚
        }

        // 提取数据（不包含最后的 \r\n）
        let start = src.position() as usize;
        let data = src.get_ref()[start..start + data_len].to_vec();

        // 记得手动把 Cursor 挪过这段数据和最后的 \r\n
        src.advance(total_data_part);

        Ok(Frame::BulkString(data))
        // match Self::find_crlf(&self.buffer) {
        //     Some(pos) => {
        //         let line = &self.buffer[0..pos];
        //         // 最小 "$-1\r\n"占四字节
        //         if line.len() < 4 {
        //             return Err(anyhow::anyhow!("Invalid bulk string header"));
        //         }
        //         // 获取长度字符串
        //         let length_str = &line[1..line.len() - 2];
        //         let length = str::from_utf8(length_str)?.parse::<isize>()?;
        //         // -1说明为null
        //         if length == -1 {
        //             self.buffer.advance(pos);  // 消耗头部
        //             return Ok(Some(Frame::Null));
        //         }
        //         // 拒绝负数
        //         if length < 0 {
        //             return Err(anyhow::anyhow!("Invalid bulk string length:{}", length));
        //         }
        //
        //         let data_length = length as usize;
        //         if data_length > MAX_BULK_LENGTH {
        //             return Err(anyhow::anyhow!("Bulk string too large"));
        //         }
        //
        //         // 总长度 = 头部长度(pos) + 数据长度 + 结尾的 \r\n (2字节)
        //         let total_len = pos + data_length + 2;
        //         if self.buffer.len() < total_len {
        //             return Ok(None);// 数据还没传完
        //         }
        //
        //         let data_with_header = self.buffer.split_to(total_len);
        //         let vec = data_with_header[pos..pos + data_length].to_vec();
        //
        //         Ok(Some(Frame::BulkString(vec)))
        //     },
        //     None => Ok(None),
        // }
    }

    fn do_parse_array (& self, src: & mut Cursor<&[u8]>) -> Result<Frame> {
        // 1. 读取数组长度行，例如 "*3\r\n"
        let line = self.read_line(src)?;
        let length = std::str::from_utf8(&line[1..line.len()-2])?.parse::<isize>()?;

        if length == -1 { return Ok(Frame::Null); }
        let array_len = length as usize;

        let mut frames = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            // 2. 关键递归：调用 parse_frame，传入同一个 src
            // 它会继续往后读，直到填满这个数组
            frames.push(self.parse_frame(src)?);
        }

        Ok(Frame::Array(frames))
        // match Self::find_crlf(&self.buffer) {
        //     Some(pos) => {
        //         let line = &self.buffer[0..pos];
        //         let len_str = &line[1..line.len() - 2];
        //
        //         let length = std::str::from_utf8(&len_str)?.parse::<isize>()?;
        //
        //         if length == -1 {
        //             self.buffer.advance(pos);  // 消耗头部
        //             return Ok(Some(Frame::Null));
        //         }
        //         // 去除非法请求
        //         if length < 0 {
        //             return Err(anyhow::anyhow!("Invalid array length:{}", length));
        //         }
        //         let array_length = length as usize;
        //         if array_length > MAX_BULK_LENGTH {
        //             return Err(anyhow::anyhow!("Bulk string too large"));
        //         }
        //
        //         let mut frames: Vec<Frame> = Vec::with_capacity(array_length);
        //
        //         for _ in 0..array_length {
        //             //     处理每个类型
        //         }
        //
        //
        //         Ok(Some(Frame::Array(frames)))
        //     },
        //     None => Ok(None),
        // }
    }
}