use bytes::{BufMut, BytesMut};

#[derive(Debug)]
pub enum Frame {
    // 简单字符串
    SimpleString(String),
    // 错误
    Error(String),
    // 数字
    Integer(i64),
    // 批量字符串
    BulkString(Vec<u8>),
    // 数组
    Array(Vec<Frame>),

    Null,
}

const CRLF: &[u8] = b"\r\n";

const CRLF_LEN: usize = 2;

impl Frame {
    fn encode_length(&self) -> usize {
        match self {
            Frame::SimpleString(s) | Frame::Error(s) => 1 + s.len() + CRLF_LEN,
            Frame::Integer(i) => 1 + Frame::count_decimal_digits(*i) + CRLF_LEN,
            Frame::Null => 5,
            Frame::BulkString(u) => {
                1 + Frame::count_decimal_digits(u.len() as i64) + CRLF_LEN + u.len() + CRLF_LEN
            }
            Frame::Array(arr) => {
                let mut header_length =
                    1 + Frame::count_decimal_digits(arr.len() as i64) + CRLF_LEN;
                for x in arr {
                    header_length += x.encode_length()
                }
                header_length
            }
        }
    }

    fn count_decimal_digits(n: i64) -> usize {
        if n == 0 {
            return 1;
        }

        let mut count = 0;
        let mut val = n;

        // 1. 处理负号：如果是负数，长度加 1，并转为正数处理
        if n < 0 {
            count += 1;
            // 特殊处理 i64::MIN，因为它转不成正的 i64
            if n == i64::MIN {
                return 20; // "-9223372036854775808"
            }
            val = -n;
        }

        // 2. 使用 ilog10 计算位数
        count += (val.ilog10() as usize) + 1;
        count
    }

    pub fn encode(& self, buf: &mut BytesMut) {
        buf.reserve(self.encode_length());
        self.encode_inner(buf);
    }

    fn encode_inner(& self, buf: &mut BytesMut) {
        match self {
            Frame::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_bytes());
                buf.put_slice(CRLF);
            }

            Frame::Error(s) => {
                buf.put_u8(b'-');
                buf.put_slice(s.as_bytes());
                buf.put_slice(CRLF);
            }
            Frame::Integer(i) => {
                // 不适用to_string、as_bytes()这样会多一次堆分配
                buf.put_u8(b':');
                // 使用itoa高性能数字转字节
                buf.put_slice(itoa::Buffer::new().format(*i).as_bytes());
                buf.put_slice(CRLF);
            }
            Frame::BulkString(b) => {
                buf.put_u8(b'$');
                buf.put_slice(itoa::Buffer::new().format(b.len()).as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(b);
                buf.put_slice(CRLF);
            }
            Frame::Null => {
                buf.put_slice(b"$-1\r\n");
            }
            Frame::Array(frames) => {
                buf.put_u8(b'*');
                buf.put_slice(itoa::Buffer::new().format(frames.len()).as_bytes());
                buf.put_slice(CRLF);
                for frame in frames {
                    frame.encode_inner(buf);
                }
            }
        }
    }
}
