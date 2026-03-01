use tokio_util::codec::Decoder;

pub enum Frame {
    // 简单字符串
    SimpleString(String),
    // 错误
    Error(String),
    // 数字
    Integer(u64),
    // 批量字符串
    BulkString(Vec<u8>),
    // 数组
    Array(Vec<Frame>),

    Null
}