use crate::frame::Frame;
use anyhow::{anyhow, Result};

/// 命令解析辅助函数集合
/// 用于从 Frame 中提取各种类型的数据

/// 从 Frame 中提取字符串
/// 用于解析命令参数中的字符串类型（如 key、选项名等）
pub fn extract_string(frame: &Frame) -> Result<String> {
    match frame {
        Frame::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes)?.to_string();
            Ok(s)
        }
        _ => Err(anyhow!("ERR expect BulkString, got other frame type")),
    }
}

/// 从 Frame 中提取字节数组
/// 用于解析二进制数据（如 Redis 的 value）
pub fn extract_bytes(frame: &Frame) -> Result<Vec<u8>> {
    match frame {
        Frame::BulkString(bytes) => Ok(bytes.clone()),
        _ => Err(anyhow!("ERR expect BulkString")),
    }
}

/// 从 Frame 中提取 u32 类型的整数
/// 用于解析 TTL、超时时间等参数
pub fn extract_u32(frame: &Frame) -> Result<u32> {
    match frame {
        Frame::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes)?;
            s.parse::<u32>()
                .map_err(|_| anyhow!("ERR value is not an integer or out of range"))
        }
        Frame::Integer(i) => {
            if *i < 0 || *i > u32::MAX as i64 {
                return Err(anyhow!("ERR value is not an integer or out of range"));
            }
            Ok(*i as u32)
        }
        _ => Err(anyhow!("ERR value is not an integer or out of range")),
    }
}

/// 从 Frame 中提取 i64 类型的整数
/// 用于解析各种整数参数
pub fn extract_i64(frame: &Frame) -> Result<i64> {
    match frame {
        Frame::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes)?;
            s.parse::<i64>()
                .map_err(|_| anyhow!("ERR value is not an integer or out of range"))
        }
        Frame::Integer(i) => Ok(*i),
        _ => Err(anyhow!("ERR value is not an integer or out of range")),
    }
}

/// 从 Frame 中提取 usize 类型的整数
/// 用于解析数组索引、长度等参数
pub fn extract_usize(frame: &Frame) -> Result<usize> {
    match frame {
        Frame::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes)?;
            s.parse::<usize>()
                .map_err(|_| anyhow!("ERR value is not an integer or out of range"))
        }
        Frame::Integer(i) => {
            if *i < 0 {
                return Err(anyhow!("ERR value is not an integer or out of range"));
            }
            Ok(*i as usize)
        }
        _ => Err(anyhow!("ERR value is not an integer or out of range")),
    }
}

/// 从 Frame 中提取 u64 类型的整数
/// 用于解析毫秒级时间戳、大数值等参数
pub fn extract_u64(frame: &Frame) -> Result<u64> {
    match frame {
        Frame::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes)?;
            s.parse::<u64>()
                .map_err(|_| anyhow!("ERR value is not an integer or out of range"))
        }
        Frame::Integer(i) => {
            if *i < 0 {
                return Err(anyhow!("ERR value is not an integer or out of range"));
            }
            Ok(*i as u64)
        }
        _ => Err(anyhow!("ERR value is not an integer or out of range")),
    }
}
