
use crate::{command::{extract_u32, parse::extract_u64}, frame::Frame};
use anyhow::{anyhow, Result};

use crate::command::{extract_bytes, extract_string};

pub struct Set {
    pub key: String,

    pub value: Vec<u8>,

    pub ttl: Option<Expiration>,

    pub nx: Option<bool>,

    pub xx: Option<bool>
}

enum Expiration {
    Seconds(u64),
    Milliseconds(u64),
}
impl Set {
    pub fn new(key: String, value: Vec<u8>, ttl: Option<Expiration>, nx: Option<bool>, xx: Option<bool>) -> Self {
        Set { key, value, ttl , nx, xx}
    }

    pub fn parse(args: &[Frame]) -> Result<Set> {
        if args.len() < 2 {
            return Err(anyhow!("ERR wrong number of arguments for 'set' command"));
        }

        let key = extract_string(&args[0])?;

        let value = extract_bytes(&args[1])?;
        // for循环提取参数
        for arg in &args[2..args.len() -1] {
            let ttl_type = extract_string(arg)?;
            if ttl_type.to_uppercase() == "EX" {
                
            }
        }

        let ttl = match args.len() {
            // 只有key value 永不过期
            2 => None,
            4 => {
                match extract_u64(&args[3]) {
                    Ok(ttl) => Some(ttl),
                    Err(_) => return Err(anyhow!("Err ttl for 'set' command"))
                }
            }
            _ => return Err(anyhow!("ERR wrong number of arguments for 'set' command"))
        };

        Ok(Set::new(key, value, ttl, Some(false), Some(true)))
    }
}