# Mini-Redis

一个用 Rust 编写的高性能 Redis 服务器实现，用于学习 Rust 异步编程、并发设计和网络协议。实现了 RESP（Redis 序列化协议），支持核心 Redis 命令，注重零拷贝解析和高效内存管理。

## 架构设计

```
┌─────────────────────────────────────────────────┐
│                  TcpListener                    │
│              (tokio 异步 accept)                │
└──────────────────────┬──────────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   ┌─────────┐  ┌─────────┐  ┌─────────┐
   │ Handler │  │ Handler │  │ Handler │   每个连接一个异步 task
   └────┬────┘  └────┬────┘  └────┬────┘
        │            │            │
        ▼            ▼            ▼
   ┌─────────────────────────────────────┐
   │           Connection                │
   │  (RESP 解析 / 零拷贝编码)           │
   └─────────────────────────────────────┘
        │            │            │
        ▼            ▼            ▼
   ┌──────────────────────────────────────┐
   │         Context (Arc 共享)           │
   │  ┌──────────┐    ┌───────────────┐   │
   │  │    Db    │    │    PubSub     │   │
   │  │ 256分片  │    │  (broadcast)  │   │
   │  │ RwLock   │    │  DashMap      │   │
   │  └──────────┘    └───────────────┘   │
   └──────────────────────────────────────┘
```

- 多线程异步运行时（tokio），每个连接独立 task
- 256 分片存储，使用 `parking_lot::RwLock` 实现细粒度锁
- 基于 `bytes::BytesMut` 的零拷贝 RESP 帧解析（split/freeze）
- 批量读取 + 批量写入优化，提升 pipeline 模式吞吐量
- 后台 TTL 清理任务，轮询式分片扫描
- 发布/订阅基于 `tokio::sync::broadcast` 通道，由 `DashMap` 管理

## 支持的命令

| 命令 | 说明 |
|------|------|
| `PING [message]` | 连接测试，返回 PONG 或回显消息 |
| `SET key value [EX s] [PX ms] [NX\|XX]` | 设置键值，支持过期时间和条件写入 |
| `GET key` | 获取键对应的值 |
| `DEL key [key ...]` | 删除一个或多个键 |
| `EXISTS key [key ...]` | 检查键是否存在 |
| `EXPIRE key seconds` | 设置过期时间（秒） |
| `PEXPIRE key milliseconds` | 设置过期时间（毫秒） |
| `TTL key` | 查询剩余过期时间（秒） |
| `PTTL key` | 查询剩余过期时间（毫秒） |
| `KEYS pattern` | 按 glob 模式匹配键（通过 `spawn_blocking` + rayon 异步并行扫描） |
| `PUBLISH channel message` | 向频道发布消息 |
| `SUBSCRIBE channel [channel ...]` | 订阅频道 |
| `UNSUBSCRIBE channel [channel ...]` | 取消订阅频道 |

## 技术栈

| 组件 | 选型 | 理由 |
|------|------|------|
| 异步运行时 | `tokio`（多线程模式） | 业界标准，work-stealing 调度器 |
| 字节缓冲 | `bytes` | 零拷贝的 `BytesMut`/`Bytes`，用于 RESP 解析 |
| 快速搜索 | `memchr` | SIMD 加速的 `\r\n` 扫描 |
| 数字格式化 | `itoa` | 无堆分配的整数转 ASCII |
| 锁 | `parking_lot::RwLock` | 比标准库 RwLock 更快，无 poisoning |
| 并发 Map | `dashmap` | 分段锁 Map，用于 Pub/Sub 频道管理 |
| Glob 匹配 | `fast-glob` | Redis 风格的模式匹配，用于 KEYS 命令 |
| 并行扫描 | `rayon` | 并行迭代器，用于 KEYS 跨分片扫描 |
| 错误处理 | `anyhow` | 简洁的错误传播 |
| 日志 | `tracing` + `tracing-subscriber` | 结构化异步日志 |

## 性能测试

测试环境：WSL2 Ubuntu 24.04（Windows 11 宿主机），使用 `redis-benchmark`。

### 单线程 vs 多线程（50 并发，普通模式）

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get
```

| 模式 | GET | SET |
|------|-----|-----|
| 单线程 | 89,766/s | 91,157/s |
| 多线程 | 103,305/s | 100,200/s |
| 提升 | +15% | +10% |

### 单线程 vs 多线程（50 并发，pipeline 16）

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get -P 16
```

| 模式 | GET | SET |
|------|-----|-----|
| 单线程 | 943,396/s | 934,579/s |
| 多线程 | 1,449,275/s | 970,873/s |
| 提升 | +54% | +4% |

### 高并发（500 并发，普通模式）

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 500000 -t set,get
```

| 模式 | GET | SET |
|------|-----|-----|
| 单线程 | 97,276/s | 76,722/s |
| 多线程 | 104,101/s | 99,324/s |
| 提升 | +7% | +29% |

### 高并发（500 并发，pipeline 16）

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 16
```

| 模式 | GET | SET |
|------|-----|-----|
| 单线程 | 979,431/s | 801,282/s |
| 多线程 | 1,297,016/s | 1,022,494/s |
| 提升 | +32% | +28% |

### 高并发（500 并发，pipeline 64）

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 64
```

| 模式 | GET | SET |
|------|-----|-----|
| 单线程 | 1,552,795/s | 1,230,012/s |
| 多线程 | 3,164,556/s | 952,381/s |
| 提升 | +104% | -23% ⚠️ |

> 注意：在极端写入压力下（500 并发 × pipeline 64），多线程 SET 性能反而下降，原因是写锁竞争。`RwLock` 的写锁是独占的——一个线程写入时，所有其他线程必须等待。相比之下，GET 扩展性良好，因为读锁允许并发访问。

### 核心结论

- 单线程普通模式 GET/SET 约 9 万 QPS，接近官方 Redis 水平
- Pipeline 模式下单线程即可达到 93 万+ QPS
- 多线程 GET 在所有场景下扩展性良好（读锁支持并发）
- 多线程 SET 在极端 pipeline 深度下遇到写锁竞争瓶颈
- 256 分片 + `parking_lot::RwLock` 的设计在中等并发下表现优秀

## 快速开始

```bash
# 编译
cargo build --release

# 启动服务
./target/release/mini-redis

# 使用 redis-cli 测试
redis-cli -p 6377
> PING
PONG
> SET hello world
OK
> GET hello
"world"
```

## 许可证

MIT
