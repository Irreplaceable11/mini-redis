# Mini-Redis

一个用 Rust 编写的高性能 Redis 服务器实现，用于学习 Rust 异步编程、并发设计和网络协议。实现了 RESP（Redis 序列化协议），支持核心 Redis 命令，注重零拷贝解析和高效内存管理。

## 功能特性

- 完整的 RESP 协议解析（Simple Strings、Errors、Integers、Bulk Strings、Arrays）
- 基于 tokio work-stealing 调度器的多线程异步运行时
- 2048 分片 `DashMap` 存储，细粒度并发访问
- 基于 `bytes::BytesMut` split/freeze 的零拷贝帧解析
- 批量读取 + 批量写入优化，提升 pipeline 模式吞吐量
- 后台 TTL 清理任务，轮询式分片扫描
- 基于 `tokio::sync::broadcast` 通道的发布/订阅消息系统
- AOF（Append-Only File）持久化，支持可配置的 fsync 策略
- jemalloc 内存分配器，减少内存碎片

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
   │  ┌──────────┐  ┌────────┐  ┌─────┐  │
   │  │    Db    │  │ PubSub │  │ AOF │  │
   │  │2048 分片 │  │DashMap │  │写入 │  │
   │  │ DashMap  │  │broadcast│ │日志 │  │
   │  └──────────┘  └────────┘  └─────┘  │
   └──────────────────────────────────────┘
```

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

## 持久化

支持 AOF（Append-Only File）持久化，提供三种 fsync 策略：

- `Always` — 每批写入后立即 fsync（最安全，最慢）
- `EverySec` — 每秒 fsync 一次（安全与性能的平衡）
- `No` — 由操作系统决定何时刷盘（最快，最不安全）

启动时自动重放 AOF 文件恢复数据，已过期的 key 在重放时会被跳过。

## 技术栈

| 组件 | 选型 | 理由 |
|------|------|------|
| 异步运行时 | `tokio`（多线程模式） | 业界标准，work-stealing 调度器 |
| 字节缓冲 | `bytes` | 零拷贝的 `BytesMut`/`Bytes`，用于 RESP 解析 |
| 快速搜索 | `memchr` | SIMD 加速的 `\r\n` 扫描 |
| 数字格式化 | `itoa` / `atoi` | 无堆分配的整数转 ASCII |
| 并发 Map | `dashmap` + `ahash` | 分段锁 Map，用于 DB 和 Pub/Sub |
| Glob 匹配 | `fast-glob` | Redis 风格的模式匹配，用于 KEYS 命令 |
| 并行扫描 | `rayon` | 并行迭代器，用于 KEYS 跨分片扫描 |
| 序列化 | `bincode` + `serde` | 高速二进制编码，用于 AOF 条目 |
| 内存分配器 | `tikv-jemallocator` | 减少并发负载下的内存碎片 |
| 错误处理 | `anyhow` | 简洁的错误传播 |
| 日志 | `tracing` + `tracing-subscriber` | 结构化异步日志 |

## 性能测试

测试环境：WSL2 Ubuntu 24.04（Windows 11 宿主机），使用 `redis-benchmark`。
压测期间关闭 AOF 持久化，测量纯内存吞吐量。

### 50 并发，普通模式

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get
```

| 命令 | 吞吐量 |
|------|--------|
| SET | 132,450/s |
| GET | 117,370/s |

### 50 并发，pipeline 16

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get -P 16
```

| 命令 | 吞吐量 |
|------|--------|
| SET | 1,492,537/s |
| GET | 1,886,792/s |

### 500 并发，普通模式

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 500000 -t set,get
```

| 命令 | 吞吐量 |
|------|--------|
| SET | 159,438/s |
| GET | 178,635/s |

### 500 并发，pipeline 16

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 16
```

| 命令 | 吞吐量 |
|------|--------|
| SET | 2,145,922/s |
| GET | 2,109,704/s |

### 500 并发，pipeline 64

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 64
```

| 命令 | 吞吐量 |
|------|--------|
| SET | 3,278,688/s |
| GET | 5,494,505/s |

### 核心结论

- 普通模式下 50 并发可达 13 万+ QPS
- Pipeline 16 模式下吞吐量提升至 150 万~210 万 QPS
- Pipeline 64 + 500 并发下，GET 达到 550 万 QPS，SET 达到 330 万 QPS
- 2048 分片 DashMap 设计在高并发场景下扩展性良好

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
> SET session abc EX 60
OK
> TTL session
(integer) 59
```

## 许可证

MIT
