# Mini-Redis

一个用 Rust 编写的高性能 Redis 服务器实现，用于学习 Rust 异步编程、并发设计和网络协议。实现了 RESP（Redis 序列化协议），支持核心 Redis 命令，注重零拷贝解析和高效内存管理。

## 功能特性

- 完整的 RESP 协议解析（Simple Strings、Errors、Integers、Bulk Strings、Arrays）
- 基于 tokio work-stealing 调度器的多线程异步运行时
- 2048 分片 `DashMap` 存储，细粒度并发访问
- 基于 `bytes::BytesMut` split/freeze 的零拷贝帧解析
- 批量读取 + 批量写入优化，提升 pipeline 模式吞吐量
- 基于分片 BTreeMap 索引的后台 TTL 清理（精准 `split_off` 替代全量扫描）
- 读取时惰性过期 + 每 5 秒轮询批次主动清理
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
   │  │+BTreeMap │  └────────┘  └─────┘  │
   │  │ 过期索引 │                        │
   │  └──────────┘                        │
   └──────────────────────────────────────┘
```

## 支持的命令

### 字符串（String）

| 命令 | 说明 |
|------|------|
| `SET key value [EX s] [PX ms] [NX\|XX]` | 设置键值，支持过期时间和条件写入 |
| `GET key` | 获取键对应的值 |
| `INCR key` | 将键的整数值加 1 |
| `DECR key` | 将键的整数值减 1 |
| `INCRBY key increment` | 将键的整数值增加指定数量 |
| `DECRBY key decrement` | 将键的整数值减少指定数量 |
| `INCRBYFLOAT key increment` | 将键的浮点数值增加指定数量 |

### 列表（List）

| 命令 | 说明 |
|------|------|
| `LPUSH key element [element ...]` | 从列表头部插入元素 |
| `RPUSH key element [element ...]` | 从列表尾部插入元素 |
| `LPOP key [count]` | 从列表头部移除并返回元素 |
| `RPOP key [count]` | 从列表尾部移除并返回元素 |
| `BLPOP key [key ...] timeout` | 阻塞式左弹出 |
| `BRPOP key [key ...] timeout` | 阻塞式右弹出 |
| `LLEN key` | 获取列表长度 |
| `LRANGE key start stop` | 获取列表指定范围内的元素 |
| `LINDEX key index` | 通过索引获取列表中的元素 |
| `LSET key index element` | 通过索引设置列表元素的值 |
| `LREM key count element` | 移除列表中的指定元素 |
| `LINSERT key BEFORE\|AFTER pivot element` | 在指定元素前或后插入元素 |
| `LPUSHX key element [element ...]` | 仅当列表存在时从头部插入元素 |
| `RPUSHX key element [element ...]` | 仅当列表存在时从尾部插入元素 |
| `LPOS key element [RANK rank] [COUNT count] [MAXLEN len]` | 返回匹配元素的索引 |
| `LTRIM key start stop` | 修剪列表，只保留指定范围内的元素 |
| `LMOVE source destination LEFT\|RIGHT LEFT\|RIGHT` | 将元素从一个列表移动到另一个列表 |

### 键（Key）

| 命令 | 说明 |
|------|------|
| `DEL key [key ...]` | 删除一个或多个键 |
| `EXISTS key [key ...]` | 检查键是否存在 |
| `EXPIRE key seconds` | 设置过期时间（秒） |
| `PEXPIRE key milliseconds` | 设置过期时间（毫秒） |
| `TTL key` | 查询剩余过期时间（秒） |
| `PTTL key` | 查询剩余过期时间（毫秒） |
| `KEYS pattern` | 按 glob 模式匹配键（通过 `spawn_blocking` + rayon 异步并行扫描） |
| `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | 增量迭代键空间 |
| `TYPE key` | 返回键所存储值的类型 |

### 发布/订阅（Pub/Sub）

| 命令 | 说明 |
|------|------|
| `PUBLISH channel message` | 向频道发布消息 |
| `SUBSCRIBE channel [channel ...]` | 订阅频道 |
| `UNSUBSCRIBE channel [channel ...]` | 取消订阅频道 |

### 服务器（Server）

| 命令 | 说明 |
|------|------|
| `PING [message]` | 连接测试，返回 PONG 或回显消息 |
| `SELECT index` | 选择数据库（已接受但为单库模式） |
| `DBSIZE` | 返回当前数据库的键数量 |
| `INFO [section]` | 返回服务器信息和统计数据 |
| `CONFIG GET parameter` | 获取配置参数 |
| `COMMAND` | 返回可用命令信息 |
| `CLIENT` | 客户端连接管理 |
| `HELLO` | 与服务器握手 |
| `BGREWRITEAOF` | 触发后台 AOF 重写 |

## 持久化

支持 AOF（Append-Only File）持久化，提供三种 fsync 策略：

- `Always` — 每批写入后立即 fsync（最安全，最慢）
- `EverySec` — 每秒 fsync 一次（安全与性能的平衡）
- `No` — 由操作系统决定何时刷盘（最快，最不安全）

AOF 使用长度前缀 + `bincode` 二进制格式序列化，紧凑高效。每条记录存储命令类型、key、value 及绝对过期时间戳（毫秒）。

启动时自动重放 AOF 文件恢复数据，已过期的 key 在重放时会被跳过。

### BGREWRITEAOF

支持后台 AOF 重写以压缩日志文件：

- 当 AOF 文件超过 10MB 时自动触发，也可通过 `BGREWRITEAOF` 命令手动触发
- 快照阶段在阻塞线程（`spawn_blocking`）中执行，避免阻塞异步运行时
- 重写期间，增量写入会双写到独立的 `.incr` 文件
- 快照完成后，追加增量文件内容并原子替换（`rename`）原 AOF 文件

## 技术栈

| 组件 | 选型 | 理由 |
|------|------|------|
| 异步运行时 | `tokio`（多线程模式） | 业界标准，work-stealing 调度器 |
| 字节缓冲 | `bytes` | 零拷贝的 `BytesMut`/`Bytes`，用于 RESP 解析 |
| 快速搜索 | `memchr` | SIMD 加速的 `\r\n` 扫描 |
| 数字格式化 | `itoa` / `atoi` / `lexical-core` | 无堆分配的整数转 ASCII 及高速浮点数解析/格式化 |
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

### 性能剖析（火焰图分析）

使用 `perf` + `inferno` 火焰图工具，在 500 并发、pipeline 64、200 万请求条件下采集。

#### 场景对比

| 场景 | 吞吐量 | 对比基线 |
|------|--------|---------|
| SET（无 TTL） | 1,919,385/s | 基线 |
| GET（无 TTL） | 5,128,205/s | 基线 |
| SET 带 TTL（EX 5） | 1,196,172/s | -37.5% |
| SET 热点 key（单 key） | 2,066,115/s | +7.8% |

#### CPU 热点分布（基线：SET/GET 无 TTL）

| 函数 | CPU 占比 | 分类 |
|------|---------|------|
| 内核态（epoll/futex/网络栈） | ~40% | 系统调用 |
| `bytes_mut::shared_v_drop` | 2.81% | Bytes 引用计数 |
| `Mutex::lock_contended` | 2.78% | 锁竞争 |
| `bytes_mut::shared_v_clone` | 2.22% | Bytes 引用计数 |
| `Db::set` | 2.21% | 核心 SET 逻辑 |
| `syscall` | 1.64% | 系统调用 |
| `Command::from_frame` | 1.49% | RESP 协议解析 |
| `handle_connection` | 1.42% | 连接处理 |
| `DashMap::_entry` | 1.20% | DashMap entry API |
| `Connection::find_crlf` | 1.06% | 帧解析 |

#### TTL 场景：额外开销

| 函数 | 基线 | 带 TTL | 变化 |
|------|------|--------|------|
| `Mutex::lock_contended` | 2.78% | 3.43% | +0.65% |
| `__vdso_clock_gettime` | 0.35% | 1.03% | +0.68% |
| `BTreeMap::remove` | — | 0.73% | 新增 |
| `BTreeMap::insert` | — | 0.34% | 新增 |

#### 关键发现

- 内核网络栈占据了约 40% 的 CPU 时间，说明用户态代码已经非常高效
- 2048 分片 DashMap 设计没有锁竞争问题 — 热点 key 测试反而比随机 key 更快，得益于更好的 CPU 缓存局部性
- TTL 开销主要来自 Mutex 争用和 `clock_gettime` 调用，而非 BTreeMap 操作本身
- 双重 hash（手动分片选择 + DashMap 内部 hash）开销可忽略 — `shard_index` 甚至未出现在 top 50 函数中

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
