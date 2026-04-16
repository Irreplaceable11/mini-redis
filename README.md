# Mini-Redis

A high-performance Redis server implementation written in Rust for learning purposes. Implements the RESP (Redis Serialization Protocol) and supports core Redis commands with a focus on concurrency, zero-copy parsing, and efficient memory management.

## Features

- Full RESP protocol parsing (Simple Strings, Errors, Integers, Bulk Strings, Arrays)
- Multi-threaded async runtime with tokio work-stealing scheduler
- 2048-shard partitioned storage using `DashMap` for fine-grained concurrent access
- Zero-copy frame parsing with `bytes::BytesMut` split/freeze
- Batch read + batch write optimization for pipeline mode
- Background TTL cleanup with BTreeMap-indexed expiry per shard (precise `split_off` instead of full scan)
- Lazy expiration on read + active expiration every 5 seconds via round-robin shard batches
- Pub/Sub messaging via `tokio::sync::broadcast` channels
- AOF (Append-Only File) persistence with configurable fsync policies
- jemalloc allocator for reduced memory fragmentation

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  TcpListener                    │
│              (tokio async accept)               │
└──────────────────────┬──────────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   ┌─────────┐  ┌─────────┐  ┌─────────┐
   │ Handler │  │ Handler │  │ Handler │   per-connection async task
   └────┬────┘  └────┬────┘  └────┬────┘
        │            │            │
        ▼            ▼            ▼
   ┌─────────────────────────────────────┐
   │           Connection                │
   │  (RESP parse / zero-copy encode)    │
   └─────────────────────────────────────┘
        │            │            │
        ▼            ▼            ▼
   ┌──────────────────────────────────────┐
   │         Context (Arc shared)         │
   │  ┌──────────┐  ┌────────┐  ┌─────┐  │
   │  │    Db    │  │ PubSub │  │ AOF │  │
   │  │2048-shard│  │DashMap │  │Write│  │
   │  │ DashMap  │  │broadcast│ │ Log │  │
   │  │+BTreeMap │  └────────┘  └─────┘  │
   │  │ expiry   │                        │
   │  └──────────┘                        │
   └──────────────────────────────────────┘
```

## Supported Commands

| Command | Description |
|---------|-------------|
| `PING [message]` | Connection test, returns PONG or echoes message |
| `SET key value [EX s] [PX ms] [NX\|XX]` | Set key with optional TTL and conditional flags |
| `GET key` | Get value by key |
| `DEL key [key ...]` | Delete one or more keys |
| `EXISTS key [key ...]` | Check if keys exist |
| `EXPIRE key seconds` | Set TTL in seconds |
| `PEXPIRE key milliseconds` | Set TTL in milliseconds |
| `TTL key` | Get remaining TTL in seconds |
| `PTTL key` | Get remaining TTL in milliseconds |
| `KEYS pattern` | Find keys matching glob pattern (async via `spawn_blocking` + rayon) |
| `INCR key` | Increment integer value by 1 |
| `DECR key` | Decrement integer value by 1 |
| `INCRBY key increment` | Increment integer value by given amount |
| `DECRBY key decrement` | Decrement integer value by given amount |
| `INCRBYFLOAT key increment` | Increment float value by given amount |
| `PUBLISH channel message` | Publish message to channel |
| `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| `UNSUBSCRIBE channel [channel ...]` | Unsubscribe from channels |
| `BGREWRITEAOF` | Trigger background AOF rewrite |

## Persistence

AOF (Append-Only File) support with three fsync policies:

- `Always` — fsync after every write batch (safest, slowest)
- `EverySec` — fsync once per second (good balance)
- `No` — let the OS decide when to flush (fastest, least safe)

AOF uses a length-prefixed binary format (`bincode`) for compact and fast serialization. Each entry stores the command type, key, value, and absolute expiry timestamp in milliseconds.

On startup, the AOF file is replayed to restore state. Expired keys are skipped during replay.

### BGREWRITEAOF

Supports background AOF rewrite to compact the log file:

- Triggered automatically when AOF file exceeds 10MB, or manually via `BGREWRITEAOF` command
- Snapshot phase runs in a blocking thread (`spawn_blocking`) to avoid stalling the async runtime
- During rewrite, incremental writes are dual-written to a separate `.incr` file
- After snapshot completes, the incremental file is appended and atomically renamed to replace the original AOF

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Async Runtime | `tokio` (multi-thread) | Industry standard, work-stealing scheduler |
| Byte Buffers | `bytes` | Zero-copy `BytesMut`/`Bytes` for RESP parsing |
| Fast Search | `memchr` | SIMD-accelerated `\r\n` scanning |
| Number Format | `itoa` / `atoi` / `lexical-core` | Heap-free integer-to-ASCII and fast float parsing/formatting |
| Concurrent Map | `dashmap` + `ahash` | Lock-sharded map for DB and Pub/Sub |
| Glob Matching | `fast-glob` | Redis-style pattern matching for KEYS |
| Parallel Scan | `rayon` | Parallel iterator for KEYS across shards |
| Serialization | `bincode` + `serde` | Fast binary encoding for AOF entries |
| Allocator | `tikv-jemallocator` | Reduced fragmentation under concurrent load |
| Error Handling | `anyhow` | Ergonomic error propagation |
| Logging | `tracing` + `tracing-subscriber` | Structured async-aware logging |

## Benchmark

Test environment: WSL2 Ubuntu 24.04 on Windows 11, using `redis-benchmark`.  
AOF persistence disabled during benchmarks to measure pure in-memory throughput.

### 50 clients, normal mode

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get
```

| Command | Throughput |
|---------|-----------|
| SET | 132,450/s |
| GET | 117,370/s |

### 50 clients, pipeline 16

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get -P 16
```

| Command | Throughput |
|---------|-----------|
| SET | 1,492,537/s |
| GET | 1,886,792/s |

### 500 clients, normal mode

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 500000 -t set,get
```

| Command | Throughput |
|---------|-----------|
| SET | 159,438/s |
| GET | 178,635/s |

### 500 clients, pipeline 16

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 16
```

| Command | Throughput |
|---------|-----------|
| SET | 2,145,922/s |
| GET | 2,109,704/s |

### 500 clients, pipeline 64

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 64
```

| Command | Throughput |
|---------|-----------|
| SET | 3,278,688/s |
| GET | 5,494,505/s |

### Key Takeaways

- Normal mode achieves ~130K+ QPS for SET/GET with 50 clients
- Pipeline 16 pushes throughput to 1.5M~2.1M QPS
- At pipeline 64 with 500 clients, GET reaches 5.5M QPS, SET reaches 3.3M QPS
- The 2048-shard DashMap design scales well under high concurrency

## Quick Start

```bash
# Build
cargo build --release

# Run server
./target/release/mini-redis

# Test with redis-cli
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

## License

MIT
