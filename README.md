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

### String

| Command | Description |
|---------|-------------|
| `SET key value [EX s] [PX ms] [NX\|XX]` | Set key with optional TTL and conditional flags |
| `GET key` | Get value by key |
| `INCR key` | Increment integer value by 1 |
| `DECR key` | Decrement integer value by 1 |
| `INCRBY key increment` | Increment integer value by given amount |
| `DECRBY key decrement` | Decrement integer value by given amount |
| `INCRBYFLOAT key increment` | Increment float value by given amount |

### List

| Command | Description |
|---------|-------------|
| `LPUSH key element [element ...]` | Prepend elements to a list |
| `RPUSH key element [element ...]` | Append elements to a list |
| `LPOP key [count]` | Remove and return elements from the head of a list |
| `RPOP key [count]` | Remove and return elements from the tail of a list |
| `BLPOP key [key ...] timeout` | Blocking left pop |
| `BRPOP key [key ...] timeout` | Blocking right pop |
| `LLEN key` | Get the length of a list |
| `LRANGE key start stop` | Get a range of elements from a list |
| `LINDEX key index` | Get an element by its index |
| `LSET key index element` | Set the value of an element by its index |
| `LREM key count element` | Remove elements from a list |
| `LINSERT key BEFORE\|AFTER pivot element` | Insert an element before or after another element |
| `LPUSHX key element [element ...]` | Prepend elements only if the list exists |
| `RPUSHX key element [element ...]` | Append elements only if the list exists |
| `LPOS key element [RANK rank] [COUNT count] [MAXLEN len]` | Return the index of matching elements |
| `LTRIM key start stop` | Trim a list to the specified range |
| `LMOVE source destination LEFT\|RIGHT LEFT\|RIGHT` | Move an element from one list to another |

### Hash

| Command | Description |
|---------|-------------|
| `HSET key field value [field value ...]` | Set one or more field-value pairs in a hash |
| `HGET key field` | Get the value of a field in a hash |
| `HMGET key field [field ...]` | Get the values of multiple fields in a hash |
| `HDEL key field [field ...]` | Delete one or more fields from a hash |
| `HEXISTS key field` | Check if a field exists in a hash |
| `HLEN key` | Get the number of fields in a hash |
| `HGETALL key` | Get all fields and values in a hash |
| `HKEYS key` | Get all field names in a hash |
| `HVALS key` | Get all values in a hash |
| `HINCRBY key field increment` | Increment the integer value of a field by a given amount |
| `HINCRBYFLOAT key field increment` | Increment the float value of a field by a given amount |
| `HSETNX key field value` | Set a field only if it does not already exist |
| `HSCAN key cursor [MATCH pattern] [COUNT count]` | Incrementally iterate over fields in a hash |

### Key

| Command | Description |
|---------|-------------|
| `DEL key [key ...]` | Delete one or more keys |
| `EXISTS key [key ...]` | Check if keys exist |
| `EXPIRE key seconds` | Set TTL in seconds |
| `PEXPIRE key milliseconds` | Set TTL in milliseconds |
| `TTL key` | Get remaining TTL in seconds |
| `PTTL key` | Get remaining TTL in milliseconds |
| `KEYS pattern` | Find keys matching glob pattern (async via `spawn_blocking` + rayon) |
| `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | Incrementally iterate the key space |
| `TYPE key` | Return the type of the value stored at key |

### Pub/Sub

| Command | Description |
|---------|-------------|
| `PUBLISH channel message` | Publish message to channel |
| `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| `UNSUBSCRIBE channel [channel ...]` | Unsubscribe from channels |

### Server

| Command | Description |
|---------|-------------|
| `PING [message]` | Connection test, returns PONG or echoes message |
| `SELECT index` | Select the database (accepted but single-db) |
| `DBSIZE` | Return the number of keys in the current database |
| `INFO [section]` | Return server information and statistics |
| `CONFIG GET parameter` | Get configuration parameters |
| `COMMAND` | Return information about available commands |
| `CLIENT` | Client connection management |
| `HELLO` | Handshake with the server |
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

### Performance Profiling (Flamegraph Analysis)

Profiled with `perf` + `inferno` flamegraph under 500 clients, pipeline 64, 2M requests.

#### Scenario Comparison

| Scenario | Throughput | vs Baseline |
|----------|-----------|-------------|
| SET (no TTL) | 1,919,385/s | baseline |
| GET (no TTL) | 5,128,205/s | baseline |
| SET with TTL (EX 5) | 1,196,172/s | -37.5% |
| SET hotkey (single key) | 2,066,115/s | +7.8% |

#### CPU Hotspot Breakdown (Baseline: SET/GET no TTL)

| Function | CPU % | Category |
|----------|-------|----------|
| Kernel (epoll/futex/network stack) | ~40% | System calls |
| `bytes_mut::shared_v_drop` | 2.81% | Bytes refcount |
| `Mutex::lock_contended` | 2.78% | Lock contention |
| `bytes_mut::shared_v_clone` | 2.22% | Bytes refcount |
| `Db::set` | 2.21% | Core SET logic |
| `syscall` | 1.64% | System calls |
| `Command::from_frame` | 1.49% | RESP parsing |
| `handle_connection` | 1.42% | Connection handler |
| `DashMap::_entry` | 1.20% | DashMap entry API |
| `Connection::find_crlf` | 1.06% | Frame parsing |

#### TTL Scenario: Additional Overhead

| Function | Baseline | With TTL | Delta |
|----------|----------|----------|-------|
| `Mutex::lock_contended` | 2.78% | 3.43% | +0.65% |
| `__vdso_clock_gettime` | 0.35% | 1.03% | +0.68% |
| `BTreeMap::remove` | — | 0.73% | new |
| `BTreeMap::insert` | — | 0.34% | new |

#### Key Findings

- The kernel network stack dominates CPU usage (~40%), meaning userspace code is already highly efficient
- The 2048-shard DashMap design shows no lock contention issues — hotkey test is actually faster than random keys due to better CPU cache locality
- TTL overhead comes primarily from Mutex contention and `clock_gettime` calls, not from BTreeMap operations themselves
- Double hashing (manual shard selection + DashMap internal hash) has negligible overhead — `shard_index` doesn't even appear in the top 50 functions

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
