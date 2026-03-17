# Mini-Redis

A high-performance Redis server implementation written in Rust for learning purposes. Implements the RESP (Redis Serialization Protocol) and supports core Redis commands with a focus on concurrency, zero-copy parsing, and efficient memory management.

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
   │  ┌──────────┐    ┌───────────────┐   │
   │  │    Db    │    │    PubSub     │   │
   │  │ 256-shard│    │  (broadcast)  │   │
   │  │ RwLock   │    │  DashMap      │   │
   │  └──────────┘    └───────────────┘   │
   └──────────────────────────────────────┘
```

- Multi-threaded async runtime (tokio) with per-connection task spawning
- 256-shard partitioned storage with `parking_lot::RwLock` for fine-grained locking
- Zero-copy RESP frame parsing using `bytes::BytesMut` split/freeze
- Batch read + batch write optimization for pipeline mode
- Background TTL cleanup with round-robin shard scanning
- Pub/Sub via `tokio::sync::broadcast` channels managed by `DashMap`

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
| `PUBLISH channel message` | Publish message to channel |
| `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| `UNSUBSCRIBE channel [channel ...]` | Unsubscribe from channels |

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Async Runtime | `tokio` (multi-thread) | Industry standard, work-stealing scheduler |
| Byte Buffers | `bytes` | Zero-copy `BytesMut`/`Bytes` for RESP parsing |
| Fast Search | `memchr` | SIMD-accelerated `\r\n` scanning |
| Number Format | `itoa` | Heap-free integer-to-ASCII conversion |
| Locking | `parking_lot::RwLock` | Faster than std RwLock, no poisoning |
| Concurrent Map | `dashmap` | Lock-sharded map for Pub/Sub channels |
| Glob Matching | `fast-glob` | Redis-style pattern matching for KEYS |
| Parallel Scan | `rayon` | Parallel iterator for KEYS across shards |
| Error Handling | `anyhow` | Ergonomic error propagation |
| Logging | `tracing` + `tracing-subscriber` | Structured async-aware logging |

## Benchmark

Test environment: WSL2 Ubuntu 24.04 on Windows 11, using `redis-benchmark`.

### Single-thread vs Multi-thread (50 clients, normal mode)

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get
```

| Mode | GET | SET |
|------|-----|-----|
| Single-thread | 89,766/s | 91,157/s |
| Multi-thread | 103,305/s | 100,200/s |
| Improvement | +15% | +10% |

### Single-thread vs Multi-thread (50 clients, pipeline 16)

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 50 -n 100000 -t set,get -P 16
```

| Mode | GET | SET |
|------|-----|-----|
| Single-thread | 943,396/s | 934,579/s |
| Multi-thread | 1,449,275/s | 970,873/s |
| Improvement | +54% | +4% |

### High Concurrency (500 clients, normal mode)

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 500000 -t set,get
```

| Mode | GET | SET |
|------|-----|-----|
| Single-thread | 97,276/s | 76,722/s |
| Multi-thread | 104,101/s | 99,324/s |
| Improvement | +7% | +29% |

### High Concurrency (500 clients, pipeline 16)

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 16
```

| Mode | GET | SET |
|------|-----|-----|
| Single-thread | 979,431/s | 801,282/s |
| Multi-thread | 1,297,016/s | 1,022,494/s |
| Improvement | +32% | +28% |

### High Concurrency (500 clients, pipeline 64)

```
redis-benchmark -h 127.0.0.1 -p 6377 -c 500 -n 1000000 -t set,get -P 64
```

| Mode | GET | SET |
|------|-----|-----|
| Single-thread | 1,552,795/s | 1,230,012/s |
| Multi-thread | 3,164,556/s | 952,381/s |
| Improvement | +104% | -23% ⚠️ |

> Note: Under extreme write pressure (500 clients × pipeline 64), multi-thread SET performance degrades due to write lock contention. The `RwLock` write lock is exclusive — all other threads must wait. In contrast, GET scales well because read locks allow concurrent access.

### Key Takeaways

- Single-thread achieves ~90K QPS for normal GET/SET, comparable to official Redis
- Pipeline mode reaches 930K+ QPS even on a single thread
- Multi-thread GET scales well under all conditions (read locks are concurrent)
- Multi-thread SET hits write lock contention at extreme pipeline depths
- The 256-shard design with `parking_lot::RwLock` provides good performance up to moderate concurrency

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
```

## License

MIT
