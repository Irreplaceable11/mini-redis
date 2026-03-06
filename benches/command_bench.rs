use criterion::{criterion_group, criterion_main, Criterion};
use mini_redis::command::Command;
use mini_redis::db::Db;
use mini_redis::frame::Frame;

/// 构造一个模拟的 RESP 命令 Frame
fn make_cmd_frame(parts: &[&[u8]]) -> Frame {
    Frame::Array(
        parts.iter().map(|p| Frame::BulkString(p.to_vec())).collect()
    )
}

/// 命令解析性能
fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_parse");

    group.bench_function("ping_no_arg", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"PING"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("ping_with_msg", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"PING", b"hello"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"GET", b"mykey"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("set_simple", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"SET", b"mykey", b"myvalue"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("set_with_ex", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"SET", b"mykey", b"myvalue", b"EX", b"60"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("set_with_px_nx", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"SET", b"mykey", b"myvalue", b"PX", b"5000", b"NX"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("del_single", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"DEL", b"key1"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("del_multi", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"DEL", b"k1", b"k2", b"k3", b"k4", b"k5"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.bench_function("unknown_cmd", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"MGET", b"k1", b"k2"]);
            Command::from_frame(frame).unwrap();
        });
    });

    group.finish();
}

/// 命令解析 + 执行的端到端性能
fn bench_execute(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_execute");
    let db = Db::new();

    // 预填充一些数据用于 GET 测试
    let value = b"benchmark_value".to_vec();
    for i in 0..1000 {
        db.set(&format!("bench:{}", i), value.clone(), None, false, false);
    }

    group.bench_function("ping_e2e", |b| {
        b.iter(|| {
            let frame = make_cmd_frame(&[b"PING"]);
            let cmd = Command::from_frame(frame).unwrap();
            cmd.execute(&db);
        });
    });

    group.bench_function("get_hit_e2e", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench:{}", i % 1000);
            let frame = make_cmd_frame(&[b"GET", key.as_bytes()]);
            let cmd = Command::from_frame(frame).unwrap();
            cmd.execute(&db);
            i += 1;
        });
    });

    group.bench_function("get_miss_e2e", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("miss:{}", i);
            let frame = make_cmd_frame(&[b"GET", key.as_bytes()]);
            let cmd = Command::from_frame(frame).unwrap();
            cmd.execute(&db);
            i += 1;
        });
    });

    group.bench_function("set_e2e", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("setbench:{}", i);
            let frame = make_cmd_frame(&[b"SET", key.as_bytes(), b"value123"]);
            let cmd = Command::from_frame(frame).unwrap();
            cmd.execute(&db);
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_execute,
);
criterion_main!(benches);
