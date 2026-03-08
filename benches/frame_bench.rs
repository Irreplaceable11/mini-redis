use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use bytes::{Bytes, BytesMut};
use mini_redis::frame::Frame;

/// Frame 编码性能
fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_encode");

    group.bench_function("simple_string", |b| {
        let frame = Frame::SimpleString("OK".to_string());
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            frame.encode(&mut buf);
        });
    });

    group.bench_function("error", |b| {
        let frame = Frame::Error("ERR unknown command".to_string());
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            frame.encode(&mut buf);
        });
    });

    group.bench_function("integer", |b| {
        let frame = Frame::Integer(123456);
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            frame.encode(&mut buf);
        });
    });

    group.bench_function("bulk_string_small", |b| {
        let frame = Frame::BulkString(Bytes::from_static(b"hello world"));
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            frame.encode(&mut buf);
        });
    });

    group.bench_function("bulk_string_1kb", |b| {
        let frame = Frame::BulkString(vec![b'x'; 1024].into());
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(2048);
            frame.encode(&mut buf);
        });
    });

    group.bench_function("null", |b| {
        let frame = Frame::Null;
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(16);
            frame.encode(&mut buf);
        });
    });

    // 模拟一个典型的 SET 命令数组: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n
    group.bench_function("array_set_cmd", |b| {
        let frame = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"mykey")),
            Frame::BulkString(Bytes::from_static(b"hello")),
        ]);
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(128);
            frame.encode(&mut buf);
        });
    });

    // 嵌套数组编码
    group.bench_function("nested_array", |b| {
        let frame = Frame::Array(vec![
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"key1")),
                Frame::BulkString(Bytes::from_static(b"value1")),
            ]),
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"key2")),
                Frame::BulkString(Bytes::from_static(b"value2")),
            ]),
        ]);
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            frame.encode(&mut buf);
        });
    });

    group.finish();
}

/// 不同大小 BulkString 编码的吞吐量对比
fn bench_encode_bulk_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_encode_bulk_size");

    for size in [16, 128, 1024, 8192, 65536] {
        let frame = Frame::BulkString(vec![b'A'; size].into());
        group.bench_with_input(BenchmarkId::new("encode", size), &size, |b, _| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(size + 32);
                frame.encode(&mut buf);
            });
        });
    }

    group.finish();
}

/// 编码到预分配 buffer vs 新 buffer 的对比
fn bench_encode_reuse_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_encode_buffer_reuse");
    let frame = Frame::Array(vec![
        Frame::BulkString(Bytes::from_static(b"SET")),
        Frame::BulkString(Bytes::from_static(b"mykey")),
        Frame::BulkString(vec![b'v'; 256].into()),
    ]);

    group.bench_function("new_buffer_each_time", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(512);
            frame.encode(&mut buf);
        });
    });

    group.bench_function("reuse_buffer", |b| {
        let mut buf = BytesMut::with_capacity(512);
        b.iter(|| {
            buf.clear();
            frame.encode(&mut buf);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_encode,
    bench_encode_bulk_sizes,
    bench_encode_reuse_buffer,
);
criterion_main!(benches);
