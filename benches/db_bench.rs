use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use mini_redis::db::Db;
use std::time::{Duration, Instant};

/// 基础 set 操作性能
fn bench_set(c: &mut Criterion) {
    let db = Db::new();
    let value = b"hello world".to_vec();

    c.bench_function("db_set_no_ttl", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key:{}", i);
            db.set(&key, value.clone(), None, false, false);
            i += 1;
        });
    });

    c.bench_function("db_set_with_ttl", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key:ttl:{}", i);
            let ttl = Some(Instant::now() + Duration::from_secs(60));
            db.set(&key, value.clone(), ttl, false, false);
            i += 1;
        });
    });
}

/// 基础 get 操作性能（命中 vs 未命中）
fn bench_get(c: &mut Criterion) {
    let db = Db::new();
    let value = b"hello world".to_vec();

    // 预填充数据
    for i in 0..10000 {
        db.set(&format!("key:{}", i), value.clone(), None, false, false);
    }

    c.bench_function("db_get_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key:{}", i % 10000);
            db.get(&key);
            i += 1;
        });
    });

    c.bench_function("db_get_miss", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("nonexistent:{}", i);
            db.get(&key);
            i += 1;
        });
    });
}

/// del 操作性能
fn bench_del(c: &mut Criterion) {
    let db = Db::new();
    let value = b"hello world".to_vec();

    c.bench_function("db_del_single", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("del:{}", i);
            db.set(&key, value.clone(), None, false, false);
            db.del(vec![key]);
            i += 1;
        });
    });

    c.bench_function("db_del_batch_10", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let keys: Vec<String> = (0..10).map(|j| format!("delbatch:{}:{}", i, j)).collect();
            for k in &keys {
                db.set(k, value.clone(), None, false, false);
            }
            db.del(keys);
            i += 1;
        });
    });
}

/// NX/XX 条件写入性能
fn bench_set_conditional(c: &mut Criterion) {
    let db = Db::new();
    let value = b"hello".to_vec();

    // 预填充一些 key
    for i in 0..1000 {
        db.set(&format!("exist:{}", i), value.clone(), None, false, false);
    }

    c.bench_function("db_set_nx_success", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("nx:new:{}", i);
            db.set(&key, value.clone(), None, true, false);
            i += 1;
        });
    });

    c.bench_function("db_set_nx_fail", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("exist:{}", i % 1000);
            db.set(&key, value.clone(), None, true, false);
            i += 1;
        });
    });
}

/// 不同 value 大小对性能的影响
fn bench_value_sizes(c: &mut Criterion) {
    let db = Db::new();
    let mut group = c.benchmark_group("db_value_size");

    for size in [16, 256, 1024, 4096, 65536] {
        let value = vec![b'x'; size];
        group.bench_with_input(BenchmarkId::new("set", size), &size, |b, _| {
            let mut i = 0u64;
            b.iter(|| {
                let key = format!("size:{}:{}", size, i);
                db.set(&key, value.clone(), None, false, false);
                i += 1;
            });
        });
    }
    group.finish();
}

/// clean_up 性能
fn bench_cleanup(c: &mut Criterion) {
    c.bench_function("db_cleanup", |b| {
        b.iter_batched(
            || {
                // setup: 创建一个有大量过期 key 的 db
                let db = Db::new();
                let value = b"val".to_vec();
                let expired = Some(Instant::now() - Duration::from_secs(1));
                for i in 0..5000 {
                    db.set(&format!("exp:{}", i), value.clone(), expired, false, false);
                }
                // 也放一些没过期的
                for i in 0..5000 {
                    db.set(&format!("alive:{}", i), value.clone(), None, false, false);
                }
                db
            },
            |db| {
                // 测量 clean_up 的耗时
                db.clean_up();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_del,
    bench_set_conditional,
    bench_value_sizes,
    bench_cleanup,
);
criterion_main!(benches);
