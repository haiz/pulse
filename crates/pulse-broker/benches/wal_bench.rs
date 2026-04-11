use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pulse_broker::config::WalConfig;
use pulse_broker::storage::sharded_wal::ShardedWalWriter;
use pulse_broker::storage::wal::WalWriter;
use pulse_protocol::MessageId;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

fn test_config() -> WalConfig {
    WalConfig {
        segment_size_bytes: 256 * 1024 * 1024,
        sync_mode: "none".into(),
        shards: 1,
    }
}

/// Sequential benchmark: one task writing as fast as possible.
/// Measures raw per-write overhead.
fn sequential_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("wal_sequential");
    let data = vec![0xABu8; 256];

    // Baseline: Mutex<WalWriter> (old architecture)
    group.throughput(Throughput::Elements(1));
    group.bench_function("mutex_walwriter", |b| {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let writer = rt
            .block_on(WalWriter::open(dir.path().join("wal"), &config))
            .unwrap();
        let writer = Mutex::new(writer);

        b.to_async(&rt).iter(|| {
            let writer = &writer;
            let data = &data;
            async move {
                let mut wal = writer.lock().await;
                wal.append_event(MessageId::new(), data).await.unwrap();
            }
        });
    });

    // ShardedWalWriter with 1 shard (new architecture, should be ~same)
    group.throughput(Throughput::Elements(1));
    group.bench_function("sharded_1", |b| {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let wal = ShardedWalWriter::open(
            dir.path().join("wal"),
            &config,
            1,
            Duration::from_millis(5),
            100,
        )
        .unwrap();
        // Pre-compute shard routing to isolate WAL write cost
        let topic = "bench.topic.0";

        b.to_async(&rt).iter(|| {
            let wal = &wal;
            let data = data.clone();
            async move {
                wal.append_event(topic, MessageId::new(), data)
                    .await
                    .unwrap();
            }
        });
    });

    group.finish();
}

/// Concurrent benchmark: N tasks writing simultaneously.
/// This is where sharding should shine.
fn concurrent_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("wal_concurrent");
    let topics: Vec<String> = (0..100).map(|i| format!("bench.topic.{i}")).collect();
    let data = vec![0xABu8; 256];
    let num_writers = 8;
    let writes_per_task = 50;

    group.throughput(Throughput::Elements((num_writers * writes_per_task) as u64));

    // Old architecture: single Mutex<WalWriter>, N tasks contending
    group.bench_function("mutex_walwriter_8tasks", |b| {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let writer = rt
            .block_on(WalWriter::open(dir.path().join("wal"), &config))
            .unwrap();
        let writer = Arc::new(Mutex::new(writer));

        b.to_async(&rt).iter(|| {
            let writer = writer.clone();
            let data = data.clone();
            async move {
                let mut handles = Vec::with_capacity(num_writers);
                for _ in 0..num_writers {
                    let w = writer.clone();
                    let d = data.clone();
                    handles.push(tokio::spawn(async move {
                        for _ in 0..writes_per_task {
                            let mut wal = w.lock().await;
                            wal.append_event(MessageId::new(), &d).await.unwrap();
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            }
        });
    });

    // New architecture: ShardedWalWriter with 4 shards
    for num_shards in [1, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("sharded_8tasks", num_shards),
            &num_shards,
            |b, &shards| {
                let dir = tempfile::tempdir().unwrap();
                let config = test_config();
                let wal = Arc::new(
                    ShardedWalWriter::open(
                        dir.path().join("wal"),
                        &config,
                        shards,
                        Duration::from_millis(5),
                        100,
                    )
                    .unwrap(),
                );

                b.to_async(&rt).iter(|| {
                    let wal = wal.clone();
                    let topics = &topics;
                    let data = data.clone();
                    async move {
                        let mut handles = Vec::with_capacity(num_writers);
                        for i in 0..num_writers {
                            let w = wal.clone();
                            let d = data.clone();
                            let t = topics[i * writes_per_task % topics.len()..].to_vec();
                            handles.push(tokio::spawn(async move {
                                for j in 0..writes_per_task {
                                    let topic = &t[j % t.len()];
                                    w.append_event(topic, MessageId::new(), d.clone())
                                        .await
                                        .unwrap();
                                }
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, sequential_benchmark, concurrent_benchmark);
criterion_main!(benches);
