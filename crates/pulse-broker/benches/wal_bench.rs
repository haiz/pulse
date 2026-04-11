use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pulse_broker::config::WalConfig;
use pulse_broker::storage::sharded_wal::ShardedWalWriter;
use pulse_broker::storage::wal::WalWriter;
use pulse_protocol::MessageId;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn wal_write_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("wal_write");

    // ── Single writer: vary payload size ──

    for payload_size in [64, 256, 1024, 4096] {
        let data = vec![0xABu8; payload_size];

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("single_writer", payload_size),
            &payload_size,
            |b, _| {
                let dir = tempfile::tempdir().unwrap();
                let config = WalConfig {
                    segment_size_bytes: 256 * 1024 * 1024,
                    sync_mode: "none".into(),
                    shards: 1,
                };
                let writer = rt
                    .block_on(WalWriter::open(dir.path().join("wal"), &config))
                    .unwrap();
                let writer = Arc::new(tokio::sync::Mutex::new(writer));

                b.to_async(&rt).iter(|| {
                    let w = writer.clone();
                    let data = data.clone();
                    async move {
                        let mut wal = w.lock().await;
                        wal.append_event(MessageId::new(), &data).await.unwrap();
                    }
                });
            },
        );
    }

    // ── Sharded writer: vary shard count ──

    for num_shards in [1, 2, 4, 8] {
        let data = vec![0xABu8; 256];

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("sharded", num_shards),
            &num_shards,
            |b, &shards| {
                let dir = tempfile::tempdir().unwrap();
                let config = WalConfig {
                    segment_size_bytes: 256 * 1024 * 1024,
                    sync_mode: "none".into(),
                    shards,
                };
                let wal = rt
                    .block_on(ShardedWalWriter::open(
                        dir.path().join("wal"),
                        &config,
                        shards,
                    ))
                    .unwrap();

                let mut counter = 0u64;
                b.to_async(&rt).iter(|| {
                    let topic = format!("bench.topic.{}", counter % 100);
                    counter += 1;
                    let wal = &wal;
                    let data = &data;
                    async move {
                        wal.append_event(&topic, MessageId::new(), data)
                            .await
                            .unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, wal_write_benchmark);
criterion_main!(benches);
