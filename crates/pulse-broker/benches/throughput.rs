use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::sync::Arc;

use pulse_protocol::{MessageId, PubPayload};

use tokio::sync::{mpsc, oneshot};

use pulse_broker::config::BrokerConfig;
use pulse_broker::pipeline::dedup::DedupEngine;
use pulse_broker::pipeline::dispatcher::Dispatcher;
use pulse_broker::storage::state_db::StateDb;
use pulse_broker::storage::sharded_wal::ShardedWalWriter;
use pulse_broker::storage::wal::WalWriter;

fn small_payload() -> PubPayload {
    PubPayload {
        topic: "bench.topic".into(),
        data: rmpv::Value::String("bench-data".into()),
        headers: HashMap::new(),
        produced_at: None,
        delivery: None,
        raw_payload: None,
    }
}

fn large_payload(size_kb: usize) -> PubPayload {
    let data = "x".repeat(size_kb * 1024);
    PubPayload {
        topic: "bench.topic".into(),
        data: rmpv::Value::String(data.into()),
        headers: HashMap::from([("trace_id".into(), "abc123".into())]),
        produced_at: Some(1700000000000),
        delivery: None,
        raw_payload: None,
    }
}

fn setup_dispatcher(rt: &tokio::runtime::Runtime) -> (tempfile::TempDir, Arc<Dispatcher>) {
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_testing(dir.path().to_path_buf());

    let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
    let wal = rt.block_on(async {
        ShardedWalWriter::open(config.data_dir.join("wal"), &config.wal, config.wal.shards)
            .await
            .unwrap()
    });
    let dedup = DedupEngine::new(state_db);
    let dispatcher = Arc::new(Dispatcher::new(dedup, wal));
    (dir, dispatcher)
}

// ─── Ingest Pipeline Benchmarks ───

fn bench_ingest(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, dispatcher) = setup_dispatcher(&rt);
    let payload = small_payload();

    let mut group = c.benchmark_group("ingest");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_event", |b| {
        b.to_async(&rt).iter(|| {
            let d = dispatcher.clone();
            let p = payload.clone();
            async move {
                black_box(d.ingest(MessageId::new(), &p).await);
            }
        });
    });

    group.finish();
}

fn bench_ingest_payload_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, dispatcher) = setup_dispatcher(&rt);

    let mut group = c.benchmark_group("ingest_payload_size");

    for size_kb in [1, 4, 16, 64] {
        let payload = large_payload(size_kb);
        group.throughput(Throughput::Bytes((size_kb * 1024) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{size_kb}KB")),
            &payload,
            |b, payload| {
                b.to_async(&rt).iter(|| {
                    let d = dispatcher.clone();
                    let p = payload.clone();
                    async move {
                        black_box(d.ingest(MessageId::new(), &p).await);
                    }
                });
            },
        );
    }

    group.finish();
}

// ─── WAL Write Benchmarks ───

fn bench_wal_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_testing(dir.path().to_path_buf());

    let wal = rt.block_on(async {
        WalWriter::open(config.data_dir.join("wal"), &config.wal)
            .await
            .unwrap()
    });
    let wal = Arc::new(tokio::sync::Mutex::new(wal));

    let mut group = c.benchmark_group("wal_write");
    group.throughput(Throughput::Elements(1));

    group.bench_function("append_event", |b| {
        b.to_async(&rt).iter(|| {
            let w = wal.clone();
            async move {
                let mut wal = w.lock().await;
                black_box(
                    wal.append_event(MessageId::new(), b"benchmark-payload-data")
                        .await
                        .unwrap(),
                );
            }
        });
    });

    group.bench_function("append_no_sync", |b| {
        b.to_async(&rt).iter(|| {
            let w = wal.clone();
            async move {
                let mut wal = w.lock().await;
                black_box(
                    wal.append_event_no_sync(MessageId::new(), b"benchmark-payload-data")
                        .await
                        .unwrap(),
                );
            }
        });
    });

    group.finish();
}

// ─── Bloom Filter Benchmarks ───

fn bench_bloom_filter(c: &mut Criterion) {
    use pulse_broker::pipeline::bloom::BloomFilter;

    let mut group = c.benchmark_group("bloom_filter");
    group.throughput(Throughput::Elements(1));

    let mut bf = BloomFilter::new(1_000_000, 0.001);
    for i in 0u64..100_000 {
        bf.insert(&i);
    }

    group.bench_function("lookup_miss", |b| {
        let mut counter = 200_000u64;
        b.iter(|| {
            counter += 1;
            black_box(bf.may_contain(&counter));
        });
    });

    group.bench_function("lookup_hit", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = (counter + 1) % 100_000;
            black_box(bf.may_contain(&counter));
        });
    });

    group.bench_function("insert", |b| {
        let mut bf2 = BloomFilter::new(1_000_000, 0.001);
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            bf2.insert(&counter);
        });
    });

    group.finish();
}

// ─── Topic Trie Benchmarks ───

fn bench_topic_trie(c: &mut Criterion) {
    use pulse_broker::routing::engine::{SubscriptionTarget, TopicTrie};
    use tokio::sync::mpsc;

    let mut trie = TopicTrie::new();

    for i in 0..100 {
        let (tx, _rx) = mpsc::channel(1);
        trie.insert(
            &format!("service{i}.events.created"),
            SubscriptionTarget {
                consumer_id: format!("consumer-{i}"),
                sub_id: format!("sub-{i}"),
                group: None,
                filter: None,
                deliver_tx: tx,
                partition_key: None,
            },
        );
    }

    // Wildcard subscriptions
    for pattern in ["service1.*", "service2.>", ">"] {
        let (tx, _rx) = mpsc::channel(1);
        trie.insert(
            pattern,
            SubscriptionTarget {
                consumer_id: format!("wc-{pattern}"),
                sub_id: format!("sub-wc-{pattern}"),
                group: None,
                filter: None,
                deliver_tx: tx,
                partition_key: None,
            },
        );
    }

    let mut group = c.benchmark_group("topic_trie");
    group.throughput(Throughput::Elements(1));

    group.bench_function("exact_match", |b| {
        b.iter(|| black_box(trie.resolve("service50.events.created")));
    });

    group.bench_function("wildcard_single", |b| {
        b.iter(|| black_box(trie.resolve("service1.anything")));
    });

    group.bench_function("wildcard_multi", |b| {
        b.iter(|| black_box(trie.resolve("service2.deep.nested.topic")));
    });

    group.bench_function("global_wildcard", |b| {
        b.iter(|| black_box(trie.resolve("any.random.topic")));
    });

    group.bench_function("no_match", |b| {
        b.iter(|| black_box(trie.resolve("nonexistent.topic")));
    });

    group.finish();
}

// ─── Content Filter Benchmarks ───

fn bench_filter(c: &mut Criterion) {
    use pulse_broker::routing::filter::CompiledFilter;

    let mut group = c.benchmark_group("content_filter");
    group.throughput(Throughput::Elements(1));

    let simple = CompiledFilter::compile("amount > 1000").unwrap();
    let complex =
        CompiledFilter::compile("amount > 1000 AND status == \"active\" AND region != \"US\"")
            .unwrap();
    let func = CompiledFilter::compile("contains(name, \"test\")").unwrap();

    let payload = rmpv::Value::Map(vec![
        (
            rmpv::Value::String("amount".into()),
            rmpv::Value::Integer(1500.into()),
        ),
        (
            rmpv::Value::String("status".into()),
            rmpv::Value::String("active".into()),
        ),
        (
            rmpv::Value::String("region".into()),
            rmpv::Value::String("VN".into()),
        ),
        (
            rmpv::Value::String("name".into()),
            rmpv::Value::String("test-service".into()),
        ),
    ]);

    group.bench_function("simple_comparison", |b| {
        b.iter(|| black_box(simple.evaluate(&payload)));
    });

    group.bench_function("complex_and_chain", |b| {
        b.iter(|| black_box(complex.evaluate(&payload)));
    });

    group.bench_function("function_contains", |b| {
        b.iter(|| black_box(func.evaluate(&payload)));
    });

    group.bench_function("compile_simple", |b| {
        b.iter(|| black_box(CompiledFilter::compile("amount > 1000").unwrap()));
    });

    group.finish();
}

// ─── End-to-End Throughput ───

fn bench_e2e_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, dispatcher) = setup_dispatcher(&rt);
    let payload = small_payload();

    let mut group = c.benchmark_group("e2e_throughput");

    // Batch of 100 events
    group.throughput(Throughput::Elements(100));
    group.bench_function("batch_100", |b| {
        b.to_async(&rt).iter(|| {
            let d = dispatcher.clone();
            let p = payload.clone();
            async move {
                for _ in 0..100 {
                    black_box(d.ingest(MessageId::new(), &p).await);
                }
            }
        });
    });

    // Batch of 1000 events
    group.throughput(Throughput::Elements(1000));
    group.bench_function("batch_1000", |b| {
        b.to_async(&rt).iter(|| {
            let d = dispatcher.clone();
            let p = payload.clone();
            async move {
                for _ in 0..1000 {
                    black_box(d.ingest(MessageId::new(), &p).await);
                }
            }
        });
    });

    group.finish();
}

// ─── Frame Encode/Decode Benchmarks ───

fn bench_frame_codec(c: &mut Criterion) {
    use pulse_protocol::Frame;

    let mut group = c.benchmark_group("frame_codec");
    group.throughput(Throughput::Elements(1));

    let pub_frame = Frame::publish(MessageId::new(), small_payload());
    let encoded = pub_frame.encode().unwrap();

    group.bench_function("encode_pub", |b| {
        let frame = pub_frame.clone();
        b.iter(|| black_box(frame.encode().unwrap()));
    });

    group.bench_function("decode_pub", |b| {
        let data = encoded.clone();
        b.iter(|| black_box(Frame::decode(&data, 1_048_576).unwrap()));
    });

    group.finish();
}

// ─── Tiered Dedup: Balanced vs Durable ───

fn bench_tiered_dedup(c: &mut Criterion) {
    use pulse_broker::config::DurabilityMode;

    let mut group = c.benchmark_group("tiered_dedup");
    group.throughput(Throughput::Elements(1));

    // Balanced mode (bloom-only)
    {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            pulse_broker::storage::state_db::StateDb::open(dir.path().join("state")).unwrap(),
        );
        let dedup = DedupEngine::tiered(db, DurabilityMode::Balanced);

        group.bench_function("balanced_check", |b| {
            b.iter(|| black_box(dedup.check(&MessageId::new()).unwrap()));
        });

        group.bench_function("balanced_insert", |b| {
            b.iter(|| {
                black_box(dedup.insert(&MessageId::new(), "bench.topic").unwrap());
            });
        });
    }

    // Durable mode (bloom + sled)
    {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            pulse_broker::storage::state_db::StateDb::open(dir.path().join("state")).unwrap(),
        );
        let dedup = DedupEngine::tiered(db, DurabilityMode::Durable);

        group.bench_function("durable_check", |b| {
            b.iter(|| black_box(dedup.check(&MessageId::new()).unwrap()));
        });

        group.bench_function("durable_insert", |b| {
            b.iter(|| {
                black_box(dedup.insert(&MessageId::new(), "bench.topic").unwrap());
            });
        });
    }

    group.finish();
}

// ─── Batch Pipeline: Balanced Mode ───

fn bench_batch_pipeline(c: &mut Criterion) {
    use pulse_broker::config::DurabilityMode;
    use pulse_broker::pipeline::batch::{BatchIngestMessage, BatchPipeline};

    let rt = tokio::runtime::Runtime::new().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_testing(dir.path().to_path_buf());
    let state_db = Arc::new(
        pulse_broker::storage::state_db::StateDb::open(config.data_dir.join("state")).unwrap(),
    );
    let wal = rt.block_on(async {
        WalWriter::open(config.data_dir.join("wal"), &config.wal)
            .await
            .unwrap()
    });
    let dedup = Arc::new(DedupEngine::tiered(state_db, DurabilityMode::Balanced));

    let (tx, rx) = mpsc::channel(8192);
    // Must spawn inside runtime context
    rt.block_on(async {
        BatchPipeline::spawn(dedup, wal, rx, None, 2, 500);
    });

    let payload = small_payload();

    let mut group = c.benchmark_group("batch_pipeline_balanced");

    group.throughput(Throughput::Elements(100));
    group.bench_function("batch_100", |b| {
        b.to_async(&rt).iter(|| {
            let tx = tx.clone();
            let p = payload.clone();
            async move {
                let mut rxs = Vec::with_capacity(100);
                for _ in 0..100 {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let _ = tx
                        .send(BatchIngestMessage {
                            msg_id: MessageId::new(),
                            pub_payload: p.clone(),
                            namespace: "default".into(),
                            reply_tx,
                        })
                        .await;
                    rxs.push(reply_rx);
                }
                for rx in rxs {
                    black_box(rx.await.ok());
                }
            }
        });
    });

    group.throughput(Throughput::Elements(1000));
    group.bench_function("batch_1000", |b| {
        b.to_async(&rt).iter(|| {
            let tx = tx.clone();
            let p = payload.clone();
            async move {
                let mut rxs = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let _ = tx
                        .send(BatchIngestMessage {
                            msg_id: MessageId::new(),
                            pub_payload: p.clone(),
                            namespace: "default".into(),
                            reply_tx,
                        })
                        .await;
                    rxs.push(reply_rx);
                }
                for rx in rxs {
                    black_box(rx.await.ok());
                }
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ingest,
    bench_ingest_payload_sizes,
    bench_wal_write,
    bench_bloom_filter,
    bench_topic_trie,
    bench_filter,
    bench_e2e_throughput,
    bench_frame_codec,
    bench_tiered_dedup,
    bench_batch_pipeline,
);
criterion_main!(benches);
