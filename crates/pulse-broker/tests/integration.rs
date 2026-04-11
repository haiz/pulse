use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use pulse_protocol::*;

use pulse_broker::broker::BrokerHandle;
use pulse_broker::config::BrokerConfig;
use pulse_broker::delivery::manager::DeliveryManager;
use pulse_broker::pipeline::dedup::DedupEngine;
use pulse_broker::pipeline::dispatcher::Dispatcher;
use pulse_broker::routing::Router;
use pulse_broker::server::listener::Listener;
use pulse_broker::storage::state_db::StateDb;
use pulse_broker::storage::sharded_wal::ShardedWalWriter;
use pulse_broker::storage::wal;

/// Start a broker on a random port and return the address + join handle.
async fn start_broker() -> (std::net::SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_testing(dir.path().to_path_buf());

    let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
    let wal_dir = config.data_dir.join("wal");
    let _ = wal::replay_wal_sharded(&wal_dir, config.wal.shards).await.unwrap();
    let wal = ShardedWalWriter::open(wal_dir, &config.wal, config.wal.shards).await.unwrap();

    let dedup = DedupEngine::new(state_db.clone());
    let delivery = DeliveryManager::new(&config.delivery, None);
    let router = Arc::new(Router::new());
    let (dispatch_tx, dispatch_rx) = mpsc::channel(1024);
    Dispatcher::spawn(dedup, wal, dispatch_rx, Some(router.clone()));

    // Bind to port 0 for random available port
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let broker = BrokerHandle::new(config, dispatch_tx, state_db, delivery, router);
    let listener = Listener::bind(addr, broker).await.unwrap();

    // Get the actual bound address
    let local_addr = listener.local_addr();

    tokio::spawn(async move {
        let _ = listener.run().await;
    });

    // Give the listener a moment to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    (local_addr, dir)
}

/// Connect a client and perform the CONNECT handshake.
async fn connect_client(addr: std::net::SocketAddr) -> Framed<TcpStream, PulseCodec> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, PulseCodec::new());

    // Send CONNECT
    let connect = Frame::connect(
        MessageId::new(),
        ConnectPayload {
            service_id: "test-service".into(),
            namespace: "test".into(),
            timestamp: 0,
            hmac: vec![],
            client_ver: None,
            max_inflight: None,
            codec: None,
        },
    );
    framed.send(connect).await.unwrap();

    // Receive CONNACK
    let connack = framed.next().await.unwrap().unwrap();
    assert_eq!(connack.msg_type, MessageType::ConnAck);
    if let Payload::ConnAck(ca) = &connack.payload {
        assert_eq!(ca.status, "ok");
    } else {
        panic!("expected ConnAck payload");
    }

    framed
}

#[tokio::test]
async fn connect_and_receive_connack() {
    let (addr, _dir) = start_broker().await;
    let _client = connect_client(addr).await;
}

#[tokio::test]
async fn publish_and_receive_ack() {
    let (addr, _dir) = start_broker().await;
    let mut client = connect_client(addr).await;

    // Send PUB
    let msg_id = MessageId::new();
    let pub_frame = Frame::publish(
        msg_id,
        PubPayload {
            topic: "order.created".into(),
            data: rmpv::Value::String("hello".into()),
            headers: HashMap::new(),
            produced_at: None,
            delivery: None,
        },
    );
    client.send(pub_frame).await.unwrap();

    // Receive ACK
    let ack = client.next().await.unwrap().unwrap();
    assert_eq!(ack.msg_type, MessageType::Ack);
    if let Payload::Ack(a) = &ack.payload {
        assert_eq!(a.status, AckStatus::Stored);
    } else {
        panic!("expected Ack payload, got: {:?}", ack.payload);
    }
}

#[tokio::test]
async fn publish_duplicate_returns_duplicate_ack() {
    let (addr, _dir) = start_broker().await;
    let mut client = connect_client(addr).await;

    let msg_id = MessageId::new();
    let pub_payload = PubPayload {
        topic: "order.created".into(),
        data: rmpv::Value::String("hello".into()),
        headers: HashMap::new(),
        produced_at: None,
        delivery: None,
    };

    // First publish
    client
        .send(Frame::publish(msg_id, pub_payload.clone()))
        .await
        .unwrap();
    let ack1 = client.next().await.unwrap().unwrap();
    assert_eq!(ack1.msg_type, MessageType::Ack);
    if let Payload::Ack(a) = &ack1.payload {
        assert_eq!(a.status, AckStatus::Stored);
    }

    // Second publish with same msg_id -> duplicate
    client
        .send(Frame::publish(msg_id, pub_payload))
        .await
        .unwrap();
    let ack2 = client.next().await.unwrap().unwrap();
    assert_eq!(ack2.msg_type, MessageType::Ack);
    if let Payload::Ack(a) = &ack2.payload {
        assert_eq!(a.status, AckStatus::Duplicate);
    }
}

#[tokio::test]
async fn ping_pong() {
    let (addr, _dir) = start_broker().await;
    let mut client = connect_client(addr).await;

    // Send PING
    let ping_id = MessageId::new();
    client.send(Frame::ping(ping_id)).await.unwrap();

    // Receive PONG
    let pong = client.next().await.unwrap().unwrap();
    assert_eq!(pong.msg_type, MessageType::Pong);
    assert_eq!(pong.msg_id, ping_id);
}

#[tokio::test]
async fn multiple_publishes() {
    let (addr, _dir) = start_broker().await;
    let mut client = connect_client(addr).await;

    for i in 0..10 {
        let pub_frame = Frame::publish(
            MessageId::new(),
            PubPayload {
                topic: format!("topic.{i}"),
                data: rmpv::Value::Integer(i.into()),
                headers: HashMap::new(),
                produced_at: None,
                delivery: None,
            },
        );
        client.send(pub_frame).await.unwrap();

        let ack = client.next().await.unwrap().unwrap();
        assert_eq!(ack.msg_type, MessageType::Ack);
        if let Payload::Ack(a) = &ack.payload {
            assert_eq!(a.status, AckStatus::Stored);
        }
    }
}

#[tokio::test]
async fn multiple_clients() {
    let (addr, _dir) = start_broker().await;

    let mut clients = Vec::new();
    for _ in 0..5 {
        clients.push(connect_client(addr).await);
    }

    // Each client publishes
    for client in &mut clients {
        let pub_frame = Frame::publish(
            MessageId::new(),
            PubPayload {
                topic: "shared.topic".into(),
                data: rmpv::Value::Nil,
                headers: HashMap::new(),
                produced_at: None,
                delivery: None,
            },
        );
        client.send(pub_frame).await.unwrap();
        let ack = client.next().await.unwrap().unwrap();
        assert_eq!(ack.msg_type, MessageType::Ack);
    }
}

#[tokio::test]
async fn end_to_end_pub_sub_delivery() {
    let (addr, _dir) = start_broker().await;

    // Subscriber connects and subscribes to "order.*"
    let mut subscriber = connect_client(addr).await;
    let sub_frame = Frame::sub(
        MessageId::new(),
        SubPayload {
            topic: "order.*".into(),
            group: None,
            filter: None,
            position: None,
            sub_id: "sub-1".into(),
        },
    );
    subscriber.send(sub_frame).await.unwrap();

    // Give subscription time to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publisher connects and publishes to "order.created"
    let mut publisher = connect_client(addr).await;
    let pub_msg_id = MessageId::new();
    let pub_frame = Frame::publish(
        pub_msg_id,
        PubPayload {
            topic: "order.created".into(),
            data: rmpv::Value::String("test-order".into()),
            headers: HashMap::new(),
            produced_at: None,
            delivery: None,
        },
    );
    publisher.send(pub_frame).await.unwrap();

    // Publisher gets ACK
    let ack = publisher.next().await.unwrap().unwrap();
    assert_eq!(ack.msg_type, MessageType::Ack);

    // Subscriber should receive the event
    let delivered = tokio::time::timeout(Duration::from_secs(2), subscriber.next())
        .await
        .expect("subscriber should receive event within 2s")
        .unwrap()
        .unwrap();

    assert_eq!(delivered.msg_type, MessageType::Pub);
    if let Payload::Pub(p) = &delivered.payload {
        assert_eq!(p.topic, "order.created");
        assert_eq!(p.data, rmpv::Value::String("test-order".into()));
    } else {
        panic!("expected PUB payload");
    }
}

#[tokio::test]
async fn subscriber_does_not_receive_unmatched_topic() {
    let (addr, _dir) = start_broker().await;

    // Subscriber subscribes to "payment.*"
    let mut subscriber = connect_client(addr).await;
    let sub_frame = Frame::sub(
        MessageId::new(),
        SubPayload {
            topic: "payment.*".into(),
            group: None,
            filter: None,
            position: None,
            sub_id: "sub-1".into(),
        },
    );
    subscriber.send(sub_frame).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publisher publishes to "order.created" (doesn't match "payment.*")
    let mut publisher = connect_client(addr).await;
    publisher
        .send(Frame::publish(
            MessageId::new(),
            PubPayload {
                topic: "order.created".into(),
                data: rmpv::Value::Nil,
                headers: HashMap::new(),
                produced_at: None,
                delivery: None,
            },
        ))
        .await
        .unwrap();

    // Publisher gets ACK
    let ack = publisher.next().await.unwrap().unwrap();
    assert_eq!(ack.msg_type, MessageType::Ack);

    // Subscriber should NOT receive anything (timeout expected)
    let result = tokio::time::timeout(Duration::from_millis(200), subscriber.next()).await;
    assert!(
        result.is_err(),
        "subscriber should not receive unmatched event"
    );
}

#[tokio::test]
async fn multiple_subscribers_receive_same_event() {
    let (addr, _dir) = start_broker().await;

    // Two subscribers on "order.>"
    let mut sub1 = connect_client(addr).await;
    let mut sub2 = connect_client(addr).await;

    for (client, sub_id) in [(&mut sub1, "s1"), (&mut sub2, "s2")] {
        client
            .send(Frame::sub(
                MessageId::new(),
                SubPayload {
                    topic: "order.>".into(),
                    group: None,
                    filter: None,
                    position: None,
                    sub_id: sub_id.into(),
                },
            ))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish one event
    let mut publisher = connect_client(addr).await;
    publisher
        .send(Frame::publish(
            MessageId::new(),
            PubPayload {
                topic: "order.created".into(),
                data: rmpv::Value::Integer(42.into()),
                headers: HashMap::new(),
                produced_at: None,
                delivery: None,
            },
        ))
        .await
        .unwrap();
    let _ = publisher.next().await; // ACK

    // Both subscribers should receive it
    for sub in [&mut sub1, &mut sub2] {
        let delivered = tokio::time::timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("subscriber should receive event")
            .unwrap()
            .unwrap();
        assert_eq!(delivered.msg_type, MessageType::Pub);
    }
}
