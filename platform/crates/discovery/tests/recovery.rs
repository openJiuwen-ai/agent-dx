mod support;

use adx_core::Error;
use adx_discovery::RedisDiscovery;
use std::{sync::atomic::Ordering, time::Duration};
use support::{RedisPeer, Reply};

#[tokio::test]
async fn retry_uses_the_remaining_budget_instead_of_a_fresh_timeout() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_millis(600)).unwrap();
    reader.lookup().await.unwrap();
    *peer.state.reply.lock().unwrap() = Reply::CloseAfter(Duration::from_millis(400));
    let started = tokio::time::Instant::now();
    assert!(matches!(reader.lookup().await, Err(Error::Unavailable(_))));
    assert_eq!(peer.state.queries.load(Ordering::SeqCst), 3);
    assert!(started.elapsed() < Duration::from_millis(750));
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn connection_setup_is_included_in_the_deadline() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    *peer.state.handshake_delay.lock().unwrap() = Duration::from_secs(10);
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_millis(80)).unwrap();
    let started = tokio::time::Instant::now();
    assert!(matches!(reader.lookup().await, Err(Error::Unavailable(_))));
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(peer.state.queries.load(Ordering::SeqCst), 0);
    *peer.state.handshake_delay.lock().unwrap() = Duration::ZERO;
    reader.lookup().await.unwrap();
}

#[tokio::test]
async fn schema_address_and_epoch_validation_fail_closed_without_retries() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_secs(1)).unwrap();
    for (schema, epoch, address, header) in [
        (2, 7, "http://127.0.0.1:9000", r#"{"epoch":7}"#),
        (1, 0, "http://127.0.0.1:9000", r#"{"epoch":0}"#),
        (1, 7, "http://127.0.0.1:9000", r#"{"epoch":8}"#),
        (1, 7, "http://user@127.0.0.1:9000", r#"{"epoch":7}"#),
        (1, 7, "http://127.0.0.1:9000", "malformed"),
    ] {
        let endpoint =
            serde_json::json!({"schema":schema,"epoch":epoch,"address":address}).to_string();
        *peer.state.reply.lock().unwrap() = Reply::Raw(
            format!(
                "*2\r\n${}\r\n{endpoint}\r\n${}\r\n{header}\r\n",
                endpoint.len(),
                header.len()
            )
            .into_bytes(),
        );
        assert!(reader.lookup().await.is_err());
    }
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(peer.state.queries.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn idle_connection_close_recovers_in_the_same_lookup() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_secs(1)).unwrap();
    let expected = reader.lookup().await.unwrap();
    peer.disconnect().await;
    assert_eq!(reader.lookup().await.unwrap(), expected);
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
    reader.lookup().await.unwrap();
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn concurrent_idle_close_uses_one_replacement() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_secs(1)).unwrap();
    reader.lookup().await.unwrap();
    peer.disconnect().await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let reader = reader.clone();
        tasks.spawn(async move { reader.lookup().await });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap().unwrap();
    }
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn repeated_transport_failure_stops_after_one_retry() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    *peer.state.reply.lock().unwrap() = Reply::Close;
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_secs(1)).unwrap();
    assert!(matches!(reader.lookup().await, Err(Error::Unavailable(_))));
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
    assert_eq!(peer.state.queries.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn records_and_permanent_errors_do_not_trigger_reconnects() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_secs(1)).unwrap();
    for reply in [
        b"-NOAUTH Authentication required\r\n".as_slice(),
        b"-NOPERM command denied\r\n".as_slice(),
        b"-WRONGTYPE invalid data\r\n".as_slice(),
        b"*2\r\n$-1\r\n$-1\r\n".as_slice(),
        b"*2\r\n$1\r\nx\r\n$-1\r\n".as_slice(),
    ] {
        *peer.state.reply.lock().unwrap() = Reply::Raw(reply.to_vec());
        assert!(reader.lookup().await.is_err());
    }
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(peer.state.queries.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn query_timeout_is_bounded_and_next_call_can_recover() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_millis(80)).unwrap();
    *peer.state.reply.lock().unwrap() = Reply::Delay(Duration::from_secs(10));
    let started = tokio::time::Instant::now();
    assert!(matches!(reader.lookup().await, Err(Error::Unavailable(_))));
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(peer.state.queries.load(Ordering::SeqCst), 1);
    *peer.state.reply.lock().unwrap() = Reply::Healthy;
    reader.lookup().await.unwrap();
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_query_does_not_poison_following_lookups() {
    let peer = RedisPeer::new("http://127.0.0.1:9000").await;
    *peer.state.reply.lock().unwrap() = Reply::Delay(Duration::from_secs(10));
    let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_secs(1)).unwrap();
    let task = {
        let reader = reader.clone();
        tokio::spawn(async move { reader.lookup().await })
    };
    peer.wait_for_queries(1).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    *peer.state.reply.lock().unwrap() = Reply::Healthy;
    reader.lookup().await.unwrap();
    assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
}
