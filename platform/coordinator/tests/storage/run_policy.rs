use super::*;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UnixStream},
    sync::{oneshot, Notify},
    time::{sleep, timeout, Duration},
};

async fn read_line<R: AsyncRead + Unpin>(reader: &mut R, raw: &mut Vec<u8>) -> io::Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let byte = reader.read_u8().await?;
        raw.push(byte);
        if byte == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
                return Ok(line);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid RESP line",
            ));
        }
        line.push(byte);
    }
}

async fn read_request(
    stream: &mut tokio::net::tcp::OwnedReadHalf,
) -> io::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut raw = Vec::new();
    if stream.read_u8().await? != b'*' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP request is not an array",
        ));
    }
    raw.push(b'*');
    let count = std::str::from_utf8(&read_line(stream, &mut raw).await?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 RESP count"))?
        .parse::<usize>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid RESP count"))?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let marker = stream.read_u8().await?;
        raw.push(marker);
        if marker != b'$' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RESP argument is not bulk",
            ));
        }
        let length = std::str::from_utf8(&read_line(stream, &mut raw).await?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 RESP length"))?
            .parse::<usize>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid RESP length"))?;
        let mut value = vec![0; length];
        stream.read_exact(&mut value).await?;
        raw.extend_from_slice(&value);
        let mut crlf = [0; 2];
        stream.read_exact(&mut crlf).await?;
        if crlf != *b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid RESP bulk terminator",
            ));
        }
        raw.extend_from_slice(&crlf);
        values.push(value);
    }
    Ok((raw, values))
}

async fn proxy_one_connection(
    listener: TcpListener,
    redis_socket: String,
    armed: Arc<AtomicBool>,
    environment_field: String,
    blocked: oneshot::Sender<()>,
    release: Arc<Notify>,
) -> io::Result<()> {
    let (client, _) = listener.accept().await?;
    let upstream = UnixStream::connect(redis_socket).await?;
    let (mut client_read, mut client_write) = client.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let responses =
        tokio::spawn(async move { tokio::io::copy(&mut upstream_read, &mut client_write).await });
    let mut blocked = Some(blocked);
    while let Ok((request, arguments)) = read_request(&mut client_read).await {
        // EVAL lists its key count before KEYS and ARGV. Match ARGV[3]
        // independently of how many keys the native CAS updates atomically.
        let environment_argument = arguments
            .get(2)
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<usize>().ok())
            .and_then(|keys| keys.checked_add(5));
        let is_target = arguments.first().is_some_and(|value| value == b"EVAL")
            && environment_argument
                .and_then(|index| arguments.get(index))
                .is_some_and(|value| value == environment_field.as_bytes());
        if is_target && armed.swap(false, Ordering::SeqCst) {
            let _ = blocked.take().expect("one intercept").send(());
            release.notified().await;
        }
        upstream_write.write_all(&request).await?;
    }
    responses.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn timeout_cas_retry_rechecks_deadline_after_a_conflict() {
    let rig = common::Redis::new().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("redis://{}", listener.local_addr().unwrap());
    let id = "deadline-cas";
    let field = format!("environment:{id}");
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let armed = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn(proxy_one_connection(
        listener,
        rig.url.strip_prefix("redis+unix://").unwrap().into(),
        armed.clone(),
        field.clone(),
        blocked_tx,
        release.clone(),
    ));
    let store = adx_coordinator::storage::RedisStore::connect(
        &proxy_url,
        "deadline-cas",
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut spec = spec(id);
    spec.run_policy = Some(RunPolicy {
        timeout_seconds: 60,
        action: RunExpiryAction::Delete,
    });
    let assignment = Assignment {
        environment_id: id.into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec.clone(), assignment.clone())
        .await
        .unwrap();
    session.commit(running(spec, assignment)).await.unwrap();
    session.set_environment_timeout(id, 1, false).await.unwrap();
    let deadline = session
        .environment_control(id)
        .await
        .unwrap()
        .expires_at_unix_ms;

    // Arm only the renewal CAS, after all setup traffic has traversed the proxy.
    armed.store(true, Ordering::SeqCst);
    let renewal = tokio::spawn({
        let session = session.clone();
        async move { session.set_environment_timeout(id, 60, false).await }
    });
    timeout(Duration::from_secs(3), blocked_rx)
        .await
        .unwrap()
        .unwrap();

    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut direct = client.get_multiplexed_async_connection().await.unwrap();
    let key = "adx:{deadline-cas}:control:v1";
    let header: String = redis::cmd("HGET")
        .arg(key)
        .arg("header")
        .query_async(&mut direct)
        .await
        .unwrap();
    let mut header: serde_json::Value = serde_json::from_str(&header).unwrap();
    header["revision"] = header["revision"]
        .as_u64()
        .unwrap()
        .checked_add(1)
        .unwrap()
        .into();
    let _: () = redis::cmd("HSET")
        .arg(key)
        .arg("header")
        .arg(serde_json::to_string(&header).unwrap())
        .query_async(&mut direct)
        .await
        .unwrap();
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    if now <= deadline {
        sleep(Duration::from_millis(deadline - now + 30)).await;
    }
    release.notify_one();
    assert!(
        matches!(
            timeout(Duration::from_secs(3), renewal)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::Conflict)
        ),
        "expired renewal must fail after CAS retry"
    );
    assert_eq!(
        session
            .environment_control(id)
            .await
            .unwrap()
            .expires_at_unix_ms,
        deadline,
        "expired renewal must not change the deadline"
    );
    task.abort();
}
