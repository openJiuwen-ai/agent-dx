use rrt_daemon::pb::filesystem_client::FilesystemClient;
use rrt_daemon::pb::*;
use std::time::Duration;

async fn start() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        rrt_daemon::serve_with_listener(listener).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    format!("http://{addr}")
}

#[tokio::test]
async fn filesystem_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_string_lossy().into_owned();
    let mut c = FilesystemClient::connect(start().await).await.unwrap();

    // write_file (with create_parents) + read_file
    let f = format!("{base}/sub/a.txt");
    c.write_file(WriteFileRequest {
        path: f.clone(),
        content: b"hello-fs".to_vec(),
        create_parents: true,
    })
    .await
    .unwrap();
    let got = c
        .read_file(ReadFileRequest { path: f.clone() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.content, b"hello-fs");

    // stat
    let st = c
        .stat(StatRequest { path: f.clone() })
        .await
        .unwrap()
        .into_inner();
    assert!(st.exists && !st.is_dir && st.size == 8);
    let st2 = c
        .stat(StatRequest {
            path: format!("{base}/nope"),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!st2.exists);

    // list_dir
    let ld = c
        .list_dir(ListDirRequest {
            path: format!("{base}/sub"),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ld.entries.len(), 1);
    assert_eq!(ld.entries[0].name, "a.txt");

    // make_dir
    c.make_dir(MakeDirRequest {
        path: format!("{base}/d1/d2"),
        parents: true,
    })
    .await
    .unwrap();
    let st3 = c
        .stat(StatRequest {
            path: format!("{base}/d1/d2"),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(st3.exists && st3.is_dir);

    // move
    c.r#move(MoveRequest {
        from: f.clone(),
        to: format!("{base}/sub/b.txt"),
    })
    .await
    .unwrap();
    assert!(
        !c.stat(StatRequest { path: f.clone() })
            .await
            .unwrap()
            .into_inner()
            .exists
    );
    assert!(
        c.stat(StatRequest {
            path: format!("{base}/sub/b.txt")
        })
        .await
        .unwrap()
        .into_inner()
        .exists
    );

    // remove (recursive dir)
    c.remove(RemoveRequest {
        path: format!("{base}/sub"),
        recursive: true,
    })
    .await
    .unwrap();
    assert!(
        !c.stat(StatRequest {
            path: format!("{base}/sub")
        })
        .await
        .unwrap()
        .into_inner()
        .exists
    );
}
