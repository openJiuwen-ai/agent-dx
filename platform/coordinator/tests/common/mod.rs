use adx_coordinator::storage::RedisStore;
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
pub struct Redis {
    dir: tempfile::TempDir,
    child: Option<Child>,
    pub url: String,
    binary: PathBuf,
}
impl Redis {
    pub async fn new() -> Self {
        let binary = std::env::var_os("ADX_TEST_REDIS_SERVER")
            .expect("set ADX_TEST_REDIS_SERVER to an actual redis-server executable")
            .into();
        // Keep Unix socket paths under the macOS/Linux sockaddr_un limit.
        let dir = tempfile::Builder::new()
            .prefix("adx-store-")
            .tempdir_in("/tmp")
            .unwrap();
        let url = format!("redis+unix://{}", dir.path().join("redis.sock").display());
        let mut redis = Self {
            dir,
            child: None,
            url,
            binary,
        };
        redis.start().await;
        redis
    }
    pub async fn start(&mut self) {
        assert!(self.child.is_none());
        let socket = self.dir.path().join("redis.sock");
        let _ = std::fs::remove_file(&socket);
        self.child = Some(
            Command::new(&self.binary)
                .args([
                    "--port",
                    "0",
                    "--save",
                    "",
                    "--appendonly",
                    "yes",
                    "--appendfsync",
                    "always",
                    "--daemonize",
                    "no",
                ])
                .arg("--unixsocket")
                .arg(&socket)
                .arg("--dir")
                .arg(self.dir.path())
                .arg("--logfile")
                .arg(self.dir.path().join("redis.log"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        for _ in 0..200 {
            assert!(
                self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "Redis exited before readiness"
            );
            if RedisStore::connect(&self.url, "probe", Duration::from_millis(100))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("Redis readiness timed out");
    }
    pub fn crash(&mut self) {
        let mut child = self.child.take().expect("Redis running");
        child.kill().unwrap();
        child.wait().unwrap();
    }
    pub async fn store(&self) -> RedisStore {
        RedisStore::connect(&self.url, "test", Duration::from_millis(300))
            .await
            .unwrap()
    }
}
impl Drop for Redis {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Ok(out) = std::env::var("ADX_TEST_EVIDENCE") {
            let name = self.dir.path().file_name().unwrap();
            let target = PathBuf::from(out).join(name);
            let _ = std::fs::create_dir_all(&target);
            let _ = std::fs::copy(self.dir.path().join("redis.log"), target.join("redis.log"));
        }
    }
}
