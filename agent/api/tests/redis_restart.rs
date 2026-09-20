#[path = "../../tests/pool_fixture.rs"]
mod pool_fixture;
// Real Redis SIGKILL/AOF recovery with independent clients and HTTP Dispatchers.
// Sandbox is simulated; this is not a Platform, replication or power-loss test.
use adx_agent_api::{
    dispatcher::DispatcherClient,
    managed::ManagedService,
    management::{InlineProfile, InlineService, Options},
    Error,
};
use adx_agent_core::{inline::*, routing::DispatcherMember, sandbox::*, *};
use adx_agent_store::{AgentState, RedisRepository, Repository};
use adx_dispatcher::{server, Config, Dispatcher};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const TOKEN: &str = "redis-restart-test-service-credential-32";
const TENANT: &str = "recovery-tenant";

#[derive(Default)]
struct Backend {
    instances: Mutex<BTreeMap<String, SandboxObservation>>,
    allocations: AtomicUsize,
    deletes: AtomicUsize,
}

#[async_trait]
impl Sandbox for Backend {
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let key = encode_key(&[&request.tenant, &request.id]);
        let mut instances = self.instances.lock().await;
        Ok(instances
            .entry(key)
            .or_insert_with(|| {
                self.allocations.fetch_add(1, Ordering::SeqCst);
                SandboxObservation {
                    id: request.id.clone(),
                    tenant: request.tenant.clone(),
                    phase: SandboxPhase::Running,
                    ready: true,
                    runtime_id: Some(format!("{}-physical-1", request.id)),
                    message: None,
                }
            })
            .clone())
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        Ok(self
            .instances
            .lock()
            .await
            .get(&encode_key(&[tenant, id]))
            .cloned())
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        let observation = SandboxObservation {
            id: id.into(),
            tenant: tenant.into(),
            phase: SandboxPhase::Deleted,
            ready: false,
            runtime_id: None,
            message: None,
        };
        self.instances
            .lock()
            .await
            .insert(encode_key(&[tenant, id]), observation.clone());
        Ok(observation)
    }
}

struct RedisProcess(Option<Child>);
impl RedisProcess {
    fn start(binary: &Path, dir: &Path, port: u16, run: &str) -> Self {
        let log = std::fs::File::create(dir.join(format!("redis-{run}.log"))).unwrap();
        let child = Command::new(binary)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--protected-mode",
                "yes",
                "--save",
                "",
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                "--dir",
            ])
            .arg(dir)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        Self(Some(child))
    }
    fn crash(&mut self) {
        let mut child = self.0.take().unwrap();
        child.kill().unwrap(); // SIGKILL, not SHUTDOWN or a graceful final save.
        assert!(!child.wait().unwrap().success());
    }
}
impl Drop for RedisProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn repository(url: &str, namespace: &str) -> Arc<RedisRepository> {
    Arc::new(
        RedisRepository::connect(url, namespace, Duration::from_millis(300))
            .await
            .unwrap(),
    )
}
async fn wait_redis(process: &mut RedisProcess, url: &str, namespace: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(
                process.0.as_mut().unwrap().try_wait().unwrap().is_none(),
                "Redis exited; inspect retained log"
            );
            if RedisRepository::connect(url, namespace, Duration::from_millis(100))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
async fn dispatcher(
    repo: Arc<RedisRepository>,
    backend: Arc<Backend>,
    id: &str,
) -> (Server, DispatcherMember) {
    let boot = uuid::Uuid::new_v4().to_string();
    let dispatcher = Arc::new(
        Dispatcher::new(
            AgentState::new(repo),
            backend,
            boot.clone(),
            Config::default(),
        )
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let member = DispatcherMember {
        node_id: id.into(),
        boot_id: boot,
        address: format!("http://{}", listener.local_addr().unwrap()),
    };
    let router = server::router(dispatcher, TOKEN, Arc::new(AtomicBool::new(true))).unwrap();
    (
        Server(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap()
        })),
        member,
    )
}
fn managed(repo: Arc<RedisRepository>) -> ManagedService {
    ManagedService::new(
        AgentState::new(repo.clone()),
        Arc::new(
            DispatcherClient::new(repo, TOKEN.into(), Duration::from_secs(3), None, true).unwrap(),
        ),
    )
}
fn inline(backend: Arc<Backend>) -> InlineService {
    InlineService::new(
        backend,
        Options {
            profiles: vec![InlineProfile {
                sandbox_type: SandboxType::Docker,
                request_image: Some("fixture:1".into()),
                image: "fixture:1".into(),
                isolation_runtime: "runc".into(),
                request_user: None,
                working_dir: "/".into(),
                default_entrypoint: vec!["/start".into()],
                service: vec![],
                preinstalled_workspace: None,
                preinstalled_mounts: vec![],
            }],
            backend_timeout: Duration::from_secs(1),
            max_inflight: 8,
        },
    )
    .unwrap()
}
fn request(name: &str) -> CreateRequest {
    serde_json::from_value(serde_json::json!({"name":name,"namespace":"default","runtime_spec":{"runtime":"Python3.11","sandbox_type":"docker","rootfs":{"imageurl":"fixture:1"}}})).unwrap()
}
fn unavailable<T: std::fmt::Debug>(result: adx_agent_api::Result<T>) {
    assert!(
        matches!(
            result,
            Err(Error::Unavailable(_) | Error::OutcomeUnknown(_))
        ),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns its own Redis 7.2.5 using ADX_AGENT_TEST_REDIS_SERVER; SIGKILLs only that child"]
async fn redis_aof_restart_preserves_bindings_and_rejects_outage_writes() {
    let binary = PathBuf::from(
        std::env::var("ADX_AGENT_TEST_REDIS_SERVER").expect("provide Redis 7.2.5 binary"),
    );
    let version = Command::new(&binary).arg("--version").output().unwrap();
    let version = String::from_utf8(version.stdout).unwrap();
    assert!(version.contains("v=7.2.5"), "{version}");
    let namespace = format!("aof-{}", uuid::Uuid::new_v4());
    let root = std::env::var_os("ADX_AGENT_TEST_EVIDENCE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../out/agent-v2-p6/redis-restart")
        });
    let dir = root.join(&namespace);
    std::fs::create_dir_all(&dir).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let url = format!("redis://127.0.0.1:{port}/");
    let mut redis = RedisProcess::start(&binary, &dir, port, "before");
    wait_redis(&mut redis, &url, &namespace).await;
    let repo_a = repository(&url, &namespace).await;
    let repo_b = repository(&url, &namespace).await;
    let backend = Arc::new(Backend::default());
    let (_a, member_a) = dispatcher(repository(&url, &namespace).await, backend.clone(), "a").await;
    let (_b, member_b) = dispatcher(repository(&url, &namespace).await, backend.clone(), "b").await;
    // Registration is real; process heartbeat/expiry is covered by separate process tests.
    for member in [&member_a, &member_b] {
        assert!(repo_a
            .register_dispatcher(member, Duration::from_secs(120))
            .await
            .unwrap());
    }
    let a = managed(repo_a.clone());
    let b = managed(repo_b.clone());
    let template: TemplateVersion = serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"fixture:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":8080}]})).unwrap();
    a.publish(TENANT, &template).await.unwrap();
    let scope = Scope {
        tenant: TENANT.into(),
        template: "test".into(),
        version: "1".into(),
        session_id: "ctx".into(),
    };
    a.create_session(scope.clone()).await.unwrap();
    let (target, _) = a
        .resolve(&scope, Some("sticky".into()), Protocol::Http, None)
        .await
        .unwrap();
    assert_eq!(
        b.resolve(&scope, Some("sticky".into()), Protocol::Http, None)
            .await
            .unwrap()
            .0,
        target
    );
    let inline_a = inline(backend.clone());
    let inline_b = inline(backend.clone());
    let created = inline_a
        .create(TENANT, request("accepted-before-crash"))
        .await
        .unwrap();
    assert_eq!(
        inline_b
            .get(TENANT, &created.instance_id)
            .await
            .unwrap()
            .status,
        "RUNNING"
    );
    assert_eq!(backend.allocations.load(Ordering::SeqCst), 2);

    redis.crash();
    assert_eq!(
        a.resolve(&scope, Some("sticky".into()), Protocol::Http, None)
            .await
            .unwrap()
            .0,
        target
    );
    unavailable(
        a.resolve_with_cache(&scope, Some("sticky".into()), Protocol::Http, None, true)
            .await,
    );
    unavailable(
        b.resolve(
            &scope,
            Some("new-during-outage".into()),
            Protocol::Http,
            None,
        )
        .await,
    );
    let during_outage = inline_a
        .create(TENANT, request("during-outage"))
        .await
        .unwrap();
    inline_b
        .kill(TENANT, &during_outage.instance_id)
        .await
        .unwrap();
    assert!(
        RedisRepository::connect(&url, &namespace, Duration::from_millis(100))
            .await
            .is_err()
    );
    assert_eq!(backend.allocations.load(Ordering::SeqCst), 3);
    assert_eq!(backend.deletes.load(Ordering::SeqCst), 1);

    redis = RedisProcess::start(&binary, &dir, port, "after");
    wait_redis(&mut redis, &url, &namespace).await;
    // A fresh client reads AOF-restored state; existing clients must reconnect too.
    let fresh_repo = repository(&url, &namespace).await;
    let fresh = managed(fresh_repo.clone());
    assert_eq!(fresh.template(TENANT, "test", "1").await.unwrap(), template);
    let mut reconnect_attempts = vec![];
    for gateway in [&a, &b, &fresh] {
        let mut attempts = 0;
        let restored_target = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                attempts += 1;
                match gateway
                    .resolve(&scope, Some("sticky".into()), Protocol::Http, None)
                    .await
                {
                    Ok((value, _)) => break value,
                    Err(Error::Unavailable(_) | Error::OutcomeUnknown(_)) => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    result => panic!("unexpected recovery result: {result:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(restored_target, target);
        reconnect_attempts.push(attempts);
    }
    let restored = inline_b.get(TENANT, &created.instance_id).await.unwrap();
    assert_eq!(restored.status, "RUNNING");
    assert!(AgentState::new(fresh_repo)
        .affinity(&scope, "new-during-outage")
        .await
        .unwrap()
        .is_none());
    assert_eq!(backend.allocations.load(Ordering::SeqCst), 3);
    inline_b.kill(TENANT, &created.instance_id).await.unwrap();
    assert!(matches!(
        inline_a.get(TENANT, &created.instance_id).await,
        Err(Error::NotFound)
    ));
    fresh.release(&scope).await.unwrap();
    assert!(matches!(fresh.session(&scope).await, Err(Error::NotFound)));
    assert_eq!(backend.deletes.load(Ordering::SeqCst), 3);
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "status":"passed", "scope":"real Redis process restart, independent clients, HTTP Dispatcher services; simulated Sandbox",
        "redis_version":version.trim(), "persistence":{"appendonly":true,"appendfsync":"always","rdb_save":false},
        "crash":"SIGKILL", "managed_instance":target.instance_id, "inline_instance":created.instance_id,
        "physical_allocations":3, "confirmed_deletes":3,
        "post_restart_selection_attempts":reconnect_attempts,
        "checks":["warm affinity selection survives Redis outage; bypass rejects outage","managed outage writes rejected; inline create/delete independent of ADX Redis","managed affinity after AOF replay; inline state stays in Sandbox","existing-client reconnect","fresh-client discovery","post-recovery release"],
        "not_proven":["replica failover","power-loss durability","real Platform lifecycle","Gateway process restart"]
    })).unwrap()).unwrap();
    println!("Redis restart evidence: {}", dir.display());
}

async fn measure<F, Fut>(samples: usize, mut operation: F) -> serde_json::Value
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let total = Instant::now();
    let mut micros = Vec::with_capacity(samples);
    for index in 0..samples {
        let start = Instant::now();
        operation(index).await;
        micros.push(start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let seconds = total.elapsed().as_secs_f64();
    micros.sort_by(f64::total_cmp);
    let percentile = |percent: usize| micros[(samples * percent).div_ceil(100) - 1];
    serde_json::json!({"samples":samples,"total_seconds":seconds,
        "operations_per_second":samples as f64 / seconds,"latency_us":{
            "p50":percentile(50),"p95":percentile(95),"p99":percentile(99),
            "min":micros[0],"max":micros[samples-1]}})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "component performance baseline; own Redis 7.2.5 process, simulated immediate-ready Sandbox"]
async fn redis_component_baseline() {
    let binary = PathBuf::from(
        std::env::var("ADX_AGENT_TEST_REDIS_SERVER").expect("provide Redis 7.2.5 binary"),
    );
    let version = Command::new(&binary).arg("--version").output().unwrap();
    let version = String::from_utf8(version.stdout).unwrap();
    assert!(version.contains("v=7.2.5"));
    let namespace = format!("baseline-{}", uuid::Uuid::new_v4());
    let root = std::env::var_os("ADX_AGENT_TEST_EVIDENCE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../out/agent-v2-p6/baseline")
        });
    let dir = root.join(&namespace);
    std::fs::create_dir_all(&dir).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let url = format!("redis://127.0.0.1:{port}/");
    let mut redis = RedisProcess::start(&binary, &dir, port, "baseline");
    wait_redis(&mut redis, &url, &namespace).await;
    let repo = repository(&url, &namespace).await;
    let state = AgentState::new(repo.clone());
    let backend = Arc::new(Backend::default());
    let cached = Arc::new(
        Dispatcher::new(
            state.clone(),
            backend.clone(),
            uuid::Uuid::new_v4().to_string(),
            Config {
                pool_cache_ttl: Duration::from_secs(3600),
                ..Config::default()
            },
        )
        .unwrap(),
    );
    let uncached = Arc::new(
        Dispatcher::new(
            AgentState::new(repository(&url, &namespace).await),
            backend.clone(),
            uuid::Uuid::new_v4().to_string(),
            Config {
                pool_cache_ttl: Duration::from_nanos(1),
                ..Config::default()
            },
        )
        .unwrap(),
    );
    let template: TemplateVersion = serde_json::from_value(serde_json::json!({"name":"test","version":"1","image":"fixture:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    state.publish(TENANT, &template).await.unwrap();
    let scope = Scope {
        tenant: TENANT.into(),
        template: "test".into(),
        version: "1".into(),
        session_id: "warm".into(),
    };
    state.create_session(scope.clone()).await.unwrap();
    // Synthetic existing pool for cache benchmarks, not a production preallocation API.
    for index in 0..4 {
        let id = format!("benchmark-{index}");
        pool_fixture::insert_instance(repo.as_ref(), &scope, &id, InstancePhase::Ready).await;
    }
    let pool = state.pool(&scope).await.unwrap();
    let ids: BTreeSet<_> = pool.into_iter().map(|instance| instance.id).collect();
    assert_eq!(ids.len(), 4);
    let request = adx_dispatcher::ResolveRequest {
        bypass_cache: false,
        scope: scope.clone(),
        affinity_key: None,
    };
    cached.resolve(&request).await.unwrap();
    uncached.resolve(&request).await.unwrap();
    let mut timings = serde_json::Map::new();
    timings.insert(
        "dispatcher_pool_cache_hit".into(),
        measure(200, |_| async {
            assert!(ids.contains(&cached.resolve(&request).await.unwrap().instance_id));
        })
        .await,
    );
    timings.insert(
        "dispatcher_pool_cache_forced_miss".into(),
        measure(200, |_| async {
            assert!(ids.contains(&uncached.resolve(&request).await.unwrap().instance_id));
        })
        .await,
    );
    timings.insert(
        "first_affinity_binding".into(),
        measure(100, |index| {
            let request = adx_dispatcher::ResolveRequest {
                bypass_cache: false,
                scope: scope.clone(),
                affinity_key: Some(format!("affinity-{index}")),
            };
            let cached = cached.clone();
            let ids = &ids;
            async move {
                assert!(ids.contains(&cached.resolve(&request).await.unwrap().instance_id));
            }
        })
        .await,
    );
    let sticky = adx_dispatcher::ResolveRequest {
        bypass_cache: false,
        scope: scope.clone(),
        affinity_key: Some("affinity-0".into()),
    };
    let target = cached.resolve(&sticky).await.unwrap();
    timings.insert(
        "existing_affinity_selection".into(),
        measure(200, |_| async {
            assert_eq!(cached.resolve(&sticky).await.unwrap(), target);
        })
        .await,
    );

    let storm_scope = Scope {
        session_id: "storm".into(),
        ..scope.clone()
    };
    state.create_session(storm_scope.clone()).await.unwrap();
    let before = backend.allocations.load(Ordering::SeqCst);
    let start = Instant::now();
    let mut tasks = vec![];
    for index in 0..32 {
        let node = if index % 2 == 0 {
            cached.clone()
        } else {
            uncached.clone()
        };
        let request = adx_dispatcher::ResolveRequest {
            bypass_cache: false,
            scope: storm_scope.clone(),
            affinity_key: None,
        };
        tasks.push(tokio::spawn(async move {
            node.resolve(&request).await.unwrap()
        }));
    }
    let mut targets = BTreeSet::new();
    for task in tasks {
        targets.insert(task.await.unwrap().instance_id);
    }
    assert_eq!(targets.len(), 1);
    assert_eq!(backend.allocations.load(Ordering::SeqCst) - before, 1);
    timings.insert("cold_start_coordination_storm".into(), serde_json::json!({
        "requests":32,"dispatcher_objects":2,"physical_allocations":1,
        "total_seconds":start.elapsed().as_secs_f64(),"sandbox_startup":"simulated immediate-ready; excludes image and runtime startup"
    }));
    for index in 0..200 {
        state
            .create_session(Scope {
                session_id: format!("scan-{index}"),
                ..scope.clone()
            })
            .await
            .unwrap();
    }
    timings.insert(
        "session_scan".into(),
        measure(10, |_| async {
            let mut cursor = 0;
            let mut keys = BTreeSet::new();
            loop {
                let page = repo.scan("session", cursor, 100).await.unwrap();
                for key in page.keys {
                    let session: Session = repo.get(&key).await.unwrap().unwrap().decode().unwrap();
                    keys.insert(session.scope.key());
                }
                cursor = page.cursor;
                if cursor == 0 {
                    break;
                }
            }
            assert_eq!(keys.len(), 202);
        })
        .await,
    );
    let inline = inline(backend.clone());
    let before = backend.allocations.load(Ordering::SeqCst);
    timings.insert(
        "inline_sandbox_create".into(),
        measure(100, |index| {
            let inline = &inline;
            async move {
                assert_eq!(
                    inline
                        .create(TENANT, self::request(&format!("bench-{index}")))
                        .await
                        .unwrap()
                        .code,
                    200
                );
            }
        })
        .await,
    );
    assert_eq!(
        backend.allocations.load(Ordering::SeqCst),
        before + 100,
        "inline success requires Sandbox acceptance"
    );
    let compiler = Command::new("rustc").arg("--version").output().unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "status":"passed", "scope":"in-process ADX component calls and loopback real Redis; not Gateway HTTP or real Sandbox latency",
        "profile":if cfg!(debug_assertions) {"debug"} else {"release"}, "architecture":std::env::consts::ARCH,
        "rustc":String::from_utf8_lossy(&compiler.stdout).trim(), "redis_version":version.trim(),
        "persistence":{"appendonly":true,"appendfsync":"always","rdb_save":false},
        "cache_ttl_seconds":3600,"forced_miss_ttl_ns":1,"pool_size":4,"session_scan_population":202,
        "measurements":timings,
        "not_proven":["production capacity or SLO","real image/runtime cold start","Gateway TLS and forwarding overhead","replica failover"]
    })).unwrap()).unwrap();
    println!("Component baseline evidence: {}", dir.display());
}
