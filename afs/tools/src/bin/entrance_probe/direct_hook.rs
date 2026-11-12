// Experiment-only hook included ONLY in the Linux build copy of node.rs.
fn cadx_direct_probe(
    fs: std::sync::Arc<crate::node::vfs::dfs::DistributedFs>,
    root: std::path::PathBuf,
    runtime: tokio::runtime::Handle,
) {
    std::thread::spawn(move || {
        use crate::node::vfs::{Backend, types::*};
        use std::time::Instant;
        let _entered = runtime.enter();
        let result = (|| -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
            while !root.join("direct.go").exists() {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            let n = 1usize << 30;
            let block = 4usize << 20;
            let mut input = vec![0u8; n];
            let mut x = 0x5eedd24c15812345u64;
            for c in input.chunks_mut(8) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                c.copy_from_slice(&x.to_le_bytes()[..c.len()]);
            }
            let mut output = vec![0u8; n];
            for c in output.chunks_mut(4096) {
                c[0] = 0;
            }
            // SAFETY: process identity queries do not dereference pointers or mutate memory.
            let ctx = RequestContext {
                uid: unsafe { libc::geteuid() },
                // SAFETY: process identity query takes no pointers or mutable memory.
                    gid: unsafe { libc::getegid() },
                pid: std::process::id(),
                umask: 0,
                supplementary_gids: vec![],
            };
            let write =
                std::env::var("CADX_DIRECT_PROBE_OP").unwrap_or_else(|_| "write".into()) == "write";
            let setup = Instant::now();
            let e = fs.lookup(&ctx, fs.root_inode(), std::ffi::OsStr::new("data.bin"))?;
            let h = fs.open(
                &ctx,
                e.inode,
                if write { libc::O_RDWR } else { libc::O_RDONLY },
            )?;
            let setup_s = setup.elapsed().as_secs_f64();
            let t = Instant::now();
            if write {
                for (i, s) in input.chunks(block).enumerate() {
                    assert_eq!(fs.write(&ctx, h, (i * block) as u64, s)?, s.len());
                }
            }
            let io_s = if write { t.elapsed().as_secs_f64() } else { 0. };
            let sync = Instant::now();
            if write {
                fs.fsync(&ctx, h, SyncMode::Full)?;
            }
            let sync_s = if write {
                sync.elapsed().as_secs_f64()
            } else {
                0.
            };
            fs.release(&ctx, h)?;
            let mut reads = Vec::new();
            for _ in 0..2 {
                let setup = Instant::now();
                let h = fs.open(&ctx, e.inode, libc::O_RDONLY)?;
                let read_setup_s = setup.elapsed().as_secs_f64();
                let t = Instant::now();
                for (i, s) in output.chunks_mut(block).enumerate() {
                    assert_eq!(fs.read(&ctx, h, (i * block) as u64, s)?, s.len());
                }
                let read_io_s = t.elapsed().as_secs_f64();
                assert_eq!(output, input, "direct backend byte mismatch");
                fs.release(&ctx, h)?;
                reads.push(
                    serde_json::json!({"setup_s":read_setup_s,"io_s":read_io_s,"verified":true}),
                );
            }
            Ok(
                serde_json::json!({"direct_backend":true,"write":write,"bytes":n,"block":block,
                "setup_s":setup_s,"io_s":io_s,"sync_s":sync_s,"total_s":setup_s+io_s+sync_s,"reads":reads,
                "expected_hash":blake3::hash(&input).to_hex().to_string(),"body_sync_skipped":std::env::var_os("CADX_SKIP_BODY_SYNC").is_some()}),
            )
        })();
        let value = match result {
            Ok(v) => v,
            Err(e) => serde_json::json!({"error":e.to_string()}),
        };
        std::fs::write(
            root.join("direct-result.json"),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();
    });
}
