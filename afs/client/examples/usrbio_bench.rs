//! usrbio 数据面的吞吐测量工具（与 FUSE/POSIX 对比用）。
//!
//! 与 `usrbio_round_trip` 的分工：那个是给人看的最小用法示例，这个是给机器跑的测量工具，
//! 参数化总字节数与块大小，输出可直接与 `dd` 的结果并列。
//!
//! ```text
//! cargo run --release --example usrbio_bench -- <node.sock> <已存在文件> <总MiB> [块KiB]
//! ```
//!
//! 三个数字分别对应：写入（不含落盘）、`sync(Full)` 落盘、读回。

use std::time::{Duration, Instant};

use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode, LocalClientError};

#[tokio::main]
async fn main() -> Result<(), LocalClientError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("用法: usrbio_bench <node.sock> <已存在文件> <总MiB> [块KiB，默认 1024]");
        std::process::exit(2);
    }
    let socket = &args[0];
    let path = &args[1];
    let total_mib: u64 = args[2].parse().expect("总 MiB 必须是整数");
    let block_kib: u64 = args
        .get(3)
        .map_or(1024, |value| value.parse().expect("块 KiB 必须是整数"));

    let block_bytes = (block_kib * 1024) as usize;
    let total_bytes = total_mib * 1024 * 1024;
    let blocks = total_bytes / block_bytes as u64;
    // 环里每次同时放 8 块；iov 容量按 16 块分配，留出在途余量。
    let wave = 8_usize;

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(socket)).await?;
    let file = client.open(path, DfsAccess::ReadWrite).await?;
    let mut iov = client.create_iov(block_bytes, 16).await?;
    let mut ring = client.create_ring(&iov, 128, 0, 30_000).await?;
    let registration = client.register_file(&file).await?;

    // 每个块填不同内容：全零或重复图案会被存储层压掉（实测 6 GiB 只占 106 MB），
    // 那样测出来的就不是真实写入量。
    // 内容必须随**绝对块号**变化：图案只要周期性重复，块哈希就会把它们判成同一块，
    // 1 GiB 会被去重成几 MB，测出来的就不是磁盘写入。
    let payload_for = |block: u64| -> Vec<u8> {
        (0..block_bytes)
            .map(|index| {
                let mixed = (index as u64)
                    .wrapping_mul(2_654_435_761)
                    .wrapping_add(block.wrapping_mul(0x9E37_79B9_7F4A_7C15));
                (mixed >> 13) as u8
            })
            .collect()
    };
    let mut payloads: Vec<Vec<u8>> = (0..wave).map(|slot| payload_for(slot as u64)).collect();

    // ---- 写入（不含落盘）----
    let started = Instant::now();
    let mut written_blocks = 0_u64;
    while written_blocks < blocks {
        let batch = (blocks - written_blocks).min(wave as u64) as usize;
        for (slot, payload) in payloads.iter_mut().enumerate().take(batch) {
            *payload = payload_for(written_blocks + slot as u64);
        }
        let items: Vec<(u64, &[u8])> = (0..batch)
            .map(|slot| {
                let offset = (written_blocks + slot as u64) * block_bytes as u64;
                (offset, payloads[slot].as_slice())
            })
            .collect();
        ring.write_batch(&mut iov, &registration, &items).await?;
        written_blocks += batch as u64;
    }
    let write_elapsed = started.elapsed();

    // ---- 落盘 ----
    let started = Instant::now();
    client.sync(&file.handle, DfsSyncMode::Full).await?;
    let sync_elapsed = started.elapsed();

    if std::env::var("AFS_BENCH_SKIP_READ").is_ok() {
        println!(
            "usrbio {} MiB / {} KiB 块: 写 {:.0} MiB/s ({:?}) / sync(Full) {:?} / 读（已跳过）",
            total_mib,
            block_kib,
            mib_per_second(total_bytes, write_elapsed),
            write_elapsed,
            sync_elapsed,
        );
        client.unregister_file(registration).await?;
        client.close(file.handle).await?;
        ring.destroy().await?;
        iov.destroy().await?;
        return Ok(());
    }

    // ---- 读回 ----
    let mut buffers: Vec<Vec<u8>> = (0..wave).map(|_| vec![0_u8; block_bytes]).collect();
    let started = Instant::now();
    let mut read_blocks = 0_u64;
    while read_blocks < blocks {
        let batch = (blocks - read_blocks).min(wave as u64) as usize;
        let base = read_blocks * block_bytes as u64;
        let mut targets: Vec<(u64, &mut [u8])> = buffers
            .iter_mut()
            .take(batch)
            .enumerate()
            .map(|(slot, buffer)| {
                (
                    base + slot as u64 * block_bytes as u64,
                    buffer.as_mut_slice(),
                )
            })
            .collect();
        ring.read_batch(&mut iov, &registration, &mut targets)
            .await?;
        read_blocks += batch as u64;
    }
    let read_elapsed = started.elapsed();

    println!(
        "usrbio {} MiB / {} KiB 块: 写 {:.0} MiB/s ({:?}) / sync(Full) {:?} / 读 {:.0} MiB/s ({:?})",
        total_mib,
        block_kib,
        mib_per_second(total_bytes, write_elapsed),
        write_elapsed,
        sync_elapsed,
        mib_per_second(total_bytes, read_elapsed),
        read_elapsed,
    );

    client.unregister_file(registration).await?;
    client.close(file.handle).await?;
    ring.destroy().await?;
    iov.destroy().await?;
    Ok(())
}

/// 字节数与耗时换算成 MiB/s。
fn mib_per_second(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
}
