//! usrbio 风格读写的完整示例：持久 iov + 共享环 + 批量提交 + 落盘。
//!
//! 运行前需要一个在跑的 node（本机 UDS）以及一个已存在的文件——**创建文件仍然走 FUSE/POSIX**，
//! 这个 SDK 只接管文件打开之后的数据读写。
//!
//! ```text
//! cargo run --example usrbio_path_round_trip -- /var/run/afs/node.sock /data.bin
//! ```
//!
//! 演示的顺序就是生产用法：
//! `connect → open → create_iov → create_ring → register_file → 读写 → sync → 收尾`。

use std::time::Instant;

use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode, LocalClientError};

/// iov 块大小：必须 ≥ 4096，且一次 IO 不能跨块边界（由 SDK 内部保证）。
const BLOCK_SIZE: usize = 64 * 1024;
/// iov 里放多少块，决定同时在途的数据量上限。
const BLOCKS: u32 = 32;
/// 环深度：SQ/CQ 各多少条目。
const ENTRIES: u32 = 64;
/// 一次提交多少条：批量是这套接口的核心，单条提交会浪费掉绝大部分收益。
const WAVE: usize = 16;

#[tokio::main]
async fn main() -> Result<(), LocalClientError> {
    let mut args = std::env::args().skip(1);
    let socket = args.next().unwrap_or_else(|| {
        eprintln!("用法: usrbio_path_round_trip <node.sock> <已存在的文件路径>");
        std::process::exit(2);
    });
    let path = args.next().unwrap_or_else(|| {
        eprintln!("用法: usrbio_path_round_trip <node.sock> <已存在的文件路径>");
        std::process::exit(2);
    });

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket)).await?;
    let file = client.open(&path, DfsAccess::ReadWrite).await?;
    let mut iov = client.create_iov(BLOCK_SIZE, BLOCKS).await?;
    let mut ring = client.create_ring(&iov, ENTRIES, 0, 5_000).await?;
    let registration = client.register_file(&file).await?;

    // 准备一批互不重叠的数据：每条一个块，长度正好一块。
    let payloads: Vec<Vec<u8>> = (0..WAVE)
        .map(|index| vec![index as u8 + 1; BLOCK_SIZE])
        .collect();
    let items: Vec<(u64, &[u8])> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| (index as u64 * BLOCK_SIZE as u64, payload.as_slice()))
        .collect();

    // ---- 写 ----
    let started = Instant::now();
    let written = ring.write_batch(&mut iov, &registration, &items).await?;
    let write_elapsed = started.elapsed();

    let total: usize = written.iter().sum();
    println!(
        "写入 {WAVE} 条 / {:.1} KiB 用时 {:?}（{:.1} MiB/s）",
        total as f64 / 1024.0,
        write_elapsed,
        mib_per_second(total, write_elapsed),
    );

    // ---- 落盘 ----
    // Data 只要保证数据可见；Full 还要求元数据落盘。按需要选，别无条件用 Full。
    client.sync(&file.handle, DfsSyncMode::Full).await?;

    // ---- 读回校验 ----
    let mut reads: Vec<Vec<u8>> = (0..WAVE).map(|_| vec![0_u8; BLOCK_SIZE]).collect();
    let started = Instant::now();
    {
        let mut targets: Vec<(u64, &mut [u8])> = reads
            .iter_mut()
            .enumerate()
            .map(|(index, buffer)| (index as u64 * BLOCK_SIZE as u64, buffer.as_mut_slice()))
            .collect();
        ring.read_batch(&mut iov, &registration, &mut targets)
            .await?;
    }
    let read_elapsed = started.elapsed();
    println!(
        "读回 {WAVE} 条 / {:.1} KiB 用时 {:?}（{:.1} MiB/s）",
        total as f64 / 1024.0,
        read_elapsed,
        mib_per_second(total, read_elapsed),
    );

    for (index, buffer) in reads.iter().enumerate() {
        let expected = index as u8 + 1;
        if buffer.iter().any(|byte| *byte != expected) {
            eprintln!("第 {index} 条内容与写入不一致");
            std::process::exit(1);
        }
    }
    println!("校验通过；iov 剩余可用块 {}/{}", iov.free_blocks(), BLOCKS);

    // ---- 收尾 ----
    // 顺序有意义：先撤注册，再关句柄，最后销毁环与 iov（环绑定了 iov id）。
    client.unregister_file(registration).await?;
    client.close(file.handle).await?;
    ring.destroy().await?;
    iov.destroy().await?;
    Ok(())
}

/// 把字节数与耗时换成 MiB/s。
fn mib_per_second(bytes: usize, elapsed: std::time::Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
}
