//! usrbio 数据面的吞吐测量工具（与 FUSE/POSIX 对比用）。
//!
//! 与 `usrbio_round_trip` 的分工：那个是给人看的最小用法示例，这个是给机器跑的测量工具，
//! 参数化总字节数与块大小，输出可直接与 `dd` 的结果并列。
//!
//! ```text
//! cargo run --release -p afs-tools --bin usrbio_bench -- <node.sock> <已存在文件> <总MiB> [块KiB]
//! ```
//!
//! 三个数字分别对应：写入（不含落盘）、`sync(Full)` 落盘、读回。

use std::time::{Duration, Instant};

use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode, LocalClientError};

/// 为绝对块号生成确定性的块内容。
///
/// 内容必须随**绝对块号**变化：全零、常数填充或周期性图案会被存储层的块去重吞掉
/// （实测 6 GiB 只占 106 MB），测出来的就不是真实写入量。生成在写侧计时**内部**进行
/// （与历史版本一致，保证口径可比）；混合器成本见 [`mix64`] 的说明。
fn payload_for(block: u64, block_bytes: usize) -> Vec<u8> {
    (0..block_bytes)
        .map(|index| {
            let mixed = (index as u64)
                .wrapping_mul(2_654_435_761)
                .wrapping_add(block.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            mix64(mixed) as u8
        })
        .collect()
}

/// splitmix64 终结器：雪崩混合，让每个输出位都依赖输入的所有位。
///
/// 前面的线性混合（乘加）保留输入的加性结构：块号相差 2^21 时混合值恰好相差
/// 「奇数 × 2^21」，bit 13..20 完全不变——旧实现直接取 bit 13..20 当字节，整块内容
/// 便以 2^21 个块为周期重复（4 KiB 档每 8 GiB 一次、1 MiB 档每 2 TiB 一次），
/// 同样会被去重吞掉。终结器先把高位折叠回低位、再乘散，结构周期随之消失
/// （回归测试 `payload_breaks_the_two_pow21_period` 钉住这一点）。
///
/// 成本：每字节从「2 次乘法 + 1 次移位」变为「3 次乘法 + 3 次移位异或」，指令数
/// 约为此前的一倍半。生成本来就在写侧计时内部进行，跨版本对比的口径不变；但
/// 「写吞吐」读数里生成器占的比重比旧版更高，对比 dd 时要记得这层偏差。
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x
}

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

    // 每个块填不同内容：内容必须随**绝对块号**变化（见 `payload_for` 的说明）。
    let mut payloads: Vec<Vec<u8>> = (0..wave)
        .map(|slot| payload_for(slot as u64, block_bytes))
        .collect();

    // ---- 写入（不含落盘）----
    let started = Instant::now();
    let mut written_blocks = 0_u64;
    while written_blocks < blocks {
        let batch = (blocks - written_blocks).min(wave as u64) as usize;
        for (slot, payload) in payloads.iter_mut().enumerate().take(batch) {
            *payload = payload_for(written_blocks + slot as u64, block_bytes);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 旧实现把线性混合截到 bit 13..20：块号相差 2^21 的两块**逐字节相同**
    /// （4 KiB 档每 8 GiB 重复一次、1 MiB 档每 2 TiB 一次），会被块去重吞掉。
    /// 生成器必须打破这个结构周期。
    #[test]
    fn payload_breaks_the_two_pow21_period() {
        const BLOCK: usize = 4096;
        for block in [0_u64, 1, 2, 12345, (1 << 21) - 1, 1 << 40] {
            assert_ne!(
                payload_for(block, BLOCK),
                payload_for(block + (1 << 21), BLOCK),
                "blocks {block} and {} share the same payload",
                block + (1 << 21)
            );
        }
    }

    /// 采样验证：前 1024 块互不相同、块内非常数、同参数确定性复现。
    ///
    /// 这不是「永不重复」的数学证明——任意确定性 8 位/字节生成器都存在整块碰撞的
    /// 可能；这里钉住的是「无结构周期 + 采样范围内无整块碰撞 + 确定性」。
    /// e2e 的跨版本 md5 一致性对比依赖确定性。
    #[test]
    fn payload_blocks_are_distinct_within_the_sampled_range() {
        const BLOCK: usize = 4096;
        let mut seen = std::collections::HashSet::new();
        for block in 0..1024_u64 {
            assert!(
                seen.insert(payload_for(block, BLOCK)),
                "block {block} duplicates an earlier block"
            );
        }
        let payload = payload_for(7, BLOCK);
        assert!(
            payload.iter().any(|byte| *byte != payload[0]),
            "payload must not be a constant fill"
        );
        assert_eq!(payload_for(7, BLOCK), payload, "determinism");
    }
}
