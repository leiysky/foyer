use std::{
    path::Path,
    time::{Duration, Instant},
};

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, EngineConfig, FsDeviceBuilder, Hint, HybridCache, HybridCachePolicy,
    HybridCacheProperties, PsyncIoEngineConfig, RecoverMode,
};
use foyer_extent::{Cache, CachePriority, EngineValue, Entry, ExtentEngineConfig};

const PAGE_SIZE: usize = 4 * 1024;
const DISK_CAPACITY: usize = 16 * 1024 * 1024;
const MEMORY_CAPACITY: usize = 1024 * 1024;

type FoyerCache = HybridCache<Bytes, EngineValue>;

#[derive(Debug, Clone, Copy)]
enum DiskEngine {
    Block,
    Extent,
}

fn engine_config(path: &std::path::Path) -> ExtentEngineConfig {
    ExtentEngineConfig::new(path, DISK_CAPACITY as u64)
        .with_slot_size(PAGE_SIZE)
        .with_segment_size(PAGE_SIZE * 8)
        .with_read_run_size(PAGE_SIZE)
        .with_write_run_size(PAGE_SIZE * 8)
        .with_index_write_buffer_size(PAGE_SIZE * 16)
        .with_index_cache_size(1024 * 1024)
        .with_checkpoint_changes(8)
        .with_queue_capacity_bytes(1024 * 1024)
        .with_queue_capacity_entries(128)
        .with_write_batch_bytes(128 * 1024)
        .with_write_batch_entries(32)
}

fn block_engine_config(path: &Path) -> Box<dyn EngineConfig<Bytes, EngineValue, HybridCacheProperties>> {
    let device = FsDeviceBuilder::new(path).with_capacity(DISK_CAPACITY).build().unwrap();
    Box::new(
        BlockEngineConfig::new(device)
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_block_size(1024 * 1024)
            .with_buffer_pool_size(2 * 1024 * 1024)
            .with_indexer_shards(2)
            .with_recover_concurrency(2)
            .with_tombstone_log(true),
    )
}

async fn build_cache(path: &Path, engine: DiskEngine, recover_mode: RecoverMode) -> FoyerCache {
    let engine: Box<dyn EngineConfig<Bytes, EngineValue, HybridCacheProperties>> = match engine {
        DiskEngine::Block => block_engine_config(&path.join("block")),
        DiskEngine::Extent => Box::new(engine_config(&path.join("extent"))),
    };

    HybridCache::builder()
        .with_name("foyer-engine-contract")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .with_flush_on_close(false)
        .memory(MEMORY_CAPACITY)
        .with_shards(2)
        .with_weighter(|key: &Bytes, value: &EngineValue| key.len() + value.value().len() + 64)
        .storage()
        .with_engine_config(engine)
        .with_recover_mode(recover_mode)
        .build()
        .await
        .unwrap()
}

fn properties(priority: CachePriority) -> HybridCacheProperties {
    let hint = match priority {
        CachePriority::Low => Hint::Low,
        CachePriority::Normal | CachePriority::High => Hint::Normal,
    };
    HybridCacheProperties::default().with_hint(hint)
}

async fn close(cache: FoyerCache) {
    cache.storage().wait().await;
    cache.close().await.unwrap();
    drop(cache);
}

async fn verify_foyer_engine_contract(path: &Path, engine: DiskEngine) {
    let key = Bytes::from_static(b"tenant/table/partition");

    let cache = build_cache(path, engine, RecoverMode::None).await;
    let usage = cache.storage().storage_usage();
    assert_eq!(usage.capacity(), DISK_CAPACITY);
    assert!(usage.allocated() > 0);
    assert!(usage.allocated() <= usage.capacity());
    cache.insert_with_properties(
        key.clone(),
        EngineValue::new(Bytes::from_static(b"old"), CachePriority::High).unwrap(),
        properties(CachePriority::High),
    );
    cache.insert_with_properties(
        key.clone(),
        EngineValue::new(Bytes::from_static(b"new"), CachePriority::Low).unwrap(),
        properties(CachePriority::Low),
    );
    close(cache).await;

    let recovered = build_cache(path, engine, RecoverMode::Strict).await;
    let entry = recovered.get(&key).await.unwrap().unwrap();
    assert_eq!(entry.key(), &key);
    assert_eq!(entry.value().value(), &Bytes::from_static(b"new"));
    assert_eq!(entry.value().priority(), CachePriority::Low);

    recovered.remove(&key);
    close(recovered).await;

    let reopened = build_cache(path, engine, RecoverMode::Strict).await;
    assert!(reopened.get(&key).await.unwrap().is_none());
    close(reopened).await;
}

#[tokio::test]
async fn block_and_extent_match_the_entry_lifecycle_contract() {
    let directory = tempfile::tempdir().unwrap();
    verify_foyer_engine_contract(&directory.path().join("block-case"), DiskEngine::Block).await;
    verify_foyer_engine_contract(&directory.path().join("extent-case"), DiskEngine::Extent).await;
}

#[tokio::test]
async fn public_cache_recovers_complete_entries() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("public-cache");
    let key = Bytes::from_static(b"tenant/table/blob");
    let value = Bytes::from_static(b"cached bytes");

    let cache = Cache::builder(MEMORY_CAPACITY, engine_config(&path))
        .with_recover_mode(RecoverMode::None)
        .build()
        .await
        .unwrap();
    assert_eq!(cache.storage_usage().capacity(), DISK_CAPACITY);
    assert!(cache.engine_handle().write_stats().is_some());
    assert_eq!(cache.statistics().disk_read_ios(), 0);
    assert_eq!(cache.estimated_entry_count(), 0);
    cache.put(Entry::new(key.clone(), value.clone(), CachePriority::High).unwrap());
    assert_eq!(cache.estimated_entry_count(), 1);
    assert_eq!(cache.get(&key).await.unwrap().value(), &value);
    cache.close().await.unwrap();
    drop(cache);

    let recovered = Cache::builder(MEMORY_CAPACITY, engine_config(&path))
        .with_recover_mode(RecoverMode::Strict)
        .build()
        .await
        .unwrap();
    assert_eq!(recovered.estimated_entry_count(), 1);
    let handle = recovered.engine_handle();
    let disk_reads_before = (
        recovered.statistics().disk_read_ios() as u64,
        recovered.statistics().disk_read_bytes() as u64,
    );
    let index_reads_before = handle.index_read_stats().unwrap();
    let payload_reads_before = handle.read_stats().unwrap();
    let entry = recovered.get(&key).await.unwrap();
    assert_eq!(entry.key(), &key);
    assert_eq!(entry.value(), &value);
    assert_eq!(entry.priority(), CachePriority::High);
    assert_eq!(recovered.estimated_entry_count(), 1);
    let index_reads_after = handle.index_read_stats().unwrap();
    let payload_reads_after = handle.read_stats().unwrap();
    assert_eq!(
        recovered.statistics().disk_read_ios() as u64 - disk_reads_before.0,
        index_reads_after.read_operations - index_reads_before.read_operations + payload_reads_after.data_runs
            - payload_reads_before.data_runs,
    );
    assert_eq!(
        recovered.statistics().disk_read_bytes() as u64 - disk_reads_before.1,
        index_reads_after.read_bytes - index_reads_before.read_bytes + payload_reads_after.data_bytes
            - payload_reads_before.data_bytes,
    );
    recovered.delete(&key);
    // Delete is an asynchronous best-effort hint. A racing read may still observe the previous
    // complete entry, but never a partial value or a different priority.
    if let Some(entry) = recovered.get(&key).await {
        assert_eq!(entry.value(), &value);
        assert_eq!(entry.priority(), CachePriority::High);
    }
    recovered.close().await.unwrap();
    drop(recovered);

    let reopened = Cache::builder(MEMORY_CAPACITY, engine_config(&path))
        .with_recover_mode(RecoverMode::Strict)
        .build()
        .await
        .unwrap();
    // Delete is best effort. A bounded close may discard it, in which case reopening may expose
    // the previous complete value but never a partially deleted or corrupted entry.
    if let Some(entry) = reopened.get(&key).await {
        assert_eq!(entry.value(), &value);
        assert_eq!(entry.priority(), CachePriority::High);
    }
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn extent_reports_complete_physical_write_statistics_to_foyer() {
    let directory = tempfile::tempdir().unwrap();
    let config = engine_config(&directory.path().join("write-statistics"));
    let handle = config.handle();
    let cache = HybridCache::builder()
        .with_name("extent-write-statistics")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .with_flush_on_close(false)
        .memory(MEMORY_CAPACITY)
        .with_shards(2)
        .storage()
        .with_engine_config(Box::new(config) as Box<dyn EngineConfig<Bytes, EngineValue, HybridCacheProperties>>)
        .with_recover_mode(RecoverMode::None)
        .build()
        .await
        .unwrap();

    cache.insert(
        Bytes::from_static(b"statistics-key"),
        EngineValue::new(Bytes::from(vec![7; PAGE_SIZE * 2]), CachePriority::Normal).unwrap(),
    );
    cache.storage().wait().await;

    let physical = handle.physical_write_stats().unwrap();
    assert_eq!(cache.statistics().disk_write_bytes(), physical.total_bytes() as usize);
    assert_eq!(cache.statistics().disk_write_ios(), physical.total_runs() as usize);
    let scheduler = handle.io_scheduler_stats().unwrap();
    assert!(scheduler.enabled());
    assert!(scheduler.write_operations > 0);
    assert_eq!(scheduler.active_reads, 0);
    assert_eq!(scheduler.active_writes, 0);
    assert_eq!(scheduler.waiting_writes, 0);
    cache.close().await.unwrap();
}

#[tokio::test]
async fn periodic_checkpoint_bounds_the_recovery_frontier() {
    let directory = tempfile::tempdir().unwrap();
    let config = engine_config(&directory.path().join("periodic-checkpoint"))
        .with_checkpoint_changes(usize::MAX)
        .with_checkpoint_interval(Duration::from_millis(20));
    let handle = config.handle();
    let cache = Cache::builder(MEMORY_CAPACITY, config)
        .with_recover_mode(RecoverMode::None)
        .build()
        .await
        .unwrap();
    cache.put(
        Entry::new(
            Bytes::from_static(b"periodic-key"),
            Bytes::from_static(b"periodic-value"),
            CachePriority::Normal,
        )
        .unwrap(),
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let checkpoint = loop {
        let checkpoint = handle.checkpoint_stats().unwrap();
        if checkpoint.published_epoch > 0 && checkpoint.durable_epoch >= checkpoint.published_epoch {
            break checkpoint;
        }
        assert!(Instant::now() < deadline, "periodic checkpoint did not advance");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(checkpoint.dirty_changes, 0);

    let writes = handle.write_stats().unwrap();
    assert_eq!(writes.accepted_commands, 1);
    assert_eq!(writes.completed_commands, 1);
    assert_eq!(writes.dropped_commands, 0);
    assert_eq!(writes.failed_batches, 0);
    assert!(handle.background_error().is_none());
    cache.close().await.unwrap();
}

#[test]
fn entry_requires_a_complete_non_empty_key_and_value() {
    assert!(foyer_extent::Entry::new(Bytes::new(), Bytes::from_static(b"v"), CachePriority::Normal).is_err());
    assert!(foyer_extent::Entry::new(Bytes::from_static(b"k"), Bytes::new(), CachePriority::Normal).is_err());
}
