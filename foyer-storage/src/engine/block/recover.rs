// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use foyer_common::{
    error::{Error, ErrorKind, Result},
    metrics::Metrics,
    spawn::Spawner,
};
use futures_util::{stream, StreamExt};

use super::indexer::Indexer;
use crate::engine::{
    block::{
        indexer::HashedEntryAddress,
        manager::{Block, BlockId, BlockManager},
        scanner::{BlockScanner, EntryInfo},
        serde::{AtomicSequence, Sequence},
        tombstone::Tombstone,
    },
    RecoverMode,
};

#[derive(Debug)]
pub struct RecoverRunner;

impl RecoverRunner {
    #[expect(clippy::too_many_arguments)]
    pub async fn run(
        recover_concurrency: usize,
        recover_mode: RecoverMode,
        blob_index_size: usize,
        blocks: Vec<BlockId>,
        sequence: &AtomicSequence,
        indexer: &Indexer,
        block_manager: &BlockManager,
        tombstones: &[Tombstone],
        spawner: Spawner,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        let now = Instant::now();

        let mut latest_sequence = tombstones
            .iter()
            .map(|tombstone| tombstone.sequence)
            .max()
            .unwrap_or_default();

        // No block needs to be inspected when recovery is disabled. At large capacities, spawning one task per block
        // is observable startup work even though every task immediately returns an empty result.
        if recover_mode == RecoverMode::None {
            sequence.store(latest_sequence + 1, Ordering::Release);
            block_manager.init(&blocks);
            Self::record_recovery(now, 0, blocks.len(), 0, latest_sequence, metrics);
            return Ok(());
        }

        // Keep only deletion metadata while blocks are streamed into the final index. This avoids retaining all entry
        // addresses in an intermediate deduplication map. An entry newer than the latest tombstone remains recoverable.
        let mut tombstone_sequences = HashMap::<u64, Sequence>::with_capacity(tombstones.len());
        for tombstone in tombstones {
            match tombstone_sequences.entry(tombstone.hash) {
                Entry::Occupied(mut entry) => {
                    let latest = (*entry.get()).max(tombstone.sequence);
                    *entry.get_mut() = latest;
                }
                Entry::Vacant(entry) => {
                    entry.insert(tombstone.sequence);
                }
            }
        }

        let mut clean_blocks = Vec::with_capacity(blocks.len());
        let mut evictable_blocks = 0;
        let mut recovered = stream::iter(blocks.into_iter().map(|id| {
            let block = block_manager.block(id).clone();
            spawner.spawn(async move {
                BlockRecoverRunner::run(recover_mode, block, blob_index_size)
                    .await
                    .map(|infos| (id, infos))
            })
        }))
        .buffer_unordered(recover_concurrency.max(1));

        let mut errors = vec![];
        while let Some(result) = recovered.next().await {
            let result = result?;
            let (block, infos) = match result {
                Ok(recovered) => recovered,
                Err(error) => {
                    errors.push(error.to_string());
                    continue;
                }
            };

            if infos.is_empty() {
                clean_blocks.push(block);
                continue;
            }
            evictable_blocks += 1;

            let mut batch = Vec::with_capacity(infos.len());
            for EntryInfo { hash, addr } in infos {
                latest_sequence = latest_sequence.max(addr.sequence);
                if tombstone_sequences
                    .get(&hash)
                    .is_none_or(|tombstone_sequence| addr.sequence > *tombstone_sequence)
                {
                    batch.push(HashedEntryAddress { hash, address: addr });
                }
            }
            indexer.insert_batch(batch);
        }

        if !errors.is_empty() {
            let mut error = Error::new(ErrorKind::Recover, "failed to recover blocks");
            for reason in errors {
                error = error.with_context("reason", reason);
            }
            return Err(error);
        }

        sequence.store(latest_sequence + 1, Ordering::Release);
        block_manager.init(&clean_blocks);
        Self::record_recovery(
            now,
            evictable_blocks,
            clean_blocks.len(),
            indexer.entry_count(),
            latest_sequence,
            metrics,
        );

        Ok(())
    }

    fn record_recovery(
        now: Instant,
        evictable_blocks: usize,
        clean_blocks: usize,
        entries: usize,
        latest_sequence: Sequence,
        metrics: Arc<Metrics>,
    ) {
        tracing::info!(
            "Recovers {evictable_blocks} blocks with data, {clean_blocks} clean blocks, {entries} total entries with max sequence as {latest_sequence}..",
        );
        let elapsed = now.elapsed();
        tracing::info!("[recover] finish in {:?}", elapsed);

        metrics
            .storage_block_engine_recover_duration
            .record(elapsed.as_secs_f64());
    }
}

#[derive(Debug)]
struct BlockRecoverRunner;

impl BlockRecoverRunner {
    async fn run(mode: RecoverMode, block: Block, blob_index_size: usize) -> Result<Vec<EntryInfo>> {
        if mode == RecoverMode::None {
            return Ok(vec![]);
        }

        let mut recovered = vec![];

        let id = block.id();
        let mut iter = BlockScanner::new(block, blob_index_size);
        loop {
            let r = iter.next().await;
            let infos = match r {
                Ok(Some(infos)) => infos,
                Ok(None) => break,
                Err(e) => {
                    if mode == RecoverMode::Strict {
                        return Err(e);
                    } else {
                        tracing::warn!("error raised when recovering block {id}, skip further recovery for {id}.");
                        break;
                    }
                }
            };

            // Sequence numbers are allocated before submissions enter a multi-producer flusher queue, so physical
            // order is not guaranteed to be monotonic. Sequence selects the newest version during index insertion; it
            // is not a valid end-of-block marker. Blob-index checksums remain the scanner's validity boundary.
            recovered.extend(infos);
        }

        Ok(recovered)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use foyer_common::{metrics::Metrics, spawn::Spawner};
    use tempfile::tempdir;

    use super::BlockRecoverRunner;
    use crate::{
        engine::{
            block::{
                buffer::{Buffer, SplitCtx, Splitter},
                manager::Block,
            },
            RecoverMode,
        },
        io::{
            bytes::IoSliceMut,
            device::{fs::FsDeviceBuilder, DeviceBuilder},
            engine::{psync::PsyncIoEngineConfig, IoEngineBuildContext, IoEngineConfig},
        },
        Compression,
    };

    const KB: usize = 1024;

    #[test_log::test(tokio::test)]
    async fn test_block_recovery_keeps_out_of_order_sequences() {
        const BLOCK_SIZE: usize = 64 * KB;
        const BLOB_INDEX_SIZE: usize = 4 * KB;

        let dir = tempdir().unwrap();
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(BLOCK_SIZE)
            .build()
            .unwrap();
        let partition = device.create_partition(BLOCK_SIZE).unwrap();
        let io_engine = PsyncIoEngineConfig::new()
            .boxed()
            .build(IoEngineBuildContext {
                spawner: Spawner::current(),
            })
            .await
            .unwrap();

        let mut buffer = Buffer::new(
            IoSliceMut::new(BLOCK_SIZE),
            BLOCK_SIZE - BLOB_INDEX_SIZE,
            Arc::new(Metrics::noop()),
        );
        for (key, sequence) in [2, 0, 1].into_iter().enumerate() {
            assert!(buffer.push(
                &(key as u64),
                &vec![key as u8; 3 * KB],
                key as u64,
                Compression::None,
                sequence,
            ));
        }

        let (bytes, infos) = buffer.finish();
        let mut split = SplitCtx::new(BLOCK_SIZE, BLOB_INDEX_SIZE);
        let batch = Splitter::split(&mut split, bytes.into_io_slice(), infos);
        assert_eq!(batch.blocks.len(), 1);
        assert_eq!(batch.blocks[0].blob_parts.len(), 1);
        let part = &batch.blocks[0].blob_parts[0];

        let (_, result) = io_engine
            .write(
                Box::new(part.data.clone()),
                partition.as_ref(),
                part.blob_block_offset as u64 + part.part_blob_offset as u64,
            )
            .await;
        result.unwrap();
        let (_, result) = io_engine
            .write(
                Box::new(part.index.clone()),
                partition.as_ref(),
                part.blob_block_offset as u64,
            )
            .await;
        result.unwrap();

        let block = Block::new_for_test(0, partition, io_engine);
        let recovered = BlockRecoverRunner::run(RecoverMode::Strict, block, BLOB_INDEX_SIZE)
            .await
            .unwrap();
        let sequences = recovered.into_iter().map(|info| info.addr.sequence).collect::<Vec<_>>();
        assert_eq!(sequences, vec![2, 0, 1]);
    }
}
