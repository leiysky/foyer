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

use std::{borrow::Cow, fmt::Debug};

use mixtrics::metrics::{BoxedCounter, BoxedGauge, BoxedHistogram, BoxedRegistry, Buckets};

#[expect(missing_docs)]
pub struct Metrics {
    /* in-memory cache metrics */
    pub memory_insert: BoxedCounter,
    pub memory_replace: BoxedCounter,
    pub memory_hit: BoxedCounter,
    pub memory_miss: BoxedCounter,
    pub memory_remove: BoxedCounter,
    pub memory_evict: BoxedCounter,
    pub memory_reinsert: BoxedCounter,
    pub memory_release: BoxedCounter,
    pub memory_queue: BoxedCounter,
    pub memory_fetch: BoxedCounter,

    pub memory_usage: BoxedGauge,
    pub memory_entries: BoxedGauge,

    /* disk cache metrics */
    pub storage_enqueue: BoxedCounter,
    pub storage_hit: BoxedCounter,
    pub storage_miss: BoxedCounter,
    pub storage_throttled: BoxedCounter,
    pub storage_delete: BoxedCounter,
    pub storage_error: BoxedCounter,
    pub storage_false_positive: BoxedCounter,

    pub storage_enqueue_duration: BoxedHistogram,
    pub storage_hit_duration: BoxedHistogram,
    pub storage_miss_duration: BoxedHistogram,
    pub storage_throttled_duration: BoxedHistogram,
    pub storage_delete_duration: BoxedHistogram,

    pub storage_queue_rotate: BoxedCounter,
    pub storage_queue_rotate_duration: BoxedHistogram,
    pub storage_queue_buffer_overflow: BoxedCounter,
    pub storage_queue_channel_overflow: BoxedCounter,

    pub storage_engine_command_accepted: BoxedCounter,
    pub storage_engine_command_dropped: BoxedCounter,
    pub storage_engine_command_completed: BoxedCounter,
    pub storage_engine_command_rejected: BoxedCounter,
    pub storage_engine_command_shutdown_dropped: BoxedCounter,
    pub storage_engine_command_shed_low: BoxedCounter,
    pub storage_engine_command_shed_normal: BoxedCounter,
    pub storage_engine_command_shed_high: BoxedCounter,
    pub storage_engine_batch_completed: BoxedCounter,
    pub storage_engine_batch_failed: BoxedCounter,

    pub storage_engine_read_rejected: BoxedCounter,
    pub storage_engine_read_active: BoxedGauge,
    pub storage_engine_read_limit: BoxedGauge,

    pub storage_engine_batch_duration: BoxedHistogram,
    pub storage_engine_publication_duration: BoxedHistogram,
    pub storage_engine_recovery_duration: BoxedHistogram,
    pub storage_engine_shutdown_duration: BoxedHistogram,

    pub storage_engine_recovery_created: BoxedCounter,
    pub storage_engine_recovery_recovered: BoxedCounter,
    pub storage_engine_recovery_recreated: BoxedCounter,

    pub storage_engine_queue_pending_entries: BoxedGauge,
    pub storage_engine_queue_pending_bytes: BoxedGauge,
    pub storage_engine_queue_capacity_entries: BoxedGauge,
    pub storage_engine_queue_capacity_bytes: BoxedGauge,

    pub storage_engine_checkpoint_published: BoxedGauge,
    pub storage_engine_checkpoint_requested: BoxedGauge,
    pub storage_engine_checkpoint_durable: BoxedGauge,
    pub storage_engine_checkpoint_in_flight: BoxedGauge,
    pub storage_engine_checkpoint_dirty: BoxedGauge,
    pub storage_engine_priority_extents: [BoxedGauge; 3],
    pub storage_engine_priority_floor_extents: [BoxedGauge; 3],
    pub storage_engine_priority_allocated_bytes: [BoxedGauge; 3],
    pub storage_engine_healthy: BoxedGauge,

    pub storage_disk_write: BoxedCounter,
    pub storage_disk_read: BoxedCounter,
    pub storage_disk_flush: BoxedCounter,

    pub storage_disk_write_bytes: BoxedCounter,
    pub storage_disk_read_bytes: BoxedCounter,

    pub storage_disk_write_duration: BoxedHistogram,
    pub storage_disk_read_duration: BoxedHistogram,
    pub storage_disk_flush_duration: BoxedHistogram,

    pub storage_block_engine_block_clean: BoxedGauge,
    pub storage_block_engine_block_writing: BoxedGauge,
    pub storage_block_engine_block_evictable: BoxedGauge,
    pub storage_block_engine_block_reclaiming: BoxedGauge,

    pub storage_block_engine_block_size_bytes: BoxedGauge,

    pub storage_entry_serialize_duration: BoxedHistogram,
    pub storage_entry_deserialize_duration: BoxedHistogram,

    pub storage_block_engine_indexer_conflict: BoxedCounter,
    pub storage_block_engine_enqueue_skip: BoxedCounter,
    pub storage_block_engine_buffer_efficiency: BoxedHistogram,
    pub storage_block_engine_recover_duration: BoxedHistogram,

    /* hybrid cache metrics */
    pub hybrid_insert: BoxedCounter,
    pub hybrid_hit: BoxedCounter,
    pub hybrid_miss: BoxedCounter,
    pub hybrid_throttled: BoxedCounter,
    pub hybrid_remove: BoxedCounter,
    pub hybrid_error: BoxedCounter,

    pub hybrid_insert_duration: BoxedHistogram,
    pub hybrid_hit_duration: BoxedHistogram,
    pub hybrid_miss_duration: BoxedHistogram,
    pub hybrid_throttled_duration: BoxedHistogram,
    pub hybrid_remove_duration: BoxedHistogram,
    pub hybrid_error_duration: BoxedHistogram,
}

impl Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish()
    }
}

impl Metrics {
    /// Create a new metric with the given name.
    pub fn new(name: impl Into<Cow<'static, str>>, registry: &BoxedRegistry) -> Self {
        let name = name.into();

        /* in-memory cache metrics */

        let foyer_memory_op_total = registry.register_counter_vec(
            "foyer_memory_op_total".into(),
            "foyer in-memory cache operations".into(),
            &["name", "op"],
        );
        let foyer_memory_usage = registry.register_gauge_vec(
            "foyer_memory_usage".into(),
            "foyer in-memory cache usage".into(),
            &["name"],
        );
        let foyer_memory_entries = registry.register_gauge_vec(
            "foyer_memory_entries".into(),
            "foyer in-memory cache entries".into(),
            &["name"],
        );

        let memory_insert = foyer_memory_op_total.counter(&[name.clone(), "insert".into()]);
        let memory_replace = foyer_memory_op_total.counter(&[name.clone(), "replace".into()]);
        let memory_hit = foyer_memory_op_total.counter(&[name.clone(), "hit".into()]);
        let memory_miss = foyer_memory_op_total.counter(&[name.clone(), "miss".into()]);
        let memory_remove = foyer_memory_op_total.counter(&[name.clone(), "remove".into()]);
        let memory_evict = foyer_memory_op_total.counter(&[name.clone(), "evict".into()]);
        let memory_reinsert = foyer_memory_op_total.counter(&[name.clone(), "reinsert".into()]);
        let memory_release = foyer_memory_op_total.counter(&[name.clone(), "release".into()]);
        let memory_queue = foyer_memory_op_total.counter(&[name.clone(), "queue".into()]);
        let memory_fetch = foyer_memory_op_total.counter(&[name.clone(), "fetch".into()]);

        let memory_usage = foyer_memory_usage.gauge(std::slice::from_ref(&name));
        let memory_entries = foyer_memory_entries.gauge(std::slice::from_ref(&name));

        /* disk cache metrics */

        let foyer_storage_op_total = registry.register_counter_vec(
            "foyer_storage_op_total".into(),
            "foyer disk cache operations".into(),
            &["name", "op"],
        );
        let foyer_storage_op_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_op_duration".into(),
            "foyer disk cache op durations".into(),
            &["name", "op"],
            // 1us ~ 4s
            Buckets::exponential(0.000_001, 2.0, 23),
        );

        let foyer_storage_inner_op_total = registry.register_counter_vec(
            "foyer_storage_inner_op_total".into(),
            "foyer disk cache inner operations".into(),
            &["name", "op"],
        );
        let foyer_storage_inner_op_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_inner_op_duration".into(),
            "foyer disk cache inner op durations".into(),
            &["name", "op"],
            // 1us ~ 16s
            Buckets::exponential(0.000_001, 2.0, 25),
        );

        let foyer_storage_engine_command_total = registry.register_counter_vec(
            "foyer_storage_engine_command_total".into(),
            "foyer disk engine asynchronous command state transitions".into(),
            &["name", "state"],
        );
        let foyer_storage_engine_batch_total = registry.register_counter_vec(
            "foyer_storage_engine_batch_total".into(),
            "foyer disk engine asynchronous batch outcomes".into(),
            &["name", "outcome"],
        );
        let foyer_storage_engine_read_total = registry.register_counter_vec(
            "foyer_storage_engine_read_total".into(),
            "foyer disk engine read admission outcomes".into(),
            &["name", "outcome"],
        );
        let foyer_storage_engine_readers = registry.register_gauge_vec(
            "foyer_storage_engine_readers".into(),
            "foyer disk engine active and permitted readers".into(),
            &["name", "state"],
        );
        let foyer_storage_engine_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_engine_duration".into(),
            "foyer disk engine operation durations".into(),
            &["name", "operation"],
            // 1us ~ 1024s
            Buckets::exponential(0.000_001, 2.0, 31),
        );
        let foyer_storage_engine_recovery_total = registry.register_counter_vec(
            "foyer_storage_engine_recovery_total".into(),
            "foyer disk engine recovery outcomes".into(),
            &["name", "outcome"],
        );
        let foyer_storage_engine_queue_entries = registry.register_gauge_vec(
            "foyer_storage_engine_queue_entries".into(),
            "foyer disk engine queue entries".into(),
            &["name", "state"],
        );
        let foyer_storage_engine_queue_bytes = registry.register_gauge_vec(
            "foyer_storage_engine_queue_bytes".into(),
            "foyer disk engine queue bytes".into(),
            &["name", "state"],
        );
        let foyer_storage_engine_checkpoint = registry.register_gauge_vec(
            "foyer_storage_engine_checkpoint".into(),
            "foyer disk engine checkpoint frontiers and dirty work".into(),
            &["name", "measure"],
        );
        let foyer_storage_engine_priority_extents = registry.register_gauge_vec(
            "foyer_storage_engine_priority_extents".into(),
            "foyer disk engine occupied and protected-floor extents by cache priority".into(),
            &["name", "priority", "state"],
        );
        let foyer_storage_engine_priority_allocated_bytes = registry.register_gauge_vec(
            "foyer_storage_engine_priority_allocated_bytes".into(),
            "foyer disk engine physically allocated payload bytes by cache priority".into(),
            &["name", "priority"],
        );
        let foyer_storage_engine_healthy = registry.register_gauge_vec(
            "foyer_storage_engine_healthy".into(),
            "whether the foyer disk engine background pipeline is healthy".into(),
            &["name"],
        );

        let foyer_storage_disk_io_total = registry.register_counter_vec(
            "foyer_storage_disk_io_total".into(),
            "foyer disk cache disk operations".into(),
            &["name", "op"],
        );
        let foyer_storage_disk_io_bytes = registry.register_counter_vec(
            "foyer_storage_disk_io_bytes_total".into(),
            "foyer disk cache disk io bytes".into(),
            &["name", "op"],
        );
        let foyer_storage_disk_io_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_disk_io_duration".into(),
            "foyer disk cache disk io duration".into(),
            &["name", "op"],
            // 1us ~ 4s
            Buckets::exponential(0.000_001, 2.0, 23),
        );

        let foyer_storage_block_engine_block = registry.register_gauge_vec(
            "foyer_storage_block_engine_block".into(),
            "foyer large object disk cache blocks".into(),
            &["name", "type"],
        );
        let foyer_storage_block_engine_block_size_bytes = registry.register_gauge_vec(
            "foyer_storage_block_engine_block_size_bytes".into(),
            "foyer large object disk cache blocks sizes".into(),
            &["name"],
        );

        let foyer_storage_entry_serde_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_entry_serde_duration".into(),
            "foyer disk cache entry serde durations".into(),
            &["name", "op"],
            // 10ns ~ 40ms
            Buckets::exponential(0.000_000_01, 2.0, 23),
        );

        let foyer_storage_block_engine_op_total = registry.register_counter_vec(
            "foyer_storage_block_engine_op_total".into(),
            "foyer large object disk cache operations".into(),
            &["name", "op"],
        );

        let foyer_storage_block_engine_buffer_efficiency = registry.register_histogram_vec_with_buckets(
            "foyer_storage_block_engine_buffer_efficiency".into(),
            "foyer large object disk cache buffer efficiency".into(),
            &["name"],
            // 0% ~ 100%
            Buckets::linear(0.1, 0.1, 10),
        );

        let foyer_storage_block_engine_recover_duration = registry.register_histogram_vec_with_buckets(
            "foyer_storage_block_engine_recover_duration".into(),
            "foyer large object disk cache recover duration".into(),
            &["name"],
            // 1ms ~ 1000s
            Buckets::exponential(0.001, 2.0, 21),
        );

        let storage_enqueue = foyer_storage_op_total.counter(&[name.clone(), "enqueue".into()]);
        let storage_hit = foyer_storage_op_total.counter(&[name.clone(), "hit".into()]);
        let storage_miss = foyer_storage_op_total.counter(&[name.clone(), "miss".into()]);
        let storage_delete = foyer_storage_op_total.counter(&[name.clone(), "delete".into()]);
        let storage_throttled = foyer_storage_op_total.counter(&[name.clone(), "throttled".into()]);
        let storage_error = foyer_storage_op_total.counter(&[name.clone(), "error".into()]);
        let storage_false_positive = foyer_storage_op_total.counter(&[name.clone(), "false_positive".into()]);

        let storage_enqueue_duration = foyer_storage_op_duration.histogram(&[name.clone(), "enqueue".into()]);
        let storage_hit_duration = foyer_storage_op_duration.histogram(&[name.clone(), "hit".into()]);
        let storage_miss_duration = foyer_storage_op_duration.histogram(&[name.clone(), "miss".into()]);
        let storage_throttled_duration = foyer_storage_op_duration.histogram(&[name.clone(), "throttled".into()]);
        let storage_delete_duration = foyer_storage_op_duration.histogram(&[name.clone(), "delete".into()]);

        let storage_queue_rotate = foyer_storage_inner_op_total.counter(&[name.clone(), "queue_rotate".into()]);
        let storage_queue_buffer_overflow =
            foyer_storage_inner_op_total.counter(&[name.clone(), "buffer_overflow".into()]);
        let storage_queue_channel_overflow =
            foyer_storage_inner_op_total.counter(&[name.clone(), "channel_overflow".into()]);

        let storage_queue_rotate_duration =
            foyer_storage_inner_op_duration.histogram(&[name.clone(), "queue_rotate".into()]);

        let storage_engine_command_accepted =
            foyer_storage_engine_command_total.counter(&[name.clone(), "accepted".into()]);
        let storage_engine_command_dropped =
            foyer_storage_engine_command_total.counter(&[name.clone(), "dropped".into()]);
        let storage_engine_command_completed =
            foyer_storage_engine_command_total.counter(&[name.clone(), "completed".into()]);
        let storage_engine_command_rejected =
            foyer_storage_engine_command_total.counter(&[name.clone(), "storage_rejected".into()]);
        let storage_engine_command_shutdown_dropped =
            foyer_storage_engine_command_total.counter(&[name.clone(), "shutdown_dropped".into()]);
        let storage_engine_command_shed_low =
            foyer_storage_engine_command_total.counter(&[name.clone(), "shed_low".into()]);
        let storage_engine_command_shed_normal =
            foyer_storage_engine_command_total.counter(&[name.clone(), "shed_normal".into()]);
        let storage_engine_command_shed_high =
            foyer_storage_engine_command_total.counter(&[name.clone(), "shed_high".into()]);
        let storage_engine_batch_completed =
            foyer_storage_engine_batch_total.counter(&[name.clone(), "completed".into()]);
        let storage_engine_batch_failed = foyer_storage_engine_batch_total.counter(&[name.clone(), "failed".into()]);

        let storage_engine_read_rejected = foyer_storage_engine_read_total.counter(&[name.clone(), "rejected".into()]);
        let storage_engine_read_active = foyer_storage_engine_readers.gauge(&[name.clone(), "active".into()]);
        let storage_engine_read_limit = foyer_storage_engine_readers.gauge(&[name.clone(), "limit".into()]);
        let storage_engine_batch_duration = foyer_storage_engine_duration.histogram(&[name.clone(), "batch".into()]);
        let storage_engine_publication_duration =
            foyer_storage_engine_duration.histogram(&[name.clone(), "publication".into()]);
        let storage_engine_recovery_duration =
            foyer_storage_engine_duration.histogram(&[name.clone(), "recovery".into()]);
        let storage_engine_shutdown_duration =
            foyer_storage_engine_duration.histogram(&[name.clone(), "shutdown".into()]);
        let storage_engine_recovery_created =
            foyer_storage_engine_recovery_total.counter(&[name.clone(), "created".into()]);
        let storage_engine_recovery_recovered =
            foyer_storage_engine_recovery_total.counter(&[name.clone(), "recovered".into()]);
        let storage_engine_recovery_recreated =
            foyer_storage_engine_recovery_total.counter(&[name.clone(), "recreated".into()]);

        let storage_engine_queue_pending_entries =
            foyer_storage_engine_queue_entries.gauge(&[name.clone(), "pending".into()]);
        let storage_engine_queue_pending_bytes =
            foyer_storage_engine_queue_bytes.gauge(&[name.clone(), "pending".into()]);
        let storage_engine_queue_capacity_entries =
            foyer_storage_engine_queue_entries.gauge(&[name.clone(), "capacity".into()]);
        let storage_engine_queue_capacity_bytes =
            foyer_storage_engine_queue_bytes.gauge(&[name.clone(), "capacity".into()]);

        let storage_engine_checkpoint_published =
            foyer_storage_engine_checkpoint.gauge(&[name.clone(), "published_epoch".into()]);
        let storage_engine_checkpoint_requested =
            foyer_storage_engine_checkpoint.gauge(&[name.clone(), "requested_epoch".into()]);
        let storage_engine_checkpoint_durable =
            foyer_storage_engine_checkpoint.gauge(&[name.clone(), "durable_epoch".into()]);
        let storage_engine_checkpoint_in_flight =
            foyer_storage_engine_checkpoint.gauge(&[name.clone(), "in_flight_epoch".into()]);
        let storage_engine_checkpoint_dirty =
            foyer_storage_engine_checkpoint.gauge(&[name.clone(), "dirty_bytes".into()]);
        let priorities = ["low", "normal", "high"];
        let storage_engine_priority_extents = std::array::from_fn(|priority| {
            foyer_storage_engine_priority_extents.gauge(&[name.clone(), priorities[priority].into(), "occupied".into()])
        });
        let storage_engine_priority_floor_extents = std::array::from_fn(|priority| {
            foyer_storage_engine_priority_extents.gauge(&[name.clone(), priorities[priority].into(), "floor".into()])
        });
        let storage_engine_priority_allocated_bytes = std::array::from_fn(|priority| {
            foyer_storage_engine_priority_allocated_bytes.gauge(&[name.clone(), priorities[priority].into()])
        });
        let storage_engine_healthy = foyer_storage_engine_healthy.gauge(std::slice::from_ref(&name));
        storage_engine_healthy.absolute(1);

        let storage_disk_write = foyer_storage_disk_io_total.counter(&[name.clone(), "write".into()]);
        let storage_disk_read = foyer_storage_disk_io_total.counter(&[name.clone(), "read".into()]);
        let storage_disk_flush = foyer_storage_disk_io_total.counter(&[name.clone(), "flush".into()]);

        let storage_disk_write_bytes = foyer_storage_disk_io_bytes.counter(&[name.clone(), "write".into()]);
        let storage_disk_read_bytes = foyer_storage_disk_io_bytes.counter(&[name.clone(), "read".into()]);

        let storage_disk_write_duration = foyer_storage_disk_io_duration.histogram(&[name.clone(), "write".into()]);
        let storage_disk_read_duration = foyer_storage_disk_io_duration.histogram(&[name.clone(), "read".into()]);
        let storage_disk_flush_duration = foyer_storage_disk_io_duration.histogram(&[name.clone(), "flush".into()]);

        let storage_block_engine_block_clean = foyer_storage_block_engine_block.gauge(&[name.clone(), "clean".into()]);
        let storage_block_engine_block_writing =
            foyer_storage_block_engine_block.gauge(&[name.clone(), "writing".into()]);
        let storage_block_engine_block_evictable =
            foyer_storage_block_engine_block.gauge(&[name.clone(), "evictable".into()]);
        let storage_block_engine_block_reclaiming =
            foyer_storage_block_engine_block.gauge(&[name.clone(), "reclaiming".into()]);

        let storage_block_engine_block_size_bytes =
            foyer_storage_block_engine_block_size_bytes.gauge(std::slice::from_ref(&name));

        let storage_entry_serialize_duration =
            foyer_storage_entry_serde_duration.histogram(&[name.clone(), "serialize".into()]);
        let storage_entry_deserialize_duration =
            foyer_storage_entry_serde_duration.histogram(&[name.clone(), "deserialize".into()]);

        let storage_block_engine_indexer_conflict =
            foyer_storage_block_engine_op_total.counter(&[name.clone(), "indexer_conflict".into()]);
        let storage_block_engine_enqueue_skip =
            foyer_storage_block_engine_op_total.counter(&[name.clone(), "enqueue_skip".into()]);
        let storage_block_engine_buffer_efficiency =
            foyer_storage_block_engine_buffer_efficiency.histogram(std::slice::from_ref(&name));
        let storage_block_engine_recover_duration =
            foyer_storage_block_engine_recover_duration.histogram(std::slice::from_ref(&name));

        /* hybrid cache metrics */

        let foyer_hybrid_op_total = registry.register_counter_vec(
            "foyer_hybrid_op_total".into(),
            "foyer hybrid cache operations".into(),
            &["name", "op"],
        );
        let foyer_hybrid_op_duration = registry.register_histogram_vec(
            "foyer_hybrid_op_duration".into(),
            "foyer hybrid cache operation durations".into(),
            &["name", "op"],
        );

        let hybrid_insert = foyer_hybrid_op_total.counter(&[name.clone(), "insert".into()]);
        let hybrid_hit = foyer_hybrid_op_total.counter(&[name.clone(), "hit".into()]);
        let hybrid_miss = foyer_hybrid_op_total.counter(&[name.clone(), "miss".into()]);
        let hybrid_throttled = foyer_hybrid_op_total.counter(&[name.clone(), "throttled".into()]);
        let hybrid_remove = foyer_hybrid_op_total.counter(&[name.clone(), "remove".into()]);
        let hybrid_error = foyer_hybrid_op_total.counter(&[name.clone(), "error".into()]);

        let hybrid_insert_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "insert".into()]);
        let hybrid_hit_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "hit".into()]);
        let hybrid_miss_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "miss".into()]);
        let hybrid_throttled_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "throttled".into()]);
        let hybrid_remove_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "remove".into()]);
        let hybrid_error_duration = foyer_hybrid_op_duration.histogram(&[name.clone(), "error".into()]);

        Self {
            memory_insert,
            memory_replace,
            memory_hit,
            memory_miss,
            memory_remove,
            memory_evict,
            memory_reinsert,
            memory_release,
            memory_queue,
            memory_fetch,
            memory_usage,
            memory_entries,

            storage_enqueue,
            storage_hit,
            storage_miss,
            storage_throttled,
            storage_delete,
            storage_error,
            storage_false_positive,
            storage_enqueue_duration,
            storage_hit_duration,
            storage_miss_duration,
            storage_throttled_duration,
            storage_delete_duration,
            storage_queue_rotate,
            storage_queue_rotate_duration,
            storage_queue_buffer_overflow,
            storage_queue_channel_overflow,
            storage_engine_command_accepted,
            storage_engine_command_dropped,
            storage_engine_command_completed,
            storage_engine_command_rejected,
            storage_engine_command_shutdown_dropped,
            storage_engine_command_shed_low,
            storage_engine_command_shed_normal,
            storage_engine_command_shed_high,
            storage_engine_batch_completed,
            storage_engine_batch_failed,
            storage_engine_read_rejected,
            storage_engine_read_active,
            storage_engine_read_limit,
            storage_engine_batch_duration,
            storage_engine_publication_duration,
            storage_engine_recovery_duration,
            storage_engine_shutdown_duration,
            storage_engine_recovery_created,
            storage_engine_recovery_recovered,
            storage_engine_recovery_recreated,
            storage_engine_queue_pending_entries,
            storage_engine_queue_pending_bytes,
            storage_engine_queue_capacity_entries,
            storage_engine_queue_capacity_bytes,
            storage_engine_checkpoint_published,
            storage_engine_checkpoint_requested,
            storage_engine_checkpoint_durable,
            storage_engine_checkpoint_in_flight,
            storage_engine_checkpoint_dirty,
            storage_engine_priority_extents,
            storage_engine_priority_floor_extents,
            storage_engine_priority_allocated_bytes,
            storage_engine_healthy,
            storage_disk_write,
            storage_disk_read,
            storage_disk_flush,
            storage_disk_write_bytes,
            storage_disk_read_bytes,
            storage_disk_write_duration,
            storage_disk_read_duration,
            storage_disk_flush_duration,
            storage_block_engine_block_clean,
            storage_block_engine_block_writing,
            storage_block_engine_block_evictable,
            storage_block_engine_block_reclaiming,
            storage_block_engine_block_size_bytes,
            storage_entry_serialize_duration,
            storage_entry_deserialize_duration,
            storage_block_engine_indexer_conflict,
            storage_block_engine_enqueue_skip,
            storage_block_engine_buffer_efficiency,
            storage_block_engine_recover_duration,

            hybrid_insert,
            hybrid_hit,
            hybrid_miss,
            hybrid_throttled,
            hybrid_throttled_duration,
            hybrid_remove,
            hybrid_error,
            hybrid_insert_duration,
            hybrid_hit_duration,
            hybrid_miss_duration,
            hybrid_remove_duration,
            hybrid_error_duration,
        }
    }

    /// Build noop metrics.
    ///
    /// Note: `noop` is only supposed to be called by other foyer components.
    #[doc(hidden)]
    pub fn noop() -> Self {
        let registry: BoxedRegistry = Box::new(mixtrics::registry::noop::NoopMetricsRegistry);
        Self::new("test", &registry)
    }
}

#[cfg(test)]
mod tests {
    use mixtrics::metrics::BoxedRegistry;

    use super::Metrics;

    fn test_fn(registry: &BoxedRegistry) {
        Metrics::new("test", registry);
    }

    mixtrics::test! { test_fn }
}
