// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;
use std::sync::atomic::Ordering;

use risingwave_common::sequence::{SEQUENCE_GLOBAL, Sequence};
#[cfg(feature = "java")]
use risingwave_jni_core::jvm_runtime::load_jvm_memory_stats;
use risingwave_stream::executor::monitor::StreamingMetrics;

use super::manager::MemoryManagerConfig;

pub enum LruEvictionPolicy {
    None,
    Stable,
    Graceful,
    Aggressive,
}

impl From<LruEvictionPolicy> for u8 {
    fn from(value: LruEvictionPolicy) -> Self {
        match value {
            LruEvictionPolicy::None => 0,
            LruEvictionPolicy::Stable => 1,
            LruEvictionPolicy::Graceful => 2,
            LruEvictionPolicy::Aggressive => 3,
        }
    }
}

/// - `allocated`: Total number of bytes allocated by the application.
/// - `active`: Total number of bytes in active pages allocated by the application. This is a multiple of the page size, and greater than or equal to `stats.allocated`. This does not include `stats.arenas.<i>.pdirty`, `stats.arenas.<i>.pmuzzy`, nor pages entirely devoted to allocator metadata.
/// - `resident`: Total number of bytes in physically resident data pages mapped by the allocator.
/// - `metadata`: Total number of bytes dedicated to jemalloc metadata.
///
/// Reference: <https://jemalloc.net/jemalloc.3.html>
#[cfg(unix)]
pub struct MemoryStats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub metadata: usize,
}

/// `LruWatermarkController` controls LRU Watermark (epoch) according to actual memory usage statistics
/// collected from Jemalloc and JVM on Unix, or process private committed memory on Windows.
///
/// Basically, it works as a negative feedback loop: collect memory statistics, and then set the LRU watarmarking
/// according to maintain the memory usage in a proper level.
///
/// ```text
///     ┌───────────────────┐        ┌───────────────────┐
///     │       INPUT       │        │       OUTPUT      │
/// ┌───►     (observe)     ├───────►│     (control)     ├───┐
/// │   │ Memory Statistics │        │ New LRU Watermark │   │
/// │   └───────────────────┘        └───────────────────┘   │
/// │                                                        │
/// └────────────────────────────────────────────────────────┘
/// ```
///
/// Check the function [`Self::tick()`] to see the control policy.
pub struct LruWatermarkController {
    metrics: Arc<StreamingMetrics>,

    threshold_stable: usize,
    threshold_graceful: usize,
    threshold_aggressive: usize,

    eviction_factor_stable: f64,
    eviction_factor_graceful: f64,
    eviction_factor_aggressive: f64,

    watermark_sequence: Sequence,

    #[cfg(windows)]
    process_metrics: sysinfo::System,
}

impl LruWatermarkController {
    pub fn new(config: &MemoryManagerConfig) -> Self {
        let threshold_stable = (config.target_memory as f64 * config.threshold_stable) as usize;
        let threshold_graceful = (config.target_memory as f64 * config.threshold_graceful) as usize;
        let threshold_aggressive =
            (config.target_memory as f64 * config.threshold_aggressive) as usize;

        Self {
            metrics: config.metrics.clone(),
            threshold_stable,
            threshold_graceful,
            threshold_aggressive,
            eviction_factor_stable: config.eviction_factor_stable,
            eviction_factor_graceful: config.eviction_factor_graceful,
            eviction_factor_aggressive: config.eviction_factor_aggressive,
            watermark_sequence: 0,
            #[cfg(windows)]
            process_metrics: sysinfo::System::new(),
        }
    }
}

impl std::fmt::Debug for LruWatermarkController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruWatermarkController")
            .field("threshold_stable", &self.threshold_stable)
            .field("threshold_graceful", &self.threshold_graceful)
            .field("threshold_aggressive", &self.threshold_aggressive)
            .finish()
    }
}

/// Get memory statistics from Jemalloc
///
/// - `stats.allocated`: Total number of bytes allocated by the application.
/// - `stats.active`: Total number of bytes in active pages allocated by the application. This is a multiple of the page size, and greater than or equal to `stats.allocated`. This does not include `stats.arenas.<i>.pdirty`, `stats.arenas.<i>.pmuzzy`, nor pages entirely devoted to allocator metadata.
/// - `stats.resident`: Total number of bytes in physically resident data pages mapped by the allocator.
/// - `stats.metadata`: Total number of bytes dedicated to jemalloc metadata.
///
/// Reference: <https://jemalloc.net/jemalloc.3.html>
#[cfg(unix)]
fn jemalloc_memory_stats() -> MemoryStats {
    if let Err(e) = tikv_jemalloc_ctl::epoch::advance() {
        tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
    }
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap();
    let active = tikv_jemalloc_ctl::stats::active::read().unwrap();
    let resident = tikv_jemalloc_ctl::stats::resident::read().unwrap();
    let metadata = tikv_jemalloc_ctl::stats::metadata::read().unwrap();
    MemoryStats {
        allocated,
        active,
        resident,
        metadata,
    }
}

impl LruWatermarkController {
    pub fn tick(&mut self) -> Sequence {
        // NOTE: Be careful! The meaning of `allocated` and `active` differ in JeMalloc and JVM
        #[cfg(unix)]
        let MemoryStats {
            allocated: jemalloc_allocated_bytes,
            active: jemalloc_active_bytes,
            resident: jemalloc_resident_bytes,
            metadata: jemalloc_metadata_bytes,
        } = jemalloc_memory_stats();
        #[cfg(feature = "java")]
        let (jvm_allocated_bytes, jvm_active_bytes) = load_jvm_memory_stats();
        #[cfg(not(feature = "java"))]
        let (jvm_allocated_bytes, jvm_active_bytes) = (0, 0);

        #[cfg(unix)]
        let cur_used_memory_bytes = jemalloc_active_bytes + jvm_allocated_bytes;
        #[cfg(windows)]
        let cur_used_memory_bytes = {
            use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate};

            let pid = Pid::from_u32(std::process::id());
            self.process_metrics.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
            // sysinfo maps virtual_memory() to Windows PROCESS_MEMORY_COUNTERS_EX.PrivateUsage.
            // This includes the JVM, if enabled, so do not add JVM usage a second time.
            let private_bytes = self
                .process_metrics
                .process(pid)
                .expect("Failed to read current process memory usage")
                .virtual_memory();
            assert!(
                private_bytes > 0,
                "Windows process memory usage is unavailable"
            );
            usize::try_from(private_bytes).expect("Process memory usage does not fit usize")
        };

        // To calculate the total amount of memory that needs to be evicted, we sequentially calculate and accumulate
        // the memory amounts exceeding the thresholds for aggressive, graceful, and stable, multiplying each by
        // different weights.
        //
        //   (range)                 : (weight)
        // * aggressive ~ inf        : evict factor aggressive
        // *   graceful ~ aggressive : evict factor graceful
        // *     stable ~ graceful   : evict factor stable
        // *          0 ~ stable     : no eviction
        //
        // Why different weights instead of 1.0? It acts like a penalty factor, used to penalize the system for not
        // keeping the memory usage down at the lower thresholds.
        let to_evict_bytes = cur_used_memory_bytes.saturating_sub(self.threshold_aggressive) as f64
            * self.eviction_factor_aggressive
            + cur_used_memory_bytes
                .saturating_sub(self.threshold_graceful)
                .min(self.threshold_aggressive - self.threshold_graceful) as f64
                * self.eviction_factor_graceful
            + cur_used_memory_bytes
                .saturating_sub(self.threshold_stable)
                .min(self.threshold_graceful - self.threshold_stable) as f64
                * self.eviction_factor_stable;
        let ratio = to_evict_bytes / cur_used_memory_bytes as f64;
        let latest_sequence = SEQUENCE_GLOBAL.load(Ordering::Relaxed);
        let sequence_diff =
            ((latest_sequence - self.watermark_sequence) as f64 * ratio) as Sequence;
        self.watermark_sequence = latest_sequence.min(self.watermark_sequence + sequence_diff);

        let policy = if cur_used_memory_bytes > self.threshold_aggressive {
            LruEvictionPolicy::Aggressive
        } else if cur_used_memory_bytes > self.threshold_graceful {
            LruEvictionPolicy::Graceful
        } else if cur_used_memory_bytes > self.threshold_stable {
            LruEvictionPolicy::Stable
        } else {
            LruEvictionPolicy::None
        };

        self.metrics.lru_latest_sequence.set(latest_sequence as _);
        self.metrics
            .lru_watermark_sequence
            .set(self.watermark_sequence as _);
        self.metrics
            .lru_eviction_policy
            .set(Into::<u8>::into(policy) as _);

        #[cfg(unix)]
        {
            self.metrics
                .jemalloc_allocated_bytes
                .set(jemalloc_allocated_bytes as _);
            self.metrics
                .jemalloc_active_bytes
                .set(jemalloc_active_bytes as _);
            self.metrics
                .jemalloc_resident_bytes
                .set(jemalloc_resident_bytes as _);
            self.metrics
                .jemalloc_metadata_bytes
                .set(jemalloc_metadata_bytes as _);
        }
        self.metrics
            .jvm_allocated_bytes
            .set(jvm_allocated_bytes as _);
        self.metrics.jvm_active_bytes.set(jvm_active_bytes as _);

        self.watermark_sequence
    }
}

#[cfg(all(test, windows))]
mod tests {
    use risingwave_common::config::MetricLevel;

    use super::*;

    #[test]
    fn windows_process_memory_drives_eviction_policy() {
        let metrics = Arc::new(StreamingMetrics::new(
            &prometheus::Registry::new(),
            MetricLevel::Disabled,
        ));
        let mut config = MemoryManagerConfig {
            target_memory: usize::MAX,
            threshold_stable: 0.7,
            threshold_graceful: 0.8,
            threshold_aggressive: 0.9,
            eviction_factor_stable: 0.1,
            eviction_factor_graceful: 0.5,
            eviction_factor_aggressive: 1.0,
            metrics: metrics.clone(),
        };

        LruWatermarkController::new(&config).tick();
        assert_eq!(
            metrics.lru_eviction_policy.get(),
            u8::from(LruEvictionPolicy::None) as i64
        );

        // Any real Windows process exceeds this budget. A missing/zero memory
        // reading must never leave eviction disabled.
        config.target_memory = 1;
        LruWatermarkController::new(&config).tick();
        assert_eq!(
            metrics.lru_eviction_policy.get(),
            u8::from(LruEvictionPolicy::Aggressive) as i64
        );
    }
}
