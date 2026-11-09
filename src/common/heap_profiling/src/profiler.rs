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

#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::path::Path;

#[cfg(unix)]
use parking_lot::Once;
use risingwave_common::config::HeapProfilingConfig;
#[cfg(unix)]
use risingwave_common::util::resource_util;
#[cfg(unix)]
use tikv_jemalloc_ctl::{opt as jemalloc_opt, prof as jemalloc_prof};
#[cfg(unix)]
use tokio::time::{self, Duration};

#[cfg(unix)]
use super::AUTO_DUMP_SUFFIX;

/// `HeapProfiler` automatically triggers heap profiling when memory usage is higher than the threshold.
///
/// To use it, both jemalloc's `opt.prof` and RisingWave's config `heap_profiling.enable_auto` must be set to true.
pub struct HeapProfiler {
    config: HeapProfilingConfig,
    #[cfg(unix)]
    threshold_auto_dump_heap_profile: usize,
    #[cfg(unix)]
    jemalloc_dump_mib: jemalloc_prof::dump_mib,
    /// If jemalloc profiling is enabled
    #[cfg(unix)]
    opt_prof: bool,
}

impl HeapProfiler {
    /// # Arguments
    ///
    /// `total_memory` must be the total available memory for the process.
    /// It will be compared with the process resident memory.
    pub fn new(total_memory: usize, config: HeapProfilingConfig) -> Self {
        #[cfg(not(unix))]
        let _ = total_memory;
        #[cfg(unix)]
        let threshold_auto_dump_heap_profile =
            (total_memory as f64 * config.threshold_auto as f64) as usize;
        #[cfg(unix)]
        let jemalloc_dump_mib = jemalloc_prof::dump::mib().unwrap();
        #[cfg(unix)]
        let opt_prof = jemalloc_opt::prof::read().unwrap();

        Self {
            config,
            #[cfg(unix)]
            threshold_auto_dump_heap_profile,
            #[cfg(unix)]
            jemalloc_dump_mib,
            #[cfg(unix)]
            opt_prof,
        }
    }

    #[cfg(unix)]
    fn auto_dump_heap_prof(&self) {
        let time_prefix = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S");
        let file_name = format!("{}.{}", time_prefix, AUTO_DUMP_SUFFIX);

        let file_path = Path::new(&self.config.dir)
            .join(&file_name)
            .to_str()
            .expect("file path is not valid utf8")
            .to_owned();
        let file_path_c = CString::new(file_path).expect("0 byte in file path");

        // FIXME(yuhao): `unsafe` here because `jemalloc_dump_mib.write` requires static lifetime
        if let Err(e) = self
            .jemalloc_dump_mib
            .write(unsafe { &*(file_path_c.as_c_str() as *const _) })
        {
            tracing::warn!("Auto Jemalloc dump heap file failed! {:?}", e);
        } else {
            tracing::info!("Successfully dumped heap profile to {}", file_name);
        }
    }

    /// Start the daemon task of auto heap profiling.
    #[cfg(unix)]
    pub fn start(self) {
        if !self.config.enable_auto || !self.opt_prof {
            tracing::info!("Auto memory dump is disabled.");
            return;
        }

        static START: Once = Once::new();
        START.call_once(|| {
            fs::create_dir_all(&self.config.dir).unwrap();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(500));
                let mut prev_used_memory_bytes = 0;
                loop {
                    interval.tick().await;
                    let cur_used_memory_bytes = resource_util::memory::total_memory_used_bytes();

                    // Dump heap profile when memory usage is crossing the threshold.
                    if cur_used_memory_bytes > self.threshold_auto_dump_heap_profile
                        && prev_used_memory_bytes <= self.threshold_auto_dump_heap_profile
                    {
                        self.auto_dump_heap_prof();
                    }
                    prev_used_memory_bytes = cur_used_memory_bytes;
                }
            });
        })
    }

    #[cfg(not(unix))]
    pub fn start(self) {
        if self.config.enable_auto {
            tracing::info!("Automatic heap profiling is unavailable on this platform.");
        }
    }
}
