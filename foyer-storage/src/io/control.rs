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

use std::sync::Arc;

use crate::io::device::{statistics::Statistics, throttle::Throttle};

/// Engine-level I/O statistics and throttling state.
///
/// An engine may back this control plane with a [`crate::Device`] or own it independently.
#[derive(Debug, Clone)]
pub struct IoControl {
    statistics: Arc<Statistics>,
}

impl IoControl {
    /// Create an independent I/O control plane.
    pub fn new(throttle: Throttle) -> Self {
        Self {
            statistics: Arc::new(Statistics::new(throttle)),
        }
    }

    /// Reuse an existing statistics and throttling domain.
    pub fn from_statistics(statistics: Arc<Statistics>) -> Self {
        Self { statistics }
    }

    /// Get the I/O statistics.
    pub fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }

    /// Get the I/O throttle configuration.
    pub fn throttle(&self) -> &Throttle {
        self.statistics.throttle()
    }
}

impl Default for IoControl {
    fn default() -> Self {
        Self::new(Throttle::default())
    }
}
