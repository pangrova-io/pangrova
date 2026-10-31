/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.scheduler.api.placement;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable snapshot of a worker's current state for selection decisions.
 * Decoupled from implementation-specific WorkerInfo to keep the SPI layer
 * free of coordinator internals.
 *
 * @param id              unique worker identifier
 * @param host            worker hostname or IP
 * @param port            worker port
 * @param availableMemory available memory in bytes
 * @param activeTaskCount number of tasks currently running on this worker
 */
public record SchedulingWorkerSnapshot(
        String id,
        String host,
        int port,
        long availableMemory,
        int activeTaskCount) {

    public SchedulingWorkerSnapshot {
        checkNotNull(id, "id");
        checkNotNull(host, "host");
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive, got: " + port);
        }
        if (availableMemory < 0) {
            throw new IllegalArgumentException("availableMemory must be non-negative, got: " + availableMemory);
        }
        if (activeTaskCount < 0) {
            throw new IllegalArgumentException("activeTaskCount must be non-negative, got: " + activeTaskCount);
        }
    }
}
