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
package org.pangrova.spi.indexing.publish;

import java.util.List;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.spi.indexing.descriptor.IndexType;

/**
 * Publishes index availability events to interested parties (typically workers).
 *
 * <p>After an index build completes, the coordinator calls
 * {@link #publishIndexReady(IndexReadyEvent)} to notify workers that a new index
 * segment is available for loading. Workers receiving this notification should
 * download/load the index and register the shard via heartbeat.</p>
 *
 * <p>Implementations may use RPC, message queue, or heartbeat-based notification.</p>
 */
@Stable(since = "1.0")
public interface IndexAvailabilityPublisher {

    /**
     * Notifies workers that an index is ready for use.
     *
     * @param event describes the newly available index
     */
    void publishIndexReady(IndexReadyEvent event);

    /**
     * Describes a newly available index segment.
     *
     * @param indexId          unique index identifier
     * @param tableName        table the index belongs to
     * @param vectorColumn     indexed vector column (null for non-vector indexes)
     * @param storagePath      path where the index files are stored (shared storage)
     * @param generation       index build generation
     * @param rowCount         number of rows indexed
     * @param indexType        the index type (e.g., VECTOR)
     * @param algorithmName    algorithm name (e.g., DISK_ANN, HNSW)
     * @param targetWorkerIds  specific worker IDs to notify (empty = all workers)
     */
    record IndexReadyEvent(
            String indexId,
            String tableName,
            String vectorColumn,
            String storagePath,
            int generation,
            long rowCount,
            IndexType indexType,
            String algorithmName,
            List<String> targetWorkerIds) {

        public IndexReadyEvent {
            if (indexId == null || indexId.isBlank()) {
                throw new IllegalArgumentException("indexId required");
            }
            if (storagePath == null || storagePath.isBlank()) {
                throw new IllegalArgumentException("storagePath required");
            }
            if (targetWorkerIds == null) {
                targetWorkerIds = List.of();
            } else {
                targetWorkerIds = List.copyOf(targetWorkerIds);
            }
        }
    }
}
