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
package org.pangrova.coordinator.worker;

/**
 * Describes a vector index shard loaded on a worker.
 * Reported via heartbeat for shard registration with VectorShardRouter.
 *
 * @param tableName    table the index belongs to
 * @param vectorColumn indexed vector column
 * @param shardId      opaque shard identifier
 * @param generation   index build generation
 * @param rowCount     approximate number of rows in this shard
 */
public record VectorShardInfo(
        String tableName,
        String vectorColumn,
        String shardId,
        int generation,
        long rowCount) {

    public VectorShardInfo {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName required");
        }
        if (vectorColumn == null || vectorColumn.isBlank()) {
            throw new IllegalArgumentException("vectorColumn required");
        }
        if (shardId == null || shardId.isBlank()) {
            throw new IllegalArgumentException("shardId required");
        }
    }
}
