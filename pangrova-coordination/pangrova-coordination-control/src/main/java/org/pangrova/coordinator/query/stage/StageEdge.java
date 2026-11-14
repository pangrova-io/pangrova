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
package org.pangrova.coordinator.query.stage;

import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.shuffle.exchange.ExchangeReadMode;

import java.util.List;

/**
 * Metadata for a directed edge between two execution stages.
 * Carries the shuffle type and hash key indices derived from the fragmented plan,
 * enabling downstream consumers to derive the correct {@link ExchangeReadMode}
 * and shuffle partition key configuration.
 *
 * @param upstreamStageId   the producing stage
 * @param downstreamStageId the consuming stage
 * @param shuffleType       the shuffle distribution type for this edge
 * @param distributionKeys  column indices used for hash partitioning (empty for non-hash)
 * @param exchangeId         stable plan-level exchange identifier
 * @param partitionCount     number of downstream output partitions
 * @param batchSerde         protocol-level batch serialization format
 * @param requireComplete    whether all committed upstream partitions are required
 */
public record StageEdge(
        int upstreamStageId,
        int downstreamStageId,
        ShuffleType shuffleType,
        List<Integer> distributionKeys,
        String exchangeId,
        int partitionCount,
        BatchSerde batchSerde,
        boolean requireComplete
) {

    /**
     * Constructor without distribution keys.
     */
    public StageEdge(int upstreamStageId, int downstreamStageId, ShuffleType shuffleType) {
        this(upstreamStageId, downstreamStageId, shuffleType, List.of());
    }

    /**
     * Constructor for topology-only DAGs; executable plans use the full record.
     */
    public StageEdge(
            int upstreamStageId,
            int downstreamStageId,
            ShuffleType shuffleType,
            List<Integer> distributionKeys) {
        this(upstreamStageId, downstreamStageId, shuffleType, distributionKeys,
                "exchange-" + upstreamStageId + "-to-" + downstreamStageId,
                1, BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
    }

    public StageEdge {
        if (shuffleType == null) {
            throw new NullPointerException("shuffleType must not be null");
        }
        distributionKeys = distributionKeys == null ? List.of() : List.copyOf(distributionKeys);
        if (exchangeId == null || exchangeId.isBlank()) {
            throw new IllegalArgumentException("exchangeId must not be blank");
        }
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        if (batchSerde != BatchSerde.BATCH_SERDE_ARROW_BATCH) {
            throw new IllegalArgumentException("batchSerde must be the canonical columnar batch format");
        }
    }

    /**
     * Derives the {@link ExchangeReadMode} from the shuffle type.
     * Complete exchanges must wait for every committed upstream partition;
     * only explicitly non-complete broadcast/singleton edges allow streaming.
     */
    public ExchangeReadMode readMode() {
        if (requireComplete) {
            return ExchangeReadMode.BLOCKING;
        }
        return switch (shuffleType) {
            case BROADCAST, SINGLETON -> ExchangeReadMode.STREAMING;
            default -> ExchangeReadMode.BLOCKING;
        };
    }

    /** External shuffle registration currently accepts hash outputs only. */
    public boolean isHashShuffle() {
        return shuffleType == ShuffleType.HASH;
    }

}
