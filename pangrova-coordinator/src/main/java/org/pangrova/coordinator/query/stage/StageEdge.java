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

import org.pangrova.planner.ShuffleType;
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
 */
public record StageEdge(
        int upstreamStageId,
        int downstreamStageId,
        ShuffleType shuffleType,
        List<Integer> distributionKeys
) {

    /**
     * Constructor without distribution keys.
     */
    public StageEdge(int upstreamStageId, int downstreamStageId, ShuffleType shuffleType) {
        this(upstreamStageId, downstreamStageId, shuffleType, List.of());
    }

    public StageEdge {
        if (shuffleType == null) {
            throw new NullPointerException("shuffleType must not be null");
        }
        distributionKeys = distributionKeys == null ? List.of() : List.copyOf(distributionKeys);
    }

    /**
     * Derives the {@link ExchangeReadMode} from the shuffle type.
     * Broadcast and singleton distributions allow streaming reads;
     * hash and range distributions require blocking until upstream commits.
     */
    public ExchangeReadMode readMode() {
        return switch (shuffleType) {
            case BROADCAST, SINGLETON -> ExchangeReadMode.STREAMING;
            default -> ExchangeReadMode.BLOCKING;
        };
    }

    /**
     * Whether this edge represents a hash-distributed shuffle.
     */
    public boolean isHashShuffle() {
        return shuffleType == ShuffleType.HASH;
    }
}
