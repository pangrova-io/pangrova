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
package org.pangrova.planner;

import java.util.List;

/**
 * Metadata for a directed edge between two execution stages produced by plan fragmentation.
 * Coordinator-friendly: uses {@link ShuffleType} instead of Calcite's RelDistribution.Type.
 */
public record StageTransfer(
        int upstreamStageId,
        int downstreamStageId,
        ShuffleType shuffleType,
        List<Integer> distributionKeys
) {
    public StageTransfer {
        if (shuffleType == null) {
            throw new NullPointerException("shuffleType must not be null");
        }
        distributionKeys = distributionKeys == null ? List.of() : List.copyOf(distributionKeys);
    }

    public StageTransfer(int upstreamStageId, int downstreamStageId, ShuffleType shuffleType) {
        this(upstreamStageId, downstreamStageId, shuffleType, List.of());
    }
}
