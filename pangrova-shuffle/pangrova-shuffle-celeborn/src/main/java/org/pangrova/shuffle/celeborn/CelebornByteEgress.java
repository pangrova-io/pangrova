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
package org.pangrova.shuffle.celeborn;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.pangrova.shuffle.ShuffleByteEgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Celeborn implementation of {@link ShuffleByteEgress} (ADR-002 D2-egress / D4).
 *
 * <p>Receives committed partition bytes from the native layer via JNI callback and pushes them
 * to Celeborn workers via {@link CelebornClientWrapper#pushData}. This replaces the
 * {@code DefaultShuffleByteEgress → SpoolStore} path with direct Celeborn push.
 *
 * <p>Thread safety: safe for concurrent calls from multiple JNI drain threads, as the underlying
 * {@code CelebornClientWrapper.pushData()} handles thread safety internally.
 *
 * <p>mapId/attemptId dedup: Celeborn handles deduplication server-side. If the same
 * (shuffleId, mapId, attemptId, partitionId) is pushed multiple times (e.g., due to task retry),
 * Celeborn will only retain one copy.
 */
public final class CelebornByteEgress implements ShuffleByteEgress {

    private static final Logger LOG = LoggerFactory.getLogger(CelebornByteEgress.class);

    private final CelebornClientWrapper clientWrapper;
    private final CelebornShuffleIdResolver shuffleIdResolver;

    public CelebornByteEgress(CelebornClientWrapper clientWrapper,
                              CelebornShuffleIdResolver shuffleIdResolver) {
        this.clientWrapper = Objects.requireNonNull(clientWrapper, "clientWrapper");
        this.shuffleIdResolver = Objects.requireNonNull(shuffleIdResolver, "shuffleIdResolver");
    }

    @Override
    public void onPartitionCommitted(String queryId, int stageId, int partitionId,
                                     int mapId, long attemptId, ByteBuffer arrowIpc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Celeborn egress: queryId={} stage={} partition={} map={} attempt={} bytes={}",
                    queryId, stageId, partitionId, mapId, attemptId, arrowIpc.remaining());
        }

        int shuffleId = shuffleIdResolver.resolveShuffleId(queryId, stageId);

        // Push the Arrow IPC bytes directly to Celeborn workers.
        // Celeborn handles replication and durability guarantees.
        clientWrapper.pushData(shuffleId, mapId, (int) attemptId, partitionId, arrowIpc);
    }
}
