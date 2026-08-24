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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resolves Pangrova query IDs to Celeborn shuffle IDs.
 *
 * <p>When a shuffle is registered via {@link CelebornLifecycleManager#registerShuffle},
 * the resulting Celeborn shuffle ID is stored here. Downstream components (sinks, sources,
 * byte egress) use this resolver to look up the shuffle ID for data operations.
 *
 * <p>This component is shared between the lifecycle manager and the exchange channel manager,
 * providing the queryId→shuffleId mapping that both need.
 *
 * <p>Thread safety: all public methods are safe for concurrent use via ConcurrentHashMap.
 */
public final class CelebornShuffleIdResolver {

    // PERF-T10: Lightweight composite key record eliminates per-lookup String concatenation.
    // record auto-generates hashCode/equals based on both fields.
    private record StageKey(String queryId, int stageId) {}

    /**
     * Maps (queryId, stageId) → Celeborn shuffleId (precise, always correct).
     */
    private final ConcurrentMap<StageKey, Integer> stageShuffleIds = new ConcurrentHashMap<>();

    /**
     * Tracks the number of active stages per queryId for ambiguity detection.
     */
    private final ConcurrentMap<String, AtomicInteger> stageCountPerQuery = new ConcurrentHashMap<>();

    /**
     * Fast path: stores the shuffleId when there is exactly ONE stage for the query.
     * When a second stage is registered, the entry is REMOVED to force fail-fast on
     * the 1-arg resolveShuffleId(queryId) call.
     */
    private final ConcurrentMap<String, Integer> singleStageShuffleIds = new ConcurrentHashMap<>();

    /**
     * Registers a mapping from queryId+stageId to Celeborn shuffleId.
     *
     * <p>If this is the first stage for the query, the shuffleId is also stored in the
     * single-stage fast-path map. If a second (or subsequent) stage is registered for the
     * same query, the fast-path entry is removed to prevent ambiguous lookups.
     */
    public void register(String queryId, int stageId, int shuffleId) {
        stageShuffleIds.put(new StageKey(queryId, stageId), shuffleId);

        AtomicInteger count = stageCountPerQuery
                .computeIfAbsent(queryId, k -> new AtomicInteger(0));
        int stages = count.incrementAndGet();

        if (stages == 1) {
            // First stage — safe for 1-arg resolution
            singleStageShuffleIds.put(queryId, shuffleId);
        } else {
            // Multiple stages — remove fast-path to trigger fail-fast
            singleStageShuffleIds.remove(queryId);
        }
    }

    /**
     * Resolves the Celeborn shuffle ID for a given query (single-stage fast path).
     *
     * <p>This method is only safe when the query has exactly ONE active stage. If multiple
     * stages are registered, it throws {@link IllegalStateException} requiring the caller
     * to use {@link #resolveShuffleId(String, int)} instead.
     *
     * @throws IllegalStateException if no shuffle is registered, or if multiple stages
     *         are active (ambiguous resolution)
     */
    public int resolveShuffleId(String queryId) {
        Integer id = singleStageShuffleIds.get(queryId);
        if (id != null) {
            return id;
        }

        // Check if the query exists but has multiple stages
        AtomicInteger count = stageCountPerQuery.get(queryId);
        if (count != null && count.get() > 1) {
            throw new IllegalStateException(
                    "Ambiguous Celeborn shuffle resolution for queryId=" + queryId
                    + ": " + count.get() + " stages are active. "
                    + "Use resolveShuffleId(queryId, stageId) for multi-stage queries.");
        }

        throw new IllegalStateException(
                "No Celeborn shuffle registered for queryId=" + queryId
                + ". Ensure registerShuffle was called before data operations.");
    }

    /**
     * Resolves the Celeborn shuffle ID for a specific stage (always precise).
     *
     * @throws IllegalStateException if no shuffle is registered for the query+stage
     */
    public int resolveShuffleId(String queryId, int stageId) {
        StageKey key = new StageKey(queryId, stageId);
        Integer id = stageShuffleIds.get(key);
        if (id == null) {
            // Fall back to single-stage resolution (works if only 1 stage exists)
            return resolveShuffleId(queryId);
        }
        return id;
    }

    /**
     * Removes all registrations for a given query.
     */
    public void removeQuery(String queryId) {
        // PERF-T10: Iterate StageKey entries and match by queryId field (no string ops).
        stageShuffleIds.keySet().removeIf(k -> k.queryId().equals(queryId));
        singleStageShuffleIds.remove(queryId);
        stageCountPerQuery.remove(queryId);
    }

    /**
     * Clears all registrations.
     */
    public void clear() {
        stageShuffleIds.clear();
        singleStageShuffleIds.clear();
        stageCountPerQuery.clear();
    }
}
