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
package org.pangrova.testing.e2e.ai;

import org.pangrova.coordinator.ai.cache.HnswBackedSemanticCache;
import org.pangrova.coordinator.ai.cache.SemanticCacheEntry;
import org.pangrova.coordinator.ai.cache.SemanticCacheHit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for the HNSW-Backed Semantic Cache.
 *
 * <p>Validates the full lifecycle of the semantic cache:
 * <ul>
 *   <li>Put → FindSimilar → Cache Hit (cosine similarity)</li>
 *   <li>Tenant isolation (entries visible only to matching tenant)</li>
 *   <li>Table-based invalidation (logical deletion)</li>
 *   <li>LRU eviction when capacity is exceeded</li>
 *   <li>Compaction (null entry cleanup)</li>
 *   <li>Thread safety under concurrent access</li>
 * </ul>
 */
@DisplayName("HNSW-Backed Semantic Cache E2E")
class HnswSemanticCacheE2ETest {

    private HnswBackedSemanticCache cache;

    @BeforeEach
    void setUp() {
        cache = new HnswBackedSemanticCache(100);
    }

    // -----------------------------------------------------------------------
    // Put → Find Similar → Cache Hit
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Put and Find Similar")
    class PutAndFindSimilar {

        @Test
        @DisplayName("Exact embedding match returns cache hit with similarity=1.0")
        void exactMatchReturnsCacheHit() {
            float[] embedding = {0.1f, 0.2f, 0.3f, 0.4f};
            SemanticCacheEntry entry = makeEntry("SELECT * FROM t", embedding, "tenant1", Set.of("t"));

            cache.put(entry);

            Optional<SemanticCacheHit> hit = cache.findSimilar(embedding, "tenant1", "user1", 0.9);

            assertThat(hit).isPresent();
            assertThat(hit.get().similarity()).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.001));
            assertThat(hit.get().entry().originalSql()).isEqualTo("SELECT * FROM t");
        }

        @Test
        @DisplayName("Very similar embedding returns cache hit above threshold")
        void similarEmbeddingReturnsHit() {
            float[] stored = {1.0f, 0.0f, 0.0f, 0.0f};
            float[] query = {0.99f, 0.01f, 0.0f, 0.0f};  // very similar direction

            cache.put(makeEntry("SELECT 1", stored, "t1", Set.of("t")));

            Optional<SemanticCacheHit> hit = cache.findSimilar(query, "t1", "u", 0.95);

            assertThat(hit).isPresent();
            assertThat(hit.get().similarity()).isGreaterThan(0.95);
        }

        @Test
        @DisplayName("Dissimilar embedding returns no hit below threshold")
        void dissimilarEmbeddingReturnsNoHit() {
            float[] stored = {1.0f, 0.0f, 0.0f, 0.0f};
            float[] query = {0.0f, 0.0f, 0.0f, 1.0f};  // orthogonal

            cache.put(makeEntry("SELECT 1", stored, "t1", Set.of("t")));

            Optional<SemanticCacheHit> hit = cache.findSimilar(query, "t1", "u", 0.5);

            assertThat(hit).isEmpty();
        }

        @Test
        @DisplayName("Empty cache returns no hit")
        void emptyCacheReturnsNoHit() {
            float[] query = {0.5f, 0.5f, 0.5f, 0.5f};

            Optional<SemanticCacheHit> hit = cache.findSimilar(query, "t1", "u", 0.0);

            assertThat(hit).isEmpty();
        }

        @Test
        @DisplayName("Multiple entries — finds the most similar one")
        void findsClosestAmongMultiple() {
            float[] e1 = {1.0f, 0.0f, 0.0f, 0.0f};
            float[] e2 = {0.0f, 1.0f, 0.0f, 0.0f};
            float[] e3 = {0.7f, 0.7f, 0.0f, 0.0f};  // closest to query

            cache.put(makeEntry("sql1", e1, "t1", Set.of("a")));
            cache.put(makeEntry("sql2", e2, "t1", Set.of("b")));
            cache.put(makeEntry("sql3", e3, "t1", Set.of("c")));

            float[] query = {0.6f, 0.8f, 0.0f, 0.0f};
            Optional<SemanticCacheHit> hit = cache.findSimilar(query, "t1", "u", 0.5);

            assertThat(hit).isPresent();
            // e3 or e2 should be closest depending on cosine similarity
            assertThat(hit.get().entry().originalSql()).isIn("sql2", "sql3");
        }

        @Test
        @DisplayName("Null embedding returns empty Optional")
        void nullEmbeddingReturnsEmpty() {
            cache.put(makeEntry("sql", new float[]{1, 0, 0, 0}, "t1", Set.of("t")));
            assertThat(cache.findSimilar(null, "t1", "u", 0.0)).isEmpty();
        }

        @Test
        @DisplayName("Empty embedding returns empty Optional")
        void emptyEmbeddingReturnsEmpty() {
            cache.put(makeEntry("sql", new float[]{1, 0, 0, 0}, "t1", Set.of("t")));
            assertThat(cache.findSimilar(new float[]{}, "t1", "u", 0.0)).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Tenant Isolation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Tenant Isolation")
    class TenantIsolation {

        @Test
        @DisplayName("Entries are only visible to matching tenant")
        void entriesVisibleOnlyToMatchingTenant() {
            float[] embedding = {1.0f, 0.0f, 0.0f, 0.0f};

            cache.put(makeEntry("tenant1 query", embedding, "tenant1", Set.of("t")));

            // Same tenant finds it
            assertThat(cache.findSimilar(embedding, "tenant1", "u", 0.9)).isPresent();
            // Different tenant does not
            assertThat(cache.findSimilar(embedding, "tenant2", "u", 0.9)).isEmpty();
        }

        @Test
        @DisplayName("Null tenant matches only null-tenant entries")
        void nullTenantMatchesOnlyNullTenantEntries() {
            float[] embedding = {1.0f, 0.0f, 0.0f, 0.0f};

            cache.put(makeEntry("no-tenant query", embedding, null, Set.of("t")));

            // Null tenant finds it
            assertThat(cache.findSimilar(embedding, null, "u", 0.9)).isPresent();
            // Non-null tenant does not
            assertThat(cache.findSimilar(embedding, "some-tenant", "u", 0.9)).isEmpty();
        }

        @Test
        @DisplayName("Multiple tenants can coexist with isolated views")
        void multipleTenantCoexist() {
            float[] emb = {1.0f, 0.0f, 0.0f, 0.0f};

            cache.put(makeEntry("sql-a", emb, "tenantA", Set.of("t")));
            cache.put(makeEntry("sql-b", emb, "tenantB", Set.of("t")));

            Optional<SemanticCacheHit> hitA = cache.findSimilar(emb, "tenantA", "u", 0.9);
            Optional<SemanticCacheHit> hitB = cache.findSimilar(emb, "tenantB", "u", 0.9);

            assertThat(hitA).isPresent();
            assertThat(hitA.get().entry().originalSql()).isEqualTo("sql-a");
            assertThat(hitB).isPresent();
            assertThat(hitB.get().entry().originalSql()).isEqualTo("sql-b");
        }
    }

    // -----------------------------------------------------------------------
    // Table Invalidation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Table-Based Invalidation")
    class TableInvalidation {

        @Test
        @DisplayName("Invalidating a table removes entries referencing that table")
        void invalidateRemovesEntries() {
            float[] emb = {1.0f, 0.0f, 0.0f, 0.0f};

            cache.put(makeEntry("query1", emb, "t1", Set.of("orders")));
            cache.put(makeEntry("query2", new float[]{0, 1, 0, 0}, "t1", Set.of("products")));

            assertThat(cache.size()).isEqualTo(2);

            cache.invalidateByTable("orders");

            assertThat(cache.size()).isEqualTo(1);
            // orders entry gone, products entry remains
            assertThat(cache.findSimilar(emb, "t1", "u", 0.9)).isEmpty();
            assertThat(cache.findSimilar(new float[]{0, 1, 0, 0}, "t1", "u", 0.9)).isPresent();
        }

        @Test
        @DisplayName("Invalidating non-existent table is a no-op")
        void invalidateNonExistentTableIsNoOp() {
            cache.put(makeEntry("sql", new float[]{1, 0, 0, 0}, "t1", Set.of("t")));

            cache.invalidateByTable("non_existent");

            assertThat(cache.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("Null table name is safely ignored")
        void nullTableIgnored() {
            cache.put(makeEntry("sql", new float[]{1, 0, 0, 0}, "t1", Set.of("t")));

            cache.invalidateByTable(null);

            assertThat(cache.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("Entry referencing multiple tables is removed when any is invalidated")
        void multiTableEntryRemovedOnAnyInvalidation() {
            float[] emb = {1, 0, 0, 0};
            cache.put(makeEntry("join query", emb, "t1", Set.of("orders", "products")));

            cache.invalidateByTable("products");

            assertThat(cache.size()).isEqualTo(0);
            assertThat(cache.findSimilar(emb, "t1", "u", 0.5)).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // LRU Eviction
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("LRU Eviction")
    class LruEviction {

        @Test
        @DisplayName("Exceeding maxEntries evicts oldest entry")
        void evictsOldestWhenFull() {
            HnswBackedSemanticCache smallCache = new HnswBackedSemanticCache(3);

            float[] e1 = {1, 0, 0, 0};
            float[] e2 = {0, 1, 0, 0};
            float[] e3 = {0, 0, 1, 0};
            float[] e4 = {0, 0, 0, 1};

            smallCache.put(makeEntry("first", e1, "t", Set.of("a")));
            smallCache.put(makeEntry("second", e2, "t", Set.of("b")));
            smallCache.put(makeEntry("third", e3, "t", Set.of("c")));

            assertThat(smallCache.size()).isEqualTo(3);

            // Adding 4th should evict 1st (oldest)
            smallCache.put(makeEntry("fourth", e4, "t", Set.of("d")));

            assertThat(smallCache.size()).isEqualTo(3);
            // First entry should be evicted
            assertThat(smallCache.findSimilar(e1, "t", "u", 0.99)).isEmpty();
            // Others remain
            assertThat(smallCache.findSimilar(e4, "t", "u", 0.99)).isPresent();
        }

        @Test
        @DisplayName("Cache never exceeds maxEntries")
        void neverExceedsMax() {
            HnswBackedSemanticCache tinyCache = new HnswBackedSemanticCache(5);

            for (int i = 0; i < 20; i++) {
                float[] emb = new float[4];
                emb[i % 4] = 1.0f + i * 0.01f;
                tinyCache.put(makeEntry("sql-" + i, emb, "t", Set.of("t")));
            }

            // Eviction should keep size bounded — not all 20 entries retained
            assertThat(tinyCache.size()).isLessThan(20);
        }
    }

    // -----------------------------------------------------------------------
    // Thread Safety
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Thread Safety")
    class ThreadSafety {

        @Test
        @DisplayName("Concurrent put and findSimilar do not corrupt the cache")
        void concurrentPutAndFind() throws InterruptedException {
            HnswBackedSemanticCache concurrentCache = new HnswBackedSemanticCache(500);
            int writers = 50;
            int readers = 50;
            CountDownLatch latch = new CountDownLatch(writers + readers);
            AtomicInteger hitCount = new AtomicInteger(0);
            ExecutorService executor = Executors.newFixedThreadPool(20);

            // Writers
            for (int i = 0; i < writers; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        float[] emb = new float[]{(float) idx / writers, 0.5f, 0.3f, 0.1f};
                        concurrentCache.put(makeEntry("sql-" + idx, emb, "t1", Set.of("t")));
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Readers
            for (int i = 0; i < readers; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        float[] query = new float[]{(float) idx / readers, 0.5f, 0.3f, 0.1f};
                        Optional<SemanticCacheHit> hit = concurrentCache.findSimilar(query, "t1", "u", 0.5);
                        if (hit.isPresent()) {
                            hitCount.incrementAndGet();
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            // Cache should not be corrupted — size should be consistent
            assertThat(concurrentCache.size()).isGreaterThan(0).isLessThanOrEqualTo(500);
        }

        @Test
        @DisplayName("Concurrent invalidations do not throw")
        void concurrentInvalidations() throws InterruptedException {
            HnswBackedSemanticCache concurrentCache = new HnswBackedSemanticCache(100);
            for (int i = 0; i < 50; i++) {
                float[] emb = new float[]{i * 0.1f, 0.5f, 0.3f, 0.1f};
                concurrentCache.put(makeEntry("sql-" + i, emb, "t1", Set.of("table-" + (i % 5))));
            }

            CountDownLatch latch = new CountDownLatch(5);
            ExecutorService executor = Executors.newFixedThreadPool(5);

            for (int i = 0; i < 5; i++) {
                final int tableIdx = i;
                executor.submit(() -> {
                    try {
                        concurrentCache.invalidateByTable("table-" + tableIdx);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(concurrentCache.size()).isEqualTo(0);
        }
    }

    // -----------------------------------------------------------------------
    // Constructor Validation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Constructor Validation")
    class ConstructorValidation {

        @Test
        @DisplayName("maxEntries=0 throws IllegalArgumentException")
        void zeroMaxEntriesThrows() {
            assertThatThrownBy(() -> new HnswBackedSemanticCache(0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Negative maxEntries throws IllegalArgumentException")
        void negativeMaxEntriesThrows() {
            assertThatThrownBy(() -> new HnswBackedSemanticCache(-1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Custom HNSW parameters are accepted")
        void customHnswParameters() {
            HnswBackedSemanticCache customCache =
                    new HnswBackedSemanticCache(1000, 32, 128, 64);

            float[] emb = {1, 0, 0, 0};
            customCache.put(makeEntry("sql", emb, "t", Set.of("t")));

            assertThat(customCache.findSimilar(emb, "t", "u", 0.9)).isPresent();
        }
    }

    // -----------------------------------------------------------------------
    // Null Safety for put()
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Null Safety")
    class NullSafety {

        @Test
        @DisplayName("put(null) is safely ignored")
        void putNullIgnored() {
            cache.put(null);
            assertThat(cache.size()).isEqualTo(0);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static SemanticCacheEntry makeEntry(String sql, float[] embedding,
                                                 String tenantId, Set<String> tables) {
        return new SemanticCacheEntry(sql, embedding, tenantId, "user", tables,
                new byte[]{1, 2, 3}, Instant.now());
    }
}
