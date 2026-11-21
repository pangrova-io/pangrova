package org.pangrova.worker.property.buffer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.worker.buffer.OutputBufferManager;
import org.pangrova.worker.buffer.QueryOutputBufferRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Buffer-registry removal isolation. Task retention is tested against TaskCleanupScheduler.
 *
 * <p>Verifies that expiring query A's result buffer does not affect
 * query B's active result stream. Removing a registry entry must close
 * only the buffer bound to the expired queryId via the
 * {@link QueryOutputBufferRegistry}, never the current global buffer.</p>
 *
 * <p><b>Validates: Requirements A1.4</b></p>
 */
class QueryOutputBufferIsolationPropertyTest {

    /**
     * For any pair of concurrent queries (A expires, B is active),
     * expiring A closes only A's buffer; B's buffer remains open
     * and its data is fully readable.
     *
     * <p><b>Validates: Requirements A1.4</b></p>
     */
    @Property(tries = 50)
    @Tag("Feature: full-pipeline-fix, Property: Result expiration isolation")
    void expirationOfQueryADoesNotAffectQueryB(
            @ForAll("batchCounts") int batchesForA,
            @ForAll("batchCounts") int batchesForB) throws Exception {

        QueryOutputBufferRegistry registry = new QueryOutputBufferRegistry();

        try {
            String queryA = "expire-query-A";
            String queryB = "active-query-B";

            OutputBufferManager bufferA = new OutputBufferManager();
            OutputBufferManager bufferB = new OutputBufferManager();
            registry.register(queryA, bufferA);
            registry.register(queryB, bufferB);

            // Write data to both buffers
            for (int i = 0; i < batchesForA; i++) {
                bufferA.addBatch(taggedBatch(queryA, i));
            }
            for (int i = 0; i < batchesForB; i++) {
                bufferB.addBatch(taggedBatch(queryB, i));
            }

            assertThat(registry.removeAndClose(queryA)).isSameAs(bufferA);
            assertThat(registry.removeAndClose(queryA)).isNull();

            // Verify: A's buffer is closed and removed
            assertThat(bufferA.isClosed()).isTrue();
            assertThat(registry.contains(queryA)).isFalse();

            // Verify: B's buffer is still open and data is intact
            assertThat(bufferB.isClosed()).isFalse();
            assertThat(registry.contains(queryB)).isTrue();

            List<byte[]> bResults = drainBuffer(bufferB, batchesForB);
            assertThat(bResults).hasSize(batchesForB);
            for (int i = 0; i < batchesForB; i++) {
                assertThat(bResults.get(i)).isEqualTo(taggedBatch(queryB, i));
            }
        } finally {
            registry.removeAndClose("expire-query-A");
            registry.removeAndClose("active-query-B");
        }
    }

    /**
     * For any N queries (2-6), expiring a subset does not affect
     * the remaining queries' buffers. Each surviving query's buffer
     * remains open with all data intact.
     *
     * <p><b>Validates: Requirements A1.4</b></p>
     */
    @Property(tries = 30)
    @Tag("Feature: full-pipeline-fix, Property: Result expiration isolation")
    void expiringSubsetPreservesRemainingBuffers(
            @ForAll("queryCounts") int totalQueries,
            @ForAll("batchCounts") int batchesPerQuery) throws Exception {

        QueryOutputBufferRegistry registry = new QueryOutputBufferRegistry();

        try {
            List<String> queryIds = new ArrayList<>();
            List<OutputBufferManager> buffers = new ArrayList<>();

            for (int q = 0; q < totalQueries; q++) {
                String queryId = "query-" + q;
                queryIds.add(queryId);
                OutputBufferManager buffer = new OutputBufferManager();
                buffers.add(buffer);
                registry.register(queryId, buffer);
                for (int b = 0; b < batchesPerQuery; b++) {
                    buffer.addBatch(taggedBatch(queryId, b));
                }
            }

            // Expire the first half
            int expireCount = totalQueries / 2;
            for (int q = 0; q < expireCount; q++) {
                registry.removeAndClose(queryIds.get(q));
            }

            // Verify expired queries
            for (int q = 0; q < expireCount; q++) {
                assertThat(buffers.get(q).isClosed()).isTrue();
                assertThat(registry.contains(queryIds.get(q))).isFalse();
            }

            // Verify surviving queries are unaffected
            for (int q = expireCount; q < totalQueries; q++) {
                OutputBufferManager surviving = buffers.get(q);
                assertThat(surviving.isClosed()).isFalse();
                assertThat(registry.contains(queryIds.get(q))).isTrue();

                List<byte[]> data = drainBuffer(surviving, batchesPerQuery);
                assertThat(data).hasSize(batchesPerQuery);
                for (int b = 0; b < batchesPerQuery; b++) {
                    assertThat(data.get(b)).isEqualTo(taggedBatch(queryIds.get(q), b));
                }
            }
        } finally {
            for (int q = 0; q < totalQueries; q++) registry.removeAndClose("query-" + q);
        }
    }

    // --- Generators ---

    @Provide
    Arbitrary<Integer> queryCounts() {
        return Arbitraries.integers().between(2, 6);
    }

    @Provide
    Arbitrary<Integer> batchCounts() {
        return Arbitraries.integers().between(1, 10);
    }

    // --- Helpers ---

    private static byte[] taggedBatch(String queryId, int batchIndex) {
        String tag = queryId + ":" + batchIndex;
        return tag.getBytes(StandardCharsets.UTF_8);
    }

    private static List<byte[]> drainBuffer(OutputBufferManager buffer, int expectedCount)
            throws InterruptedException {
        List<byte[]> results = new ArrayList<>();
        for (int i = 0; i < expectedCount; i++) {
            byte[] batch = buffer.pollBatch(1000);
            if (batch == null) {
                break;
            }
            results.add(batch);
        }
        return results;
    }
}
