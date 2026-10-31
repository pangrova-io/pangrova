package org.pangrova.worker.property.buffer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.worker.buffer.OutputBufferManager;
import org.pangrova.worker.buffer.QueryOutputBufferRegistry;
import org.pangrova.worker.buffer.ResultExpirationScheduler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for result expiration isolation (P0-FPV-6).
 *
 * <p>Verifies that expiring query A's result buffer does not affect
 * query B's active result stream. The expiration callback must close
 * only the buffer bound to the expired queryId via the
 * {@link QueryOutputBufferRegistry}, never the current global buffer.</p>
 *
 * <p><b>Validates: Requirements A1.4</b></p>
 */
class ResultExpirationIsolationPropertyTest {

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
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

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

            // Wire expiration callback using registry (production path)
            CountDownLatch expirationDone = new CountDownLatch(1);
            ResultExpirationScheduler expirationScheduler = new ResultExpirationScheduler(
                    scheduler, Duration.ofMillis(10), queryId -> {
                registry.removeAndClose(queryId);
                expirationDone.countDown();
            });

            // Expire query A
            expirationScheduler.scheduleExpiration(queryA);
            assertThat(expirationDone.await(5, TimeUnit.SECONDS)).isTrue();

            // Verify: A's buffer is closed and removed
            assertThat(bufferA.isClosed()).isTrue();
            assertThat(registry.contains(queryA)).isFalse();
            assertThat(expirationScheduler.isExpired(queryA)).isTrue();

            // Verify: B's buffer is still open and data is intact
            assertThat(bufferB.isClosed()).isFalse();
            assertThat(registry.contains(queryB)).isTrue();
            assertThat(expirationScheduler.isExpired(queryB)).isFalse();

            List<byte[]> bResults = drainBuffer(bufferB, batchesForB);
            assertThat(bResults).hasSize(batchesForB);
            for (int i = 0; i < batchesForB; i++) {
                assertThat(bResults.get(i)).isEqualTo(taggedBatch(queryB, i));
            }
        } finally {
            scheduler.shutdownNow();
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
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

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
            CountDownLatch expirationsDone = new CountDownLatch(expireCount);
            ResultExpirationScheduler expirationScheduler = new ResultExpirationScheduler(
                    scheduler, Duration.ofMillis(10), queryId -> {
                registry.removeAndClose(queryId);
                expirationsDone.countDown();
            });

            for (int q = 0; q < expireCount; q++) {
                expirationScheduler.scheduleExpiration(queryIds.get(q));
            }
            assertThat(expirationsDone.await(5, TimeUnit.SECONDS)).isTrue();

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
            scheduler.shutdownNow();
        }
    }

    /**
     * Cancelling expiration before it fires preserves the buffer.
     *
     * <p><b>Validates: Requirements A1.4</b></p>
     */
    @Property(tries = 30)
    @Tag("Feature: full-pipeline-fix, Property: Result expiration isolation")
    void cancelledExpirationPreservesBuffer(
            @ForAll("batchCounts") int batchCount) throws Exception {

        QueryOutputBufferRegistry registry = new QueryOutputBufferRegistry();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            String queryId = "cancel-test-query";
            OutputBufferManager buffer = new OutputBufferManager();
            registry.register(queryId, buffer);
            for (int i = 0; i < batchCount; i++) {
                buffer.addBatch(taggedBatch(queryId, i));
            }

            // Schedule with long retention, then cancel immediately
            ResultExpirationScheduler expirationScheduler = new ResultExpirationScheduler(
                    scheduler, Duration.ofSeconds(60), qid -> registry.removeAndClose(qid));

            expirationScheduler.scheduleExpiration(queryId);
            expirationScheduler.cancelExpiration(queryId);

            // Wait a bit to confirm expiration did not fire
            Thread.sleep(50);

            assertThat(buffer.isClosed()).isFalse();
            assertThat(registry.contains(queryId)).isTrue();
            assertThat(expirationScheduler.isExpired(queryId)).isFalse();

            List<byte[]> data = drainBuffer(buffer, batchCount);
            assertThat(data).hasSize(batchCount);
        } finally {
            scheduler.shutdownNow();
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
