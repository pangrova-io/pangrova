package org.pangrova.testing.integration.e2e.jni;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.exception.NativeRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Verifies that concurrent create/close of NativeTaskExecutor handles
 * does not cause native memory leaks, SIGSEGV, or deadlocks.
 *
 * <p>This test exercises the JNI boundary under race conditions:
 * rapid create→close cycles, concurrent close on same handle,
 * and interleaved cancel+close sequences.
 *
 * <p>Requirements: E2E-06, §5.7 (NativeTaskExecutorCloseRaceE2ETest)
 */
@DisplayName("JNI Chain - NativeTaskExecutor Close Race Safety")
class NativeTaskExecutorCloseRaceE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_race_" + method + "_" + System.nanoTime();
    }

    @Nested
    @DisplayName("Rapid create-close cycles")
    class RapidCreateCloseCycles {

        @Test
        @DisplayName("50 sequential create-close cycles complete without leak")
        void sequentialCreateClose_noLeak() {
            assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
                for (int i = 0; i < 50; i++) {
                    long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
                    NativeRuntime.nativeReleaseExecutor(handle);
                }
            }, "50 sequential create-close cycles must complete within 60s");

            // Verify cluster still healthy after rapid lifecycle churn
            verifyClusterHealthy();
        }

        @Test
        @DisplayName("Concurrent create-close from 8 threads, no SIGSEGV")
        void concurrentCreateClose_noSigsegv() throws Exception {
            int threadCount = 8;
            int cyclesPerThread = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(executor.submit(() -> {
                    awaitLatch(startLatch);
                    for (int i = 0; i < cyclesPerThread; i++) {
                        try {
                            long handle = NativeRuntime.nativeCreateTaskExecutor(
                                    CanonicalNativeTask.valuesRequest());
                            NativeRuntime.nativeReleaseExecutor(handle);
                        } catch (Throwable e) {
                            errors.add(e);
                        }
                    }
                }));
            }

            startLatch.countDown();
            awaitAllFutures(futures, executor);

            assertThat(errors)
                    .as("All concurrent create-close cycles should complete without errors")
                    .isEmpty();

            verifyClusterHealthy();
        }
    }

    @Nested
    @DisplayName("Concurrent close on same handle")
    class ConcurrentCloseOnSameHandle {

        @Test
        @DisplayName("Two threads closing same handle: exactly one succeeds, one gets exception")
        void doubleClose_oneSucceedsOneThrows() throws Exception {
            long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());

            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger exceptionCount = new AtomicInteger(0);
            CopyOnWriteArrayList<Throwable> unexpectedErrors = new CopyOnWriteArrayList<>();

            ExecutorService executor = Executors.newFixedThreadPool(2);
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < 2; i++) {
                futures.add(executor.submit(() -> {
                    awaitLatch(startLatch);
                    try {
                        NativeRuntime.nativeReleaseExecutor(handle);
                        successCount.incrementAndGet();
                    } catch (NativeRuntimeException e) {
                        exceptionCount.incrementAndGet();
                    } catch (Throwable e) {
                        unexpectedErrors.add(e);
                    }
                }));
            }

            startLatch.countDown();
            awaitAllFutures(futures, executor);

            assertThat(unexpectedErrors)
                    .as("No unexpected errors (SIGSEGV, NPE, etc.)")
                    .isEmpty();
            assertThat(successCount.get() + exceptionCount.get())
                    .as("Both threads must complete (success or NativeRuntimeException)")
                    .isEqualTo(2);
            assertThat(successCount.get())
                    .as("Exactly one thread should succeed in releasing")
                    .isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Cancel + close interleaving")
    class CancelCloseInterleaving {

        @Test
        @DisplayName("Cancel immediately followed by close completes without deadlock")
        void cancelThenClose_noDeadlock() {
            assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
                for (int i = 0; i < 20; i++) {
                    long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
                    NativeRuntime.nativeCancelTask(handle);
                    NativeRuntime.nativeReleaseExecutor(handle);
                }
            }, "20 cancel-then-close cycles must complete within 30s");

            verifyClusterHealthy();
        }

        @Test
        @DisplayName("Concurrent cancel and close on same handle: no SIGSEGV")
        void concurrentCancelAndClose_noSigsegv() throws Exception {
            int iterations = 20;
            CopyOnWriteArrayList<Throwable> unexpectedErrors = new CopyOnWriteArrayList<>();

            for (int i = 0; i < iterations; i++) {
                long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
                CountDownLatch startLatch = new CountDownLatch(1);
                ExecutorService executor = Executors.newFixedThreadPool(2);
                List<Future<?>> futures = new ArrayList<>();

                futures.add(executor.submit(() -> {
                    awaitLatch(startLatch);
                    try {
                        NativeRuntime.nativeCancelTask(handle);
                    } catch (NativeRuntimeException ignored) {
                        // Expected if handle already released
                    } catch (Throwable e) {
                        unexpectedErrors.add(e);
                    }
                }));

                futures.add(executor.submit(() -> {
                    awaitLatch(startLatch);
                    try {
                        NativeRuntime.nativeReleaseExecutor(handle);
                    } catch (NativeRuntimeException ignored) {
                        // Expected if handle already released
                    } catch (Throwable e) {
                        unexpectedErrors.add(e);
                    }
                }));

                startLatch.countDown();
                awaitAllFutures(futures, executor);
            }

            assertThat(unexpectedErrors)
                    .as("No unexpected errors during concurrent cancel+close")
                    .isEmpty();

            verifyClusterHealthy();
        }
    }

    @Nested
    @DisplayName("Post-churn cluster health")
    class PostChurnClusterHealth {

        @Test
        @DisplayName("Query succeeds after heavy JNI lifecycle churn")
        void queryAfterHeavyChurn_succeeds() {
            // Heavy churn phase
            for (int i = 0; i < 30; i++) {
                try {
                    long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
                    NativeRuntime.nativeReleaseExecutor(handle);
                } catch (NativeRuntimeException ignored) {
                    // Tolerate individual failures during churn
                }
            }

            // Verify real query still works after churn
            String fqn = FQN_PREFIX + tableName;
            sqlRunner.executeDdl(
                    "CREATE TABLE " + fqn + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute(
                    "INSERT INTO " + fqn + " (id, name) VALUES "
                            + "(1, 'post_churn_1'), (2, 'post_churn_2')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("post_churn_1");
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("post_churn_2");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void verifyClusterHealthy() {
        String fqn = FQN_PREFIX + "t_health_" + System.nanoTime();
        sqlRunner.executeDdl(
                "CREATE TABLE " + fqn + " (id BIGINT)");
        sqlRunner.execute(
                "INSERT INTO " + fqn + " (id) VALUES (1)");
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + fqn);
        assertThat(rows).isNotEmpty();
        assertThat(((Number) rows.get(0).getRaw("cnt")).longValue()).isEqualTo(1L);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitAllFutures(List<Future<?>> futures,
                                        ExecutorService executor) throws Exception {
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
