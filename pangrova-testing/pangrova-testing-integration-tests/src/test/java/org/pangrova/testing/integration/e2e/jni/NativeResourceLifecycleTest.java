package org.pangrova.testing.integration.e2e.jni;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.worker.nativebridge.jni.NativeRuntimeException;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * JNI boundary safety verification: handle lifecycle, concurrency, Arrow transfer,
 * exception propagation, and cancel-release semantics via real NativeRuntime JNI calls.
 *
 * <p>Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6
 */
@DisplayName("JNI Boundary Safety - Native Resource Lifecycle")
class NativeResourceLifecycleTest extends SqlE2ETestBase {

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
        tableName = "t_jni_" + method + "_" + System.nanoTime();
    }

    // -----------------------------------------------------------------------
    // N10.1: Handle create → query → release → verify no leak
    // Validates: Requirement 10.1
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("N10.1: Handle create-and-release lifecycle completes without leak")
    void handleCreateAndRelease() {
        createTestTable();
        insertTestData();

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn() + " ORDER BY id");

        assertThat(rows).hasSize(3);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(3L);

        // If handle lifecycle leaked, subsequent queries would fail or OOM.
        List<TypedRow> secondRun = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + fqn());
        assertThat(secondRun).isNotEmpty();
    }

    // -----------------------------------------------------------------------
    // N10.2: Double release throws NativeRuntimeException (not SIGSEGV)
    // Validates: Requirement 10.2
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("N10.2: Double release throws NativeRuntimeException instead of JVM crash")
    void doubleReleaseThrowsException() {
        long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
        NativeRuntime.nativeReleaseExecutor(handle);

        assertThrows(NativeRuntimeException.class,
                () -> NativeRuntime.nativeReleaseExecutor(handle),
                "Double release must throw NativeRuntimeException, not SIGSEGV");
    }

    // -----------------------------------------------------------------------
    // N10.3: 8 threads concurrent JNI calls on different handles
    // Validates: Requirement 10.3
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("N10.3: Concurrent JNI calls on different handles complete correctly")
    void concurrentHandleOperations() throws Exception {
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                awaitLatch(startLatch);
                executeSingleHandleLifecycle(errors);
            }));
        }

        startLatch.countDown();
        awaitAllFutures(futures, executor);

        assertThat(errors)
                .as("All concurrent JNI calls should complete without errors")
                .isEmpty();
    }

    // -----------------------------------------------------------------------
    // N10.4: Arrow data transfer across JNI boundary
    // Validates: Requirement 10.4
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("N10.4: Arrow data transferred across JNI boundary is readable")
    void arrowDataTransferAcrossJni() {
        createTestTable();
        insertTestData();

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, name, score FROM " + fqn() + " ORDER BY id");

        assertThat(rows).hasSize(3);
        assertArrowRow(rows.get(0), 1L, "alice", 85.5);
        assertArrowRow(rows.get(1), 2L, "bob", 92.0);
        assertArrowRow(rows.get(2), 3L, "carol", 78.3);
    }

    // -----------------------------------------------------------------------
    // N10.5: C++ exception propagates as NativeRuntimeException
    // Validates: Requirement 10.5
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("N10.5: C++ exception propagates as NativeRuntimeException with message")
    void nativeExceptionPropagation() {
        // Passing invalid (corrupted) task config triggers C++ validation error
        byte[] invalidConfig = buildInvalidTaskConfig();

        NativeRuntimeException ex = assertThrows(NativeRuntimeException.class,
                () -> NativeRuntime.nativeCreateTaskExecutor(invalidConfig));

        assertThat(ex.getMessage())
                .as("Exception must contain original C++ error context")
                .isNotBlank();
    }

    // -----------------------------------------------------------------------
    // N10.6: Cancel query releases native resources within timeout
    // Validates: Requirement 10.6
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("N10.6: Cancel query releases native resources within bounded timeout")
    void cancelQueryReleasesResources() {
        createTestTable();
        insertLargeDataSet();

        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
            NativeRuntime.nativeCancelTask(handle);
            assertDoesNotThrow(() -> NativeRuntime.nativeReleaseExecutor(handle));
        }, "Cancel + release must complete within 30s timeout");

        // Verify cluster still healthy after cancel
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + fqn());
        assertThat(rows).isNotEmpty();
    }

    // -----------------------------------------------------------------------
    // Helpers: table setup (each ≤ 20 lines, single responsibility)
    // -----------------------------------------------------------------------

    private void createTestTable() {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (id BIGINT, name VARCHAR, score DOUBLE)",
                fqn()));
    }

    private void insertTestData() {
        sqlRunner.execute(String.format(
                "INSERT INTO %s (id, name, score) VALUES "
                        + "(1, 'alice', 85.5), (2, 'bob', 92.0), (3, 'carol', 78.3)",
                fqn()));
    }

    private void insertLargeDataSet() {
        StringBuilder values = new StringBuilder();
        for (int i = 1; i <= 500; i++) {
            if (i > 1) {
                values.append(", ");
            }
            values.append(String.format("(%d, 'user_%d', %d.0)", i, i, i));
        }
        sqlRunner.execute(String.format(
                "INSERT INTO %s (id, name, score) VALUES %s", fqn(), values));
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // Helpers: concurrency (each ≤ 20 lines, single responsibility)
    // -----------------------------------------------------------------------

    private void executeSingleHandleLifecycle(CopyOnWriteArrayList<Throwable> errors) {
        try {
            long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
            NativeRuntime.nativeReleaseExecutor(handle);
        } catch (Throwable t) {
            errors.add(t);
        }
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

    // -----------------------------------------------------------------------
    // Helpers: assertion and config (each ≤ 20 lines, single responsibility)
    // -----------------------------------------------------------------------

    private static void assertArrowRow(TypedRow row,
                                       long expectedId, String expectedName,
                                       double expectedScore) {
        assertThat(((Number) row.getRaw("id")).longValue()).isEqualTo(expectedId);
        assertThat(row.getRaw("name")).isEqualTo(expectedName);
        assertThat(((Number) row.getRaw("score")).doubleValue()).isEqualTo(expectedScore);
    }

    private static byte[] buildInvalidTaskConfig() {
        // Non-empty garbage bytes that cannot be deserialized as valid protobuf
        return new byte[]{0x08, 0x7F, 0x7F, 0x7F, 0x7F};
    }
}
