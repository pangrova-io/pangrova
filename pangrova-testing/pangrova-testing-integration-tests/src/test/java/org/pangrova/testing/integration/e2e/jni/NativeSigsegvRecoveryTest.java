package org.pangrova.testing.integration.e2e.jni;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;
import org.pangrova.worker.nativebridge.jni.NativeRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that use-after-free on a released native handle throws
 * {@link NativeRuntimeException} instead of crashing the JVM with SIGSEGV,
 * and that the native runtime remains functional after the error.
 *
 * <p>The HandleRegistry (Phase 1) catches stale handles and throws
 * {@code std::runtime_error}, which the JNI_CATCH_AND_THROW macro converts
 * to {@link NativeRuntimeException}. The SIGSEGV signal handler (Phase 4)
 * is a secondary safety net for cases that bypass HandleRegistry.
 *
 * <p>Test ordering is intentional: the use-after-free test runs first to
 * prove the JVM survives, then the recovery test verifies the native
 * runtime can be re-initialized and used for real queries.
 *
 * <p>Requirements: REQ-3, REQ-4, NFR-3
 */
@DisplayName("JNI Chain - SIGSEGV Recovery via HandleRegistry")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NativeSigsegvRecoveryTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Test
    @Order(1)
    @DisplayName("Use-after-free on released handle throws NativeRuntimeException, not SIGSEGV")
    void useAfterFree_throwsNativeRuntimeException() {
        // 1. Create a task executor handle
        long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
        assertThat(handle).as("Handle must be non-zero").isNotZero();

        // 2. Release the executor — handle becomes stale
        NativeRuntime.nativeReleaseExecutor(handle);

        // 3. Call nativeGetTaskStatus on the released handle — HandleRegistry
        //    detects the stale handle and throws std::runtime_error, which
        //    JNI_CATCH_AND_THROW converts to NativeRuntimeException.
        assertThatThrownBy(() -> NativeRuntime.nativeGetTaskStatus(handle))
                .isInstanceOf(NativeRuntimeException.class)
                .as("Stale handle must throw NativeRuntimeException, not crash the JVM");

        // 4. JVM is still alive — this assertion proves no SIGSEGV occurred
        assertThat(NativeRuntime.isInitialized())
                .as("Native runtime must remain initialized after use-after-free")
                .isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("Recovery: native runtime remains functional after use-after-free")
    void recovery_runtimeStillFunctionalAfterUseAfterFree() {
        // After the use-after-free in Order(1), the native runtime must
        // still be functional — no shutdown/re-init needed.
        // Velox global singletons (arbitrator factory, filesystem factories)
        // cannot be re-registered after shutdown in the same JVM process,
        // so we verify recovery without a restart cycle.
        assertThat(NativeRuntime.isInitialized())
                .as("Runtime must still be initialized after use-after-free")
                .isTrue();

        // 1. Create a new task executor — proves HandleRegistry works
        long handle = NativeRuntime.nativeCreateTaskExecutor(CanonicalNativeTask.valuesRequest());
        assertThat(handle)
                .as("New handle must be non-zero")
                .isNotZero();

        // 2. Clean up
        NativeRuntime.nativeReleaseExecutor(handle);

        // 3. Verify real query execution works after recovery
        String tableName = "t_recovery_" + System.nanoTime();
        String fqn = FQN_PREFIX + tableName;
        sqlRunner.executeDdl(
                "CREATE TABLE " + fqn + " (id BIGINT, name VARCHAR)");
        sqlRunner.execute(
                "INSERT INTO " + fqn + " (id, name) VALUES (1, 'recovered')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, name FROM " + fqn);

        assertThat(rows).hasSize(1);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("recovered");
    }
}
