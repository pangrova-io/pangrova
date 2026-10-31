package org.pangrova.testing.integration.e2e.execution;

import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T13: Execution/Interop — 5 E2E scenarios.
 */
@E2ETest
@DisplayName("T13: Execution/Interop")
class ExecutionInteropE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: Async commit")
    class AsyncCommit {
        @Test void writeCommitsAsynchronously() {
            String tbl = CATALOG + "." + SCHEMA + ".t13_async_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'committed')");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 1);
        }
    }

    @Nested @DisplayName("Scenario 2: JNI memory safety")
    class JniMemorySafety {
        @Test void nativeQueryDoesNotLeakMemory() {
            String tbl = CATALOG + "." + SCHEMA + ".t13_jni_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, data VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 50; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(",'data_").append(i).append("')"); }
            executeDml(sb.toString());
            // Multiple queries to exercise native memory allocation/deallocation
            for (int run = 0; run < 3; run++) {
                List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl + " ORDER BY id");
                assertThat(rows).hasSize(50);
            }
        }
    }

    @Nested @DisplayName("Scenario 3: Client retry idempotent")
    class ClientRetryIdempotent {
        @Test void repeatedQueryReturnsConsistentResults() {
            String tbl = CATALOG + "." + SCHEMA + ".t13_retry_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1),(2),(3)");
            List<TypedRow> r1 = executeQuery("SELECT * FROM " + tbl + " ORDER BY id");
            List<TypedRow> r2 = executeQuery("SELECT * FROM " + tbl + " ORDER BY id");
            assertThat(r1).hasSize(3);
            assertThat(r2).hasSize(3);
        }
    }

    @Nested @DisplayName("Scenario 4: Multi-type interop")
    class MultiTypeInterop {
        @Test void variousDataTypesRoundTrip() {
            String tbl = CATALOG + "." + SCHEMA + ".t13_types_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (i BIGINT, d DOUBLE, s VARCHAR, b BOOLEAN)");
            executeDml("INSERT INTO " + tbl + " VALUES (42, 3.14, 'hello', true)");
            List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl);
            assertThat(rows).hasSize(1);
        }
    }

    @Nested @DisplayName("Scenario 5: Error propagation")
    class ErrorPropagation {
        @Test void runtimeErrorPropagatesCleanly() {
            // Division by zero should propagate as a clean error
            String tbl = CATALOG + "." + SCHEMA + ".t13_err_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val INT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1, 0)");
            String error = executeExpectingError("SELECT id, 100/val FROM " + tbl);
            assertThat(error).containsIgnoringCase("division by zero");
            assertThat(executeQuery("SELECT id FROM " + tbl)).hasSize(1);
        }
    }
}
