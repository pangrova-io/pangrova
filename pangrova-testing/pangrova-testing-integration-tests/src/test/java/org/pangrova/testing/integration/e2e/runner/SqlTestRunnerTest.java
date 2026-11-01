package org.pangrova.testing.integration.e2e.runner;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.testkit.TestCluster;
import org.pangrova.testing.integration.e2e.E2ETestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlTestRunnerTest {

    private SqlTestRunner runner;

    @BeforeEach
    void setUp() {
        TestCluster cluster = new TestCluster(() -> () -> {}, () -> () -> {});
        E2ETestFramework framework = new E2ETestFramework(
            cluster,
            sql -> {
                if (sql.contains("ERROR")) {
                    throw new RuntimeException("Syntax error near 'ERROR'");
                }
                if (sql.equals("SELECT 1")) {
                    return List.of(TypedRow.fromRawMap(Map.of("1", 1)));
                }
                return List.of();
            }
        );
        runner = new SqlTestRunner(framework);
    }

    @Test
    void executeReturnsRowsOnSuccess() {
        List<TypedRow> rows = runner.execute("SELECT 1");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).toRawMap()).containsEntry("1", 1);
    }

    @Test
    void executeThrowsSqlExecutionExceptionOnFailure() {
        assertThatThrownBy(() -> runner.execute("SELECT ERROR"))
            .isInstanceOf(SqlExecutionException.class)
            .hasMessageContaining("ERROR")
            .hasMessageContaining("Syntax error");
    }

    @Test
    void sqlExecutionExceptionCarriesContext() {
        try {
            runner.execute("SELECT ERROR");
        } catch (SqlExecutionException e) {
            assertThat(e.sql()).isEqualTo("SELECT ERROR");
            assertThat(e.errorMessage()).contains("Syntax error");
            return;
        }
        throw new AssertionError("Expected SqlExecutionException");
    }

    @Test
    void executeExpectingErrorReturnsQueryError() {
        QueryError error = runner.executeExpectingError("SELECT ERROR");
        assertThat(error.errorType()).isEqualTo("SYNTAX");
        assertThat(error.message()).contains("Syntax error");
    }

    @Test
    void executeExpectingErrorThrowsOnSuccess() {
        assertThatThrownBy(() -> runner.executeExpectingError("SELECT 1"))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("Expected error but query succeeded");
    }

    @Test
    void executeDdlDelegatesToExecute() {
        // DDL on empty result should not throw
        runner.executeDdl("CREATE TABLE test (id BIGINT)");
    }

    @Test
    void executeDdlThrowsOnFailure() {
        assertThatThrownBy(() -> runner.executeDdl("CREATE ERROR TABLE"))
            .isInstanceOf(SqlExecutionException.class);
    }
}
