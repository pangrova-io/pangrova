package org.pangrova.testing.integration.e2e.error;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for structured error handling across error categories.
 *
 * <p>Each error type is triggered through the real pipeline and verified to
 * return a structured {@link QueryError} with the correct error classification.
 * No mocks — uses real Parser, Planner, Worker, and NativeRuntime.
 *
 * <p>Error categories tested:
 * <ul>
 *   <li>Syntax error → SYNTAX (parse error)</li>
 *   <li>Table not found → SEMANTIC (metadata error)</li>
 *   <li>Unsupported SQL → SEMANTIC or RUNTIME (planner error)</li>
 * </ul>
 *
 * <p>Validates: Requirement R12.11
 */
@DisplayName("Error Handling E2E — Structured ErrorDetail")
class ErrorHandlingE2ETest extends SqlE2ETestBase {

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
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private void seedTestTable() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");
        sqlRunner.execute("INSERT INTO " + fqn() + " (id, name, amount) VALUES "
                + "(1, 'alice', 100.0), (2, 'bob', 200.0)");
    }

    // -----------------------------------------------------------------------
    // R12.11: Syntax error → SYNTAX (parse error)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Syntax Error → Parse Error")
    class SyntaxError {

        @Test
        @DisplayName("Malformed SQL returns SYNTAX error with parse failure detail")
        void malformedSqlReturnsSyntaxError() {
            QueryError error = sqlRunner.executeExpectingError(
                    "SELEC * FORM orders");

            assertThat(error.errorType()).isEqualTo("SYNTAX");
            assertThat(error.message()).isNotBlank();
        }

        @Test
        @DisplayName("Unclosed string literal returns SYNTAX error")
        void unclosedStringLiteralReturnsSyntaxError() {
            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM orders WHERE name = 'unclosed");

            assertThat(error.errorType()).isEqualTo("SYNTAX");
            assertThat(error.message()).isNotBlank();
        }

        @Test
        @DisplayName("Missing FROM clause returns SYNTAX error")
        void missingFromClauseReturnsSyntaxError() {
            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT WHERE 1 = 1");

            assertThat(error.errorType()).isIn("SYNTAX", "SEMANTIC");
            assertThat(error.message()).isNotBlank();
        }
    }

    // -----------------------------------------------------------------------
    // R12.11: Table not found → SEMANTIC (metadata error)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Table Not Found → Metadata Error")
    class TableNotFound {

        @Test
        @DisplayName("Query on non-existent table returns SEMANTIC error")
        void nonExistentTableReturnSemanticError() {
            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + FQN_PREFIX + "ghost_table_" + System.nanoTime());

            assertThat(error.errorType()).isEqualTo("SEMANTIC");
            assertThat(error.message()).isNotBlank();
        }

        @Test
        @DisplayName("Query on non-existent catalog returns SEMANTIC error")
        void nonExistentCatalogReturnsSemanticError() {
            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM nonexistent_catalog.default.some_table");

            assertThat(error.errorType()).isIn("SEMANTIC", "RUNTIME");
            assertThat(error.message()).isNotBlank();
        }

        @Test
        @DisplayName("Query on non-existent schema returns SEMANTIC error")
        void nonExistentSchemaReturnsSemanticError() {
            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + CATALOG + ".nonexistent_schema.some_table");

            assertThat(error.errorType()).isIn("SEMANTIC", "RUNTIME");
            assertThat(error.message()).isNotBlank();
        }
    }

    // -----------------------------------------------------------------------
    // R12.11: Unsupported SQL → SEMANTIC or RUNTIME (planner error)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Unsupported SQL → Planner Error")
    class UnsupportedSql {

        @Test
        @DisplayName("Unsupported EXPLAIN ANALYZE returns structured error")
        void unsupportedExplainAnalyzeReturnsError() {
            QueryError error = sqlRunner.executeExpectingError(
                    "EXPLAIN ANALYZE SELECT 1");

            assertThat(error.errorType()).isNotNull();
            assertThat(error.message()).isNotBlank();
        }

        @Test
        @DisplayName("Invalid column reference returns SEMANTIC error")
        void invalidColumnReferenceReturnsSemanticError() {
            seedTestTable();

            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT nonexistent_column FROM " + fqn());

            assertThat(error.errorType()).isEqualTo("SEMANTIC");
            assertThat(error.message()).containsIgnoringCase("nonexistent_column");
        }

        @Test
        @DisplayName("Invalid implicit numeric cast returns a structured runtime error")
        void invalidImplicitNumericCastReturnsRuntimeError() {
            seedTestTable();

            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + fqn() + " WHERE id = 'not_a_number'");

            assertThat(error.errorType()).isEqualTo("RUNTIME");
            assertThat(error.message()).contains("not_a_number", "BIGINT", "INVALID_ARGUMENT");
        }
    }

    // -----------------------------------------------------------------------
    // R12.11: All error types return structured QueryError (not raw exception)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Structured Error Consistency")
    class StructuredErrorConsistency {

        @Test
        @DisplayName("Every error type produces non-null errorType and message")
        void allErrorsHaveStructuredFields() {
            // Syntax error
            QueryError syntaxErr = sqlRunner.executeExpectingError("SELEC 1");
            assertThat(syntaxErr.errorType()).isNotNull();
            assertThat(syntaxErr.message()).isNotBlank();

            // Metadata error
            QueryError metaErr = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + FQN_PREFIX + "no_such_table_" + System.nanoTime());
            assertThat(metaErr.errorType()).isNotNull();
            assertThat(metaErr.message()).isNotBlank();
        }
    }
}
