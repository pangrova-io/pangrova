package org.pangrova.testing.integration.e2e.index.interaction;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying indexes handle special characters correctly.
 *
 * <p>Special characters (punctuation, mixed case, whitespace) must be indexed
 * and queried without corruption or incorrect filtering.</p>
 *
 * <p>Note: Non-ASCII Unicode literals (CJK, emoji) are excluded because Calcite
 * converts them to {@code u&'\xxxx'} NlsString escapes that the Substrait
 * converter does not yet support (PLANNER-0002). This test focuses on ASCII
 * special-character edge cases instead.</p>
 *
 * <p>Validates: Requirements R19.1, R19.2, R19.3</p>
 */
@DisplayName("Index Special Character E2E — special character handling")
class IndexUnicodeE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    private String fqn;

    @BeforeEach
    void createTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        String table = "t_special_" + method + "_" + System.nanoTime();
        fqn = FQN_PREFIX + table;
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, text_col VARCHAR, category VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (id, text_col, category) VALUES"
                + " (1, 'hello world one', 'greeting'),"
                + " (2, 'hello world two', 'greeting')");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (id, text_col, category) VALUES"
                + " (3, 'data-lake_v2.0', 'tech'),"
                + " (4, 'data-lake_v3.1', 'tech')");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (id, text_col, category) VALUES"
                + " (5, 'plain_english', 'en'),"
                + " (6, 'another_english', 'en')");
    }

    @Test
    @DisplayName("BLOOM_FILTER: special character equality query returns correct results")
    void bloomFilterSpecialCharEquality() {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT * FROM " + fqn
                        + " WHERE text_col = 'hello world one'");
        sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                + "(text_col) USING BLOOM_FILTER");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn
                        + " WHERE text_col = 'hello world one'");
        assertThat(withIndex).containsAll(baseline);
    }

    @Test
    @DisplayName("PREFIX: hyphen-underscore LIKE prefix query returns correct results")
    void prefixSpecialCharLike() {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT * FROM " + fqn
                        + " WHERE text_col LIKE 'data-lake%'");
        sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn
                + "(text_col) USING PREFIX");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn
                        + " WHERE text_col LIKE 'data-lake%'");
        assertThat(withIndex).containsAll(baseline);
    }
}
