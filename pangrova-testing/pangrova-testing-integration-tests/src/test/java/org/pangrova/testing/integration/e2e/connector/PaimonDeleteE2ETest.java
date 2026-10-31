package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * DELETE correctness on Paimon Parquet primary-key tables through the real SQL pipeline.
 *
 * <p>Covers metadata truncation and native survivor overwrites, including empty
 * partitions, latest-value reads across commits, and SQL three-valued logic.
 */
@DisplayName("Paimon DELETE - E2E Tests")
class PaimonDeleteE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_delete";
    private static final String SCHEMA = "default";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().orElseThrow().getName();
        tableName = "t_delete_" + method + "_" + System.nanoTime();
    }

    @ParameterizedTest(name = "DELETE ALL: partitioned={0}")
    @ValueSource(booleans = {false, true})
    void deleteAllCanRepeatAndReinsert(boolean partitioned) {
        createTable(partitioned);
        insertLatestRows();

        sqlRunner.execute("DELETE FROM " + fqn());
        assertRows();
        sqlRunner.execute("DELETE FROM " + fqn());
        assertRows();

        insertRows("(1, 100, 'us'), (3, 300, 'eu')");
        assertRows(tuple(1L, 100L, "us"), tuple(3L, 300L, "eu"));
    }

    @Test
    void zeroRowInsertSelectDoesNotCreateRowsOrBlockTheNextWrite() {
        createTable(false);
        sqlRunner.execute("INSERT INTO " + fqn()
                + " SELECT id, score, region FROM " + fqn() + " WHERE id < 0");
        assertRows();
        insertRows("(1, 100, 'us')");
        assertRows(tuple(1L, 100L, "us"));
    }

    @Test
    @DisplayName("Metadata partition equality removes only the exact VARCHAR partition")
    void partitionEqualityDeletesOnlyMatchingPartition() {
        createTable(true);
        insertLatestRows();
        insertRows("(6, 60, 'usa')");

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE region = 'us'");
        assertRows(tuple(3L, 30L, "eu"), tuple(4L, 40L, "eu"),
                tuple(5L, 50L, "ap"), tuple(6L, 60L, "usa"));

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE region = 'us'");
        assertRows(tuple(3L, 30L, "eu"), tuple(4L, 40L, "eu"),
                tuple(5L, 50L, "ap"), tuple(6L, 60L, "usa"));
        insertRows("(1, 100, 'us')");
        assertRows(tuple(1L, 100L, "us"), tuple(3L, 30L, "eu"),
                tuple(4L, 40L, "eu"), tuple(5L, 50L, "ap"), tuple(6L, 60L, "usa"));
    }

    @Test
    @DisplayName("Metadata partition conjunction must match both VARCHAR keys")
    void partitionConjunctionDeletesOnlyMatchingPartition() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, score BIGINT, region VARCHAR, segment VARCHAR,"
                + " PRIMARY KEY (id, region, segment) NOT ENFORCED)"
                + " PARTITIONED BY (region, segment)"
                + " WITH ('file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, score, region, segment) VALUES "
                + "(1, 10, 'us', 'active'), (2, 20, 'us', 'archive'),"
                + " (3, 30, 'eu', 'active'), (4, 40, 'us', 'active')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, score, region, segment) VALUES "
                + "(1, 100, 'us', 'active'), (2, 200, 'us', 'archive')");
        assertThat(readSegmentRows()).containsExactly(
                tuple(1L, 100L, "us", "active"), tuple(2L, 200L, "us", "archive"),
                tuple(3L, 30L, "eu", "active"), tuple(4L, 40L, "us", "active"));

        sqlRunner.execute("DELETE FROM " + fqn()
                + " WHERE region = 'us' AND segment = 'active'");

        assertThat(readSegmentRows()).containsExactly(
                tuple(2L, 200L, "us", "archive"), tuple(3L, 30L, "eu", "active"));
    }

    @ParameterizedTest(name = "Native data predicate: partitioned={0}")
    @ValueSource(booleans = {false, true})
    void dataPredicateEmptiesPartitionAndRetainsOthers(boolean partitioned) {
        createTable(partitioned);
        insertLatestRows();

        // A data-column predicate is not a metadata partition truncation.
        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE score < 25");
        assertRows(tuple(3L, 30L, "eu"), tuple(4L, 40L, "eu"), tuple(5L, 50L, "ap"));
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn() + " WHERE region = 'us'"))
                .isEmpty();

        // Reusing a deleted key must not resurrect other rows from the emptied partition.
        insertRows("(1, 100, 'us')");
        assertRows(tuple(1L, 100L, "us"), tuple(3L, 30L, "eu"),
                tuple(4L, 40L, "eu"), tuple(5L, 50L, "ap"));
    }

    @ParameterizedTest(name = "Complex native predicate: partitioned={0}")
    @ValueSource(booleans = {false, true})
    void complexPredicateDeletesUsingLatestValues(boolean partitioned) {
        createTable(partitioned);
        insertLatestRows();

        sqlRunner.execute("DELETE FROM " + fqn()
                + " WHERE score + id < 25 OR (region = 'eu' AND score * 2 = 80)");

        // The US partition is empty; the EU partition retains its latest nonmatching row.
        assertRows(tuple(3L, 30L, "eu"), tuple(5L, 50L, "ap"));
    }

    @ParameterizedTest(name = "Native overwrite without survivors: partitioned={0}")
    @ValueSource(booleans = {false, true})
    void nativeDeleteCanRemoveEveryRow(boolean partitioned) {
        createTable(partitioned);
        insertLatestRows();

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE score + id > 0");
        assertRows();
        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE score + id > 0");
        assertRows();

        insertRows("(3, 300, 'eu')");
        assertRows(tuple(3L, 300L, "eu"));
    }

    @ParameterizedTest(name = "NULL and NOT semantics: {0}")
    @ValueSource(strings = {"score < 25", "NOT (score >= 25)", "NOT (score + 1 >= 26)"})
    void unknownPredicateRetainsNullRows(String predicate) {
        createTable(true);
        insertRows("(1, 5, 'us'), (2, 10, 'us'), (3, 20, 'eu'), (4, 40, 'eu')");
        insertRows("(1, NULL, 'us'), (3, 30, 'eu')");
        assertRows(tuple(1L, null, "us"), tuple(2L, 10L, "us"),
                tuple(3L, 30L, "eu"), tuple(4L, 40L, "eu"));

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE " + predicate);
        assertRows(tuple(1L, null, "us"), tuple(3L, 30L, "eu"), tuple(4L, 40L, "eu"));

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE score IS NULL");
        assertRows(tuple(3L, 30L, "eu"), tuple(4L, 40L, "eu"));
    }

    @ParameterizedTest(name = "No-match DELETE: partitioned={0}")
    @ValueSource(booleans = {false, true})
    void noMatchPreservesLatestRowsIncludingNull(boolean partitioned) {
        createTable(partitioned);
        insertLatestRows();
        insertRows("(6, NULL, 'ap')");
        Tuple[] expected = {
                tuple(1L, 10L, "us"), tuple(2L, 20L, "us"), tuple(3L, 30L, "eu"),
                tuple(4L, 40L, "eu"), tuple(5L, 50L, "ap"), tuple(6L, null, "ap")
        };
        assertRows(expected);

        for (String predicate : List.of("region = 'missing'", "score = 999", "score + id = 999")) {
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE " + predicate);
            assertRows(expected);
        }
    }

    private String fqn() {
        return CATALOG + "." + SCHEMA + "." + tableName;
    }

    private void createTable(boolean partitioned) {
        String primaryKey = partitioned ? "id, region" : "id";
        String partitionClause = partitioned ? " PARTITIONED BY (region)" : "";
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, score BIGINT, region VARCHAR,"
                + " PRIMARY KEY (" + primaryKey + ") NOT ENFORCED)"
                + partitionClause + " WITH ('file.format' = 'parquet')");
    }

    private void insertRows(String values) {
        sqlRunner.execute("INSERT INTO " + fqn() + " (id, score, region) VALUES " + values);
    }

    private void insertLatestRows() {
        insertRows("(1, 60, 'us'), (2, 20, 'us'), (3, 5, 'eu'), (4, 40, 'eu')");
        // Both updates cross the deletion threshold in opposite directions.
        insertRows("(1, 10, 'us'), (3, 30, 'eu'), (5, 50, 'ap')");
        assertRows(tuple(1L, 10L, "us"), tuple(2L, 20L, "us"),
                tuple(3L, 30L, "eu"), tuple(4L, 40L, "eu"), tuple(5L, 50L, "ap"));
    }

    private void assertRows(Tuple... expected) {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, score, region FROM " + fqn() + " ORDER BY id, region");
        assertThat(rows)
                .extracting(row -> row.getLong("id"), row -> row.getLong("score"),
                        row -> row.getString("region"))
                .containsExactly(expected);
    }

    private List<Tuple> readSegmentRows() {
        return sqlRunner.execute("SELECT id, score, region, segment FROM " + fqn()
                        + " ORDER BY id, region, segment").stream()
                .map(row -> tuple(row.getLong("id"), row.getLong("score"),
                        row.getString("region"), row.getString("segment")))
                .toList();
    }
}
