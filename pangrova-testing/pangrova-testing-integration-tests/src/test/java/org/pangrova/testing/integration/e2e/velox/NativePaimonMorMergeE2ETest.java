package org.pangrova.testing.integration.e2e.velox;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * E2E test: Paimon MOR merge engine full pipeline.
 *
 * <p>Full pipeline: SQL → Parser → Planner → Substrait → Native →
 * VeloxMergeReader → transport → Client.
 *
 * <p>Validates: Requirements R8, R16.6d, R16.7
 */
@DisplayName("Native Paimon MOR Merge E2E")
class NativePaimonMorMergeE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA,
                        paimonWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA,
                        paimonWarehouse().toString())));

        String fqn = CATALOG + "." + SCHEMA;

        // Drop tables if they exist from a prior test run
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + fqn + ".mor_deduplicate_table"); } catch (Exception ignored) { }
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + fqn + ".mor_partial_update_table"); } catch (Exception ignored) { }

        // DEDUPLICATE merge engine: latest version per primary key
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".mor_deduplicate_table "
                + "(id BIGINT, value VARCHAR) "
                + "TBLPROPERTIES ('primary-key' = 'id', "
                + "'merge-engine' = 'deduplicate', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_deduplicate_table (id, value) VALUES "
                + "(1, 'old'), (2, 'keep')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_deduplicate_table (id, value) VALUES "
                + "(1, 'new')");

        // PARTIAL_UPDATE merge engine: non-null fields merged
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".mor_partial_update_table "
                + "(id BIGINT, name VARCHAR, city VARCHAR) "
                + "TBLPROPERTIES ('primary-key' = 'id', "
                + "'merge-engine' = 'partial-update', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_partial_update_table (id, name, city) VALUES "
                + "(1, 'alice', NULL)");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_partial_update_table (id, name, city) VALUES "
                + "(1, NULL, 'beijing')");

        // AGGREGATION merge engine: SUM on value column
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".mor_aggregation_table "
                + "(id BIGINT, amount BIGINT) "
                + "TBLPROPERTIES ('primary-key' = 'id', "
                + "'merge-engine' = 'aggregation', "
                + "'fields.amount.aggregate-function' = 'sum', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_aggregation_table (id, amount) VALUES "
                + "(1, 10), (2, 20)");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_aggregation_table (id, amount) VALUES "
                + "(1, 5)");

        // FIRST_ROW merge engine: first occurrence per primary key
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".mor_first_row_table "
                + "(id BIGINT, value VARCHAR) "
                + "TBLPROPERTIES ('primary-key' = 'id', "
                + "'merge-engine' = 'first-row', "
                + "'changelog-producer' = 'lookup', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_first_row_table (id, value) VALUES "
                + "(1, 'first'), (2, 'keep')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mor_first_row_table (id, value) VALUES "
                + "(1, 'second')");
    }

    @Test
    @DisplayName("MOR DEDUPLICATE: latest version per primary key")
    void morDeduplicateReturnsLatestVersion() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + CATALOG + "." + SCHEMA +
                ".mor_deduplicate_table");
        assertThat(rows).extracting(row -> row.getRaw("id"), row -> row.getRaw("value"))
                .containsExactlyInAnyOrder(tuple(1L, "new"), tuple(2L, "keep"));
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_deduplicate_table WHERE value = 'old'")).isEmpty();
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_deduplicate_table WHERE value = 'new'"))
                .singleElement().satisfies(row -> assertThat(row.getRaw("id")).isEqualTo(1L));
    }

    @Test
    @DisplayName("MOR PARTIAL_UPDATE: non-null fields merged")
    void morPartialUpdateMergesFields() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + CATALOG + "." + SCHEMA +
                ".mor_partial_update_table");
        assertThat(rows).singleElement().satisfies(row -> {
            assertThat(row.getRaw("id")).isEqualTo(1L);
            assertThat(row.getRaw("name")).isEqualTo("alice");
            assertThat(row.getRaw("city")).isEqualTo("beijing");
        });
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_partial_update_table WHERE name = 'alice' AND city IS NULL")).isEmpty();
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_partial_update_table WHERE name = 'alice' AND city = 'beijing'"))
                .singleElement().satisfies(row -> assertThat(row.getRaw("id")).isEqualTo(1L));
    }

    @Test
    @DisplayName("MOR AGGREGATION: SUM aggregation on value column")
    void morAggregationSumsValues() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + CATALOG + "." + SCHEMA +
                ".mor_aggregation_table");
        assertThat(rows).extracting(row -> row.getRaw("id"), row -> row.getRaw("amount"))
                .containsExactlyInAnyOrder(tuple(1L, 15L), tuple(2L, 20L));
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_aggregation_table WHERE amount = 10")).isEmpty();
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_aggregation_table WHERE amount = 15"))
                .singleElement().satisfies(row -> assertThat(row.getRaw("id")).isEqualTo(1L));
    }

    @Test
    @DisplayName("MOR FIRST_ROW: first occurrence per primary key")
    void morFirstRowKeepsFirstOccurrence() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + CATALOG + "." + SCHEMA +
                ".mor_first_row_table");
        assertThat(rows).extracting(row -> row.getRaw("id"), row -> row.getRaw("value"))
                .containsExactlyInAnyOrder(tuple(1L, "first"), tuple(2L, "keep"));
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_first_row_table WHERE value = 'second'")).isEmpty();
        assertThat(sqlRunner.execute("SELECT id FROM " + CATALOG + "." + SCHEMA
                + ".mor_first_row_table WHERE value = 'first'"))
                .singleElement().satisfies(row -> assertThat(row.getRaw("id")).isEqualTo(1L));
    }
}
