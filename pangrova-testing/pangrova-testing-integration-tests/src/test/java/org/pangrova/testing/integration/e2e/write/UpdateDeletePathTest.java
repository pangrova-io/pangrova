package org.pangrova.testing.integration.e2e.write;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for UPDATE and DELETE operations.
 *
 * <p>Covers conditional UPDATE, multi-column UPDATE, DELETE with WHERE,
 * and DELETE ALL through the full pipeline.
 */
@E2ETest
@DisplayName("UPDATE/DELETE Path - E2E Tests")
class UpdateDeletePathTest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_updel_" + method.replaceAll("[^a-zA-Z0-9]", "_") + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private void createAndPopulate() {
        executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, score BIGINT)");
        executeDml("INSERT INTO " + fqn() + " VALUES "
                + "(1, 'alice', 80), (2, 'bob', 90), (3, 'carol', 70), (4, 'dave', 85), (5, 'eve', 95)");
    }

    // ── UPDATE ──

    @Nested
    @DisplayName("UPDATE")
    class Update {

        @Test
        @DisplayName("UPDATE with WHERE modifies matching rows only")
        void updateWithWhere() {
            createAndPopulate();
            executeDml("UPDATE " + fqn() + " SET score = 100 WHERE name = 'alice'");

            List<TypedRow> rows = executeQuery(
                    "SELECT score FROM " + fqn() + " WHERE name = 'alice'");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("score")).longValue()).isEqualTo(100);
        }

        @Test
        @DisplayName("UPDATE multiple columns at once")
        void updateMultipleColumns() {
            createAndPopulate();
            executeDml("UPDATE " + fqn() + " SET name = 'BOB', score = 99 WHERE id = 2");

            List<TypedRow> rows = executeQuery("SELECT name, score FROM " + fqn() + " WHERE id = 2");
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("BOB");
            assertThat(((Number) rows.get(0).getRaw("score")).longValue()).isEqualTo(99);
        }

        @Test
        @DisplayName("UPDATE with no matching WHERE modifies zero rows")
        void updateNoMatch() {
            createAndPopulate();
            executeDml("UPDATE " + fqn() + " SET score = 0 WHERE id = 999");

            // All rows should be unchanged
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 5);
        }
    }

    // ── DELETE ──

    @Nested
    @DisplayName("DELETE")
    class Delete {

        @Test
        @DisplayName("DELETE with WHERE removes matching rows")
        void deleteWithWhere() {
            createAndPopulate();
            executeDml("DELETE FROM " + fqn() + " WHERE id = 3");

            assertQueryReturnsRows("SELECT * FROM " + fqn(), 4);
        }

        @Test
        @DisplayName("DELETE with compound WHERE removes subset")
        void deleteCompoundWhere() {
            createAndPopulate();
            executeDml("DELETE FROM " + fqn() + " WHERE score < 85");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows.size()).isLessThan(5);
            // All remaining rows should have score >= 85
        }

        @Test
        @DisplayName("DELETE all rows empties the table")
        void deleteAll() {
            createAndPopulate();
            executeDml("DELETE FROM " + fqn() + " WHERE id > 0");

            assertQueryReturnsEmpty("SELECT * FROM " + fqn());
        }

        @Test
        @DisplayName("DELETE that empties a partition removes that partition")
        void deleteEmptiesPartition() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'eu'), (2, 'us'), (3, 'eu')");

            executeDml("DELETE FROM " + fqn() + " WHERE region = 'eu'");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(rows.get(0).getRaw("region")).isEqualTo("us");
        }

        @Test
        @DisplayName("DELETE keeps survivors inside a partition")
        void deleteKeepsSurvivorsInPartition() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'eu'), (2, 'eu'), (3, 'us')");

            executeDml("DELETE FROM " + fqn() + " WHERE id = 2");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        }
    }
}
