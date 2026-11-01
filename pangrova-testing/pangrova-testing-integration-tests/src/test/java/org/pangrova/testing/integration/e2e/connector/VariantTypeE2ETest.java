package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Variant type E2E tests: CREATE TABLE with VARIANT column, INSERT JSON,
 * SELECT, path expression queries, and nested path queries.
 *
 * <p>Covers A3 acceptance criteria: VARIANT column creation, JSON insert/select,
 * path expression evaluation (dot and bracket notation), and nested path queries.
 * All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg/Paimon → local filesystem.
 *
 * <p>Requirements: A3
 */
@DisplayName("Variant Type E2E Tests")
class VariantTypeE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String PAI_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";
    private static final String ICE_PREFIX = ICE_CATALOG + "." + SCHEMA + ".";
    private static final String PAI_PREFIX = PAI_CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String iceFqn() {
        return ICE_PREFIX + tableName;
    }

    private String paiFqn() {
        return PAI_PREFIX + tableName;
    }

    // --- 2.3.7: Iceberg CREATE TABLE VARIANT column INSERT JSON SELECT ---

    @Nested
    @DisplayName("2.3.7 - Iceberg VARIANT Column")
    class IcebergVariantColumn {

        @Test
        @DisplayName("Create Iceberg v3 table with VARIANT column, insert JSON, select back")
        void createVariantTableInsertJsonSelect() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"name\":\"alice\",\"age\":30}'),"
                    + "(2, '{\"name\":\"bob\",\"age\":25}'),"
                    + "(3, '{\"name\":\"carol\",\"items\":[1,2,3]}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, payload FROM " + iceFqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            String payload0 = rows.get(0).getString("payload");
            assertThat(payload0).contains("alice");

            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            String payload1 = rows.get(1).getString("payload");
            assertThat(payload1).contains("bob");

            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
            String payload2 = rows.get(2).getString("payload");
            assertThat(payload2).contains("carol");
        }

        @Test
        @DisplayName("VARIANT column with NULL values")
        void variantColumnWithNulls() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"key\":\"value\"}'),"
                    + "(2, NULL)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, payload FROM " + iceFqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getString("payload")).isNotNull();
            assertThat(rows.get(1).getString("payload")).isNull();
        }
    }

    // --- 2.3.8: Paimon CREATE TABLE VARIANT column INSERT JSON SELECT ---

    @Nested
    @DisplayName("2.3.8 - Paimon VARIANT Column")
    class PaimonVariantColumn {

        @Test
        @DisplayName("Create Paimon table with VARIANT column, insert JSON, select back")
        void createVariantTableInsertJsonSelect() {
            sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                    + " (id BIGINT, payload VARIANT,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"name\":\"alice\",\"score\":95.5}'),"
                    + "(2, '{\"name\":\"bob\",\"score\":88.0}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, payload FROM " + paiFqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            String payload0 = rows.get(0).getString("payload");
            assertThat(payload0).contains("alice");

            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            String payload1 = rows.get(1).getString("payload");
            assertThat(payload1).contains("bob");
        }

        @Test
        @DisplayName("Paimon VARIANT column upsert preserves JSON")
        void variantColumnUpsert() {
            sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                    + " (id BIGINT, payload VARIANT,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " (id, payload) VALUES (1, '{\"v\":1}')");
            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " (id, payload) VALUES (1, '{\"v\":2}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT payload FROM " + paiFqn() + " WHERE id = 1");

            assertThat(rows).hasSize(1);
            String payload = rows.get(0).getString("payload");
            assertThat(payload).contains("2");
        }
    }

    // --- 2.3.9: Variant path expression queries ---

    @Nested
    @DisplayName("2.3.9 - Variant Path Expression Queries")
    class VariantPathExpressionQuery {

        @Test
        @DisplayName("variant_get with dot notation extracts top-level field")
        void variantGetDotNotation() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"name\":\"alice\",\"age\":30}'),"
                    + "(2, '{\"name\":\"bob\",\"age\":25}')");



            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, variant_get(payload, 'name') AS vname"
                    + " FROM " + iceFqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getString("vname")).isEqualTo("alice");
            assertThat(rows.get(1).getString("vname")).isEqualTo("bob");
        }

        @Test
        @DisplayName("variant_get with bracket notation extracts field")
        void variantGetBracketNotation() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"user-name\":\"alice\"}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT variant_get(payload, '[''user-name'']') AS uname"
                    + " FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString("uname"))
                    .isEqualTo("alice");
        }

        @Test
        @DisplayName("variant_get returns NULL for missing path")
        void variantGetMissingPathReturnsNull() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES (1, '{\"name\":\"alice\"}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT variant_get(payload, 'nonexistent') AS val"
                    + " FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString("val")).isNull();
        }
    }

    // --- 2.3.10: Nested path queries ---

    @Nested
    @DisplayName("2.3.10 - Nested Path Queries")
    class NestedPathQuery {

        @Test
        @DisplayName("variant_get with nested dot path (payload.address.city)")
        void nestedDotPath() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"address\":{\"city\":\"seattle\",\"zip\":\"98101\"}}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT variant_get(payload, 'address.city') AS city"
                    + " FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString("city"))
                    .isEqualTo("seattle");
        }

        @Test
        @DisplayName("variant_get with array index (items[0])")
        void arrayIndexPath() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"items\":[\"apple\",\"banana\",\"cherry\"]}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT variant_get(payload, 'items[0]') AS first_item"
                    + " FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString("first_item"))
                    .isEqualTo("apple");
        }

        @Test
        @DisplayName("variant_get with mixed nested path (items[0].name)")
        void mixedNestedPath() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"items\":[{\"name\":\"widget\",\"qty\":5},"
                    + "{\"name\":\"gadget\",\"qty\":3}]}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT variant_get(payload, 'items[0].name') AS item_name"
                    + " FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString("item_name"))
                    .isEqualTo("widget");
        }

        @Test
        @DisplayName("variant_get with out-of-bounds array index returns NULL")
        void outOfBoundsArrayIndex() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, payload VARIANT)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, payload) VALUES "
                    + "(1, '{\"items\":[\"only\"]}')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT variant_get(payload, 'items[99]') AS val"
                    + " FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString("val")).isNull();
        }
    }
}
