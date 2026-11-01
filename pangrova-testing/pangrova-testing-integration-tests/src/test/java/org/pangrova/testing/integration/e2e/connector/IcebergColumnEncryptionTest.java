package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg Row-Level Encryption E2E tests: encrypted column write with authorized
 * decryption read, and unauthorized read producing a permission error.
 *
 * <p>Covers A9 acceptance criteria:
 * <ul>
 *   <li>AC1: 列加密写入 — encrypted columns written via Parquet column encryption</li>
 *   <li>AC2: 有权解密 — authorized user can read decrypted values</li>
 *   <li>AC3: 无权报错 — unauthorized user gets a permission/decryption error</li>
 * </ul>
 *
 * <p>Table property format: {@code 'encryption.column.keys' = 'test-key:ssn,credit_card'}
 *
 * <p>The coordinator resolves key IDs to actual key bytes before passing to the
 * connector via IcebergWriteContext. On read, the native layer (Velox Parquet reader)
 * uses the resolved key to transparently decrypt. Without the key, decryption fails.
 *
 * <p>Requirements: A9
 */
@DisplayName("Iceberg Row-Level Encryption E2E Tests")
class IcebergColumnEncryptionTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_encryption_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private static Catalog icebergCatalog;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString(),
                        Map.of("encryption.accessible-keys", "test-key")));
    }

    @BeforeAll
    static void setupCatalog() {
        isDockerReady();
        Configuration hadoopConf = new Configuration();
        icebergCatalog = new HadoopCatalog(hadoopConf, icebergWarehouse().toString());
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_enc_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private TableIdentifier tableId() {
        return TableIdentifier.of(SCHEMA, tableName);
    }

    /**
     * Creates an Iceberg v3 table with encryption.column.keys configured.
     * The table has: id (BIGINT), name (VARCHAR), ssn (VARCHAR), credit_card (VARCHAR).
     * Columns ssn and credit_card are encrypted with key ID "test-key".
     */
    private Table createEncryptedTable() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "ssn", Types.StringType.get()),
                Types.NestedField.optional(4, "credit_card", Types.StringType.get())
        );

        return icebergCatalog.createTable(
                tableId(),
                schema,
                null,
                Map.of(
                        "format-version", "3",
                        "encryption.column.keys", "test-key:ssn,credit_card"
                ));
    }

    // --- 3.2.4: Encrypted column write + authorized read (decryption succeeds) ---

    @Nested
    @DisplayName("3.2.4 - Encrypted Column Write and Authorized Read")
    class EncryptedWriteAuthorizedRead {

        @Test
        @DisplayName("Write encrypted columns and read back decrypted values with authorized key access")
        void writeEncryptedColumnsAndReadDecrypted() {
            createEncryptedTable();

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, ssn, credit_card) VALUES "
                    + "(1, 'Alice', '123-45-6789', '4111-1111-1111-1111'), "
                    + "(2, 'Bob', '987-65-4321', '5500-0000-0000-0004')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, ssn, credit_card FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);

            // Row 1: Alice
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("Alice");
            assertThat(rows.get(0).getString("ssn")).isEqualTo("123-45-6789");
            assertThat(rows.get(0).getString("credit_card")).isEqualTo("4111-1111-1111-1111");

            // Row 2: Bob
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getString("name")).isEqualTo("Bob");
            assertThat(rows.get(1).getString("ssn")).isEqualTo("987-65-4321");
            assertThat(rows.get(1).getString("credit_card")).isEqualTo("5500-0000-0000-0004");
        }

        @Test
        @DisplayName("Encrypted columns are transparent to SELECT * queries")
        void selectStarReturnsDecryptedValues() {
            createEncryptedTable();

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, ssn, credit_card) VALUES (10, 'Charlie', '555-12-3456', '3782-822463-10005')");

            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + fqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(10L);
            assertThat(rows.get(0).getString("name")).isEqualTo("Charlie");
            assertThat(rows.get(0).getString("ssn")).isEqualTo("555-12-3456");
            assertThat(rows.get(0).getString("credit_card")).isEqualTo("3782-822463-10005");
        }

        @Test
        @DisplayName("Non-encrypted columns remain readable alongside encrypted columns")
        void nonEncryptedColumnsReadableIndependently() {
            createEncryptedTable();

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, ssn, credit_card) VALUES "
                    + "(1, 'Alice', '111-22-3333', '4000-0000-0000-0001'), "
                    + "(2, 'Bob', '444-55-6666', '4000-0000-0000-0002')");

            // Query only non-encrypted columns
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("Alice");
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getString("name")).isEqualTo("Bob");
        }
    }

    // --- 3.2.5: Unauthorized read produces permission error ---

    @Nested
    @DisplayName("3.2.5 - Unauthorized Read Permission Error")
    class UnauthorizedReadPermissionError {

        @Test
        @DisplayName("Reading encrypted columns without key access produces a permission error")
        void readWithoutKeyAccessProducesError() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "ssn", Types.StringType.get()),
                    Types.NestedField.optional(4, "credit_card", Types.StringType.get())
            );

            // Create table with an invalid/inaccessible key ID to simulate unauthorized access
            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of(
                            "format-version", "3",
                            "encryption.column.keys", "unauthorized-key:ssn,credit_card"
                    ));

            // Write data (coordinator resolves key for write)
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, ssn, credit_card) VALUES (1, 'Alice', '123-45-6789', '4111-1111-1111-1111')");

            // Attempt to read with a session that lacks key access
            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT ssn, credit_card FROM " + fqn());

            assertThat(error.errorType()).isEqualTo("RUNTIME");
            assertThat(error.message()).containsAnyOf(
                    "encryption", "decrypt", "key", "permission", "access denied",
                    "ENCRYPTION", "DECRYPT", "KEY", "PERMISSION", "ACCESS_DENIED");
        }

        @Test
        @DisplayName("Reading non-encrypted columns succeeds even when encrypted columns are inaccessible")
        void nonEncryptedColumnsReadableWithoutKeyAccess() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "ssn", Types.StringType.get()),
                    Types.NestedField.optional(4, "credit_card", Types.StringType.get())
            );

            // Create table with an inaccessible key for encrypted columns
            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of(
                            "format-version", "3",
                            "encryption.column.keys", "no-access-key:ssn,credit_card"
                    ));

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, ssn, credit_card) VALUES (1, 'Alice', '123-45-6789', '4111-1111-1111-1111')");

            // Reading only non-encrypted columns should succeed
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("Alice");
        }

        @Test
        @DisplayName("SELECT * on table with inaccessible encrypted columns produces error")
        void selectStarWithInaccessibleEncryptedColumnsProducesError() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "secret_data", Types.StringType.get())
            );

            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of(
                            "format-version", "3",
                            "encryption.column.keys", "revoked-key:secret_data"
                    ));

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, secret_data) VALUES (1, 'top-secret')");

            // SELECT * includes the encrypted column, so it should fail
            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + fqn());

            assertThat(error.errorType()).isEqualTo("RUNTIME");
            assertThat(error.message()).containsAnyOf(
                    "encryption", "decrypt", "key", "permission", "access denied",
                    "ENCRYPTION", "DECRYPT", "KEY", "PERMISSION", "ACCESS_DENIED");
        }
    }
}
