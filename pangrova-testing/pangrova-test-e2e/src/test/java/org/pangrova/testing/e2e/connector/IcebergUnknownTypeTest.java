package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
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
 * Iceberg v3 Unknown Type E2E tests: queries on UNKNOWN type columns return NULL,
 * and ALTER COLUMN TYPE can replace UNKNOWN with a concrete type.
 *
 * <p>Covers A8 acceptance criteria:
 * <ul>
 *   <li>AC1: unknown type column queries return NULL</li>
 *   <li>AC2: ALTER COLUMN TYPE replaces unknown type with concrete type</li>
 * </ul>
 *
 * <p>UNKNOWN type is an Iceberg v3 placeholder type for columns whose type is not
 * yet determined. It cannot be created via SQL DDL; tables with UNKNOWN columns
 * must be created via the Iceberg Java API.
 *
 * <p>Requirements: A8
 */
@DisplayName("Iceberg v3 Unknown Type E2E Tests")
class IcebergUnknownTypeTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private static Catalog icebergCatalog;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
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
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private TableIdentifier tableId() {
        return TableIdentifier.of(SCHEMA, tableName);
    }

    // --- 3.1.2: Unknown type column queries return NULL ---

    @Nested
    @DisplayName("3.1.2 - Unknown Type Column Queries Return NULL")
    class UnknownTypeQueryReturnsNull {

        @Test
        @DisplayName("Physical values under an UNKNOWN field ID are ignored")
        void physicalUnknownFieldIsIgnored() throws Exception {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "future", Types.UnknownType.get()));
            var table = icebergCatalog.createTable(tableId(), schema, null, Map.of("format-version", "3"));
            Schema malformedPhysicalSchema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "future", Types.StringType.get()));
            String path = table.location() + "/data/physical-unknown.parquet";
            DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(path))
                    .schema(malformedPhysicalSchema)
                    .withSpec(table.spec())
                    .createWriterFunc(GenericParquetWriter::create)
                    .build();
            try (writer) {
                GenericRecord row = GenericRecord.create(malformedPhysicalSchema);
                row.setField("id", 1L);
                row.setField("future", "must_be_ignored");
                writer.write(row);
            }
            table.newAppend().appendFile(writer.toDataFile()).commit();

            List<TypedRow> rows = sqlRunner.execute("SELECT id, future FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("future")).isNull();
        }

        @Test
        @DisplayName("Non-NULL writes to an UNKNOWN column are rejected")
        void nonNullUnknownWriteIsRejected() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "future", Types.UnknownType.get()));
            icebergCatalog.createTable(tableId(), schema, null, Map.of("format-version", "3"));

            var error = sqlRunner.executeExpectingError(
                    "INSERT INTO " + fqn() + " (id, future) VALUES (1, 'unexpected')");
            assertThat(error.message()).contains("only NULL");
            assertThat(icebergCatalog.loadTable(tableId()).currentSnapshot()).isNull();
        }

        @Test
        @DisplayName("Query table with UNKNOWN type column returns NULL for that column")
        void unknownTypeColumnReturnsNull() throws Exception {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "unknown_col", Types.UnknownType.get())
            );

            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of("format-version", "3"));

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            try (var tasks = icebergCatalog.loadTable(tableId()).newScan().planFiles()) {
                for (var task : tasks) {
                    try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                            new org.apache.hadoop.fs.Path(task.file().path().toString()),
                            new Configuration()))) {
                        assertThat(reader.getFooter().getFileMetaData().getSchema().getFields())
                                .extracting(org.apache.parquet.schema.Type::getName)
                                .containsExactly("id", "name");
                    }
                }
            }

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, unknown_col FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("alice");
            assertThat(rows.get(0).getString("unknown_col")).isNull();
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getString("name")).isEqualTo("bob");
            assertThat(rows.get(1).getString("unknown_col")).isNull();
        }

        @Test
        @DisplayName("SELECT * on table with UNKNOWN type column includes NULL values")
        void selectStarIncludesUnknownColumnAsNull() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "placeholder", Types.UnknownType.get()),
                    Types.NestedField.required(3, "value", Types.DoubleType.get())
            );

            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of("format-version", "3"));

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value) VALUES (100, 3.14)");

            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + fqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(100L);
            assertThat(rows.get(0).getString("placeholder")).isNull();
            assertThat(rows.get(0).getDouble("value")).isEqualTo(3.14);
        }

        @Test
        @DisplayName("WHERE clause on UNKNOWN type column with IS NULL works")
        void whereIsNullOnUnknownTypeColumn() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "future_field", Types.UnknownType.get())
            );

            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of("format-version", "3"));

            sqlRunner.execute("INSERT INTO " + fqn() + " (id) VALUES (1), (2), (3)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id FROM " + fqn() + " WHERE future_field IS NULL ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);

            assertThat(sqlRunner.execute("SELECT id FROM " + fqn()
                    + " WHERE future_field IS NOT NULL")).isEmpty();
            assertThat(sqlRunner.execute("SELECT id FROM " + fqn()
                    + " WHERE future_field IS NULL AND id > 1 ORDER BY id"))
                    .extracting(row -> row.getLong("id")).containsExactly(2L, 3L);
            assertThat(sqlRunner.execute("SELECT id FROM " + fqn()
                    + " WHERE future_field IS NOT NULL OR id = 1 ORDER BY id"))
                    .extracting(row -> row.getLong("id")).containsExactly(1L);
        }
    }

    // --- 3.1.3: ALTER COLUMN TYPE replaces UNKNOWN type ---

    @Nested
    @DisplayName("3.1.3 - ALTER COLUMN TYPE Replaces Unknown Type")
    class AlterColumnReplacesUnknownType {

        @Test
        @DisplayName("ALTER COLUMN TYPE changes UNKNOWN to VARCHAR")
        void alterUnknownToVarchar() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "evolving_col", Types.UnknownType.get())
            );

            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of("format-version", "3"));

            sqlRunner.execute("INSERT INTO " + fqn() + " (id) VALUES (1)");

            List<TypedRow> beforeRows = sqlRunner.execute(
                    "SELECT id, evolving_col FROM " + fqn());
            assertThat(beforeRows).hasSize(1);
            assertThat(beforeRows.get(0).getString("evolving_col")).isNull();

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " ALTER COLUMN evolving_col TYPE VARCHAR");

            registerTestConnectors(connectorConfigs());

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, evolving_col) VALUES (2, 'new_value')");

            List<TypedRow> afterRows = sqlRunner.execute(
                    "SELECT id, evolving_col FROM " + fqn() + " ORDER BY id");

            assertThat(afterRows).hasSize(2);
            assertThat(afterRows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(afterRows.get(0).getString("evolving_col")).isNull();
            assertThat(afterRows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(afterRows.get(1).getString("evolving_col")).isEqualTo("new_value");
        }

        @Test
        @DisplayName("ALTER COLUMN TYPE changes UNKNOWN to BIGINT")
        void alterUnknownToBigint() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "numeric_placeholder", Types.UnknownType.get())
            );

            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of("format-version", "3"));

            sqlRunner.execute("INSERT INTO " + fqn() + " (id) VALUES (1)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " ALTER COLUMN numeric_placeholder TYPE BIGINT");

            registerTestConnectors(connectorConfigs());

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, numeric_placeholder) VALUES (2, 42)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, numeric_placeholder FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong("numeric_placeholder")).isNull();
            assertThat(rows.get(1).getLong("numeric_placeholder")).isEqualTo(42L);
        }

        @Test
        @DisplayName("ALTER COLUMN TYPE changes UNKNOWN to DOUBLE")
        void alterUnknownToDouble() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "metric", Types.UnknownType.get())
            );

            icebergCatalog.createTable(
                    tableId(),
                    schema,
                    null,
                    Map.of("format-version", "3"));

            sqlRunner.execute("INSERT INTO " + fqn() + " (id) VALUES (1)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " ALTER COLUMN metric TYPE DOUBLE");

            registerTestConnectors(connectorConfigs());

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, metric) VALUES (2, 99.99)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, metric FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getDouble("metric")).isNull();
            assertThat(rows.get(1).getDouble("metric")).isEqualTo(99.99);
        }
    }
}
