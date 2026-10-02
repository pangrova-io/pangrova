package org.pangrova.testing.e2e.setup;

import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.testing.TestDataGenerator;
import org.pangrova.testing.TestDataGenerator.ColumnDef;
import org.pangrova.testing.TestDataGenerator.ColumnType;
import org.pangrova.testing.TestDataGenerator.TableSchema;
import org.pangrova.testing.e2e.runner.SqlTestRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.StringJoiner;

/**
 * Loads TPC-H test data into target connectors.
 * Supports two modes: SQL-based (via SqlTestRunner) and Connector SPI (real data files).
 */
public final class TestDataSetup {

    private static final Logger LOG = LoggerFactory.getLogger(TestDataSetup.class);
    private static final int BASE_ROWS_PER_TABLE = 100;
    private static final int INSERT_BATCH_SIZE = 50;
    private static final String DEFAULT_SCHEMA = "default";

    private final SqlTestRunner runner;
    private TestDataGenerator generator;
    private Connector connector;

    public TestDataSetup(SqlTestRunner runner) {
        this.runner = Objects.requireNonNull(runner, "runner must not be null");
        this.generator = new TestDataGenerator();
    }

    /**
     * Load TPC-H data by writing real data files via Connector SPI.
     *
     * @param connector   the connector instance to use for metadata and data writing
     * @param scaleFactor multiplier for row count per table
     */
    public void loadTpcHDataViaConnector(Connector connector, int scaleFactor) {
        this.connector = Objects.requireNonNull(connector, "connector must not be null");
        if (scaleFactor <= 0) {
            throw new IllegalArgumentException("scaleFactor must be positive, got: " + scaleFactor);
        }
        ConnectorSession session = new TestConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        createDefaultSchema(metadata, session);
        Map<String, TableSchema> schemas = TestDataGenerator.getSchemas();
        for (String tableName : schemas.keySet().stream().sorted().toList()) {
            loadTableViaConnector(metadata, session, tableName, scaleFactor);
        }
        LOG.info("Loaded {} TPC-H tables via Connector SPI", schemas.size());
    }

    /**
     * Returns the connector instance used for data loading.
     *
     * @return the connector, or null if not yet initialized
     */
    public Connector getConnector() {
        return connector;
    }

    // --- SQL-based loading ---

    public void loadTpcHData(String catalog, String schema, int scaleFactor) {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        if (scaleFactor <= 0) {
            throw new IllegalArgumentException("scaleFactor must be positive, got: " + scaleFactor);
        }
        for (String tableName : TestDataGenerator.getSchemas().keySet().stream().sorted().toList()) {
            createTable(catalog, schema, tableName);
            insertData(catalog, schema, tableName, scaleFactor);
        }
    }

    public void createTestTable(String catalog, String schema,
                                String tableName, String columnDefs) {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(columnDefs, "columnDefs must not be null");
        runner.executeDdl(String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s.%s (%s)",
            catalog, schema, tableName, columnDefs));
    }

    // --- Connector SPI loading internals ---

    private void loadTableViaConnector(ConnectorMetadata metadata,
                                       ConnectorSession session,
                                       String tableName, int scaleFactor) {
        SchemaTableName stn = new SchemaTableName(DEFAULT_SCHEMA, tableName);
        if (metadata.getTableHandle(session, stn) != null) {
            LOG.debug("event=table_already_exists table={} skipping_creation", tableName);
            return;
        }
        TableSchema tableSchema = TestDataGenerator.getSchemas().get(tableName);
        List<ConnectorTableMetadata.Column> columns = toPangrovaColumns(tableSchema);
        ConnectorOutputTableHandle outputHandle = createTableViaMetadata(
                metadata, session, tableName, columns);
        writeDataViaPageSink(outputHandle, tableName, tableSchema, scaleFactor);
    }

    private ConnectorOutputTableHandle createTableViaMetadata(
            ConnectorMetadata metadata, ConnectorSession session,
            String tableName, List<ConnectorTableMetadata.Column> columns) {
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
            new SchemaTableName(DEFAULT_SCHEMA, tableName),
            columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
            columns, Map.of());
        return metadata.createTable(session, tableMetadata);
    }

    private void writeDataViaPageSink(ConnectorOutputTableHandle outputHandle,
                                      String tableName, TableSchema tableSchema,
                                      int scaleFactor) {
        int rowCount = scaleFactor * BASE_ROWS_PER_TABLE;
        List<TypedRow> rows = generator.generateTable(tableName, rowCount);
        List<String> columnNames = tableSchema.columns().stream()
            .map(ColumnDef::name).toList();
        Page page = new Page(columnNames, rows);
        ConnectorSession session = new TestConnectorSession();
        ConnectorPageSinkProvider sinkProvider = connector.getPageSinkProvider();
        ConnectorPageSink sink = sinkProvider.createPageSink(
                ConnectorTransactionHandle.EMPTY, session, outputHandle);
        byte[] commitMetadata;
        try {
            sink.appendPage(page);
            commitMetadata = sink.finishWithMetadata();
        } finally {
            sink.close();
        }
        // Commit the written data files to the Iceberg/Paimon catalog.
        // finishWithMetadata() only closes writers and serializes DataFile descriptors;
        // the actual catalog commit (table.newAppend().appendFile().commit()) happens
        // in ConnectorMetadata.finishDistributedWrite().
        if (commitMetadata != null && commitMetadata.length > 0) {
            ConnectorMetadata metadata = connector.getMetadata(session);
            metadata.finishDistributedWrite(session, outputHandle, List.of(commitMetadata));
        }
        LOG.debug("Wrote and committed {} rows to table {}", rowCount, tableName);
    }

    private static void createDefaultSchema(ConnectorMetadata metadata,
                                            ConnectorSession session) {
        try {
            metadata.createSchema(session, DEFAULT_SCHEMA);
        } catch (UnsupportedOperationException e) {
            LOG.debug("Connector does not support createSchema, skipping");
        } catch (Exception e) {
            // Schema may already exist
            LOG.debug("Schema creation skipped: {}", e.getMessage());
        }
    }

    // --- Type mapping ---

    private static List<ConnectorTableMetadata.Column> toPangrovaColumns(
            TableSchema schema) {
        List<ColumnDef> cols = schema.columns();
        return java.util.stream.IntStream.range(0, cols.size())
            .mapToObj(i -> toPangrovaColumn(cols.get(i), i + 1))
            .toList();
    }

    private static ConnectorTableMetadata.Column toPangrovaColumn(
            ColumnDef col, int fieldId) {
        PangrovaType type = toPangrovaType(col.type());
        return new ConnectorTableMetadata.Column(
            col.name(), type, OptionalInt.of(fieldId));
    }

    static PangrovaType toPangrovaType(ColumnType type) {
        return switch (type) {
            case STRING -> PangrovaType.VARCHAR;
            case LONG -> PangrovaType.BIGINT;
            case DOUBLE -> PangrovaType.DOUBLE;
            case DATE -> PangrovaType.DATE;
            case BOOLEAN -> PangrovaType.BOOLEAN;
        };
    }

    // --- SQL-based loading internals ---

    private void createTable(String catalog, String schema, String tableName) {
        TableSchema tableSchema = TestDataGenerator.getSchemas().get(tableName);
        StringJoiner columnDefs = new StringJoiner(", ");
        for (ColumnDef col : tableSchema.columns()) {
            columnDefs.add(col.name() + " " + toSqlType(col.type()));
        }
        createTestTable(catalog, schema, tableName, columnDefs.toString());
    }

    private void insertData(String catalog, String schema,
                            String tableName, int scaleFactor) {
        int rowCount = scaleFactor * BASE_ROWS_PER_TABLE;
        List<TypedRow> rows = generator.generateTable(tableName, rowCount);
        TableSchema tableSchema = TestDataGenerator.getSchemas().get(tableName);
        String columns = buildColumnList(tableSchema);

        for (int i = 0; i < rows.size(); i += INSERT_BATCH_SIZE) {
            int end = Math.min(i + INSERT_BATCH_SIZE, rows.size());
            String valuesList = buildValuesList(rows, i, end, tableSchema);
            String sql = String.format("INSERT INTO %s.%s.%s (%s) VALUES %s",
                catalog, schema, tableName, columns, valuesList);
            runner.execute(sql);
        }
    }

    private static String buildColumnList(TableSchema tableSchema) {
        StringJoiner columnList = new StringJoiner(", ");
        for (ColumnDef col : tableSchema.columns()) {
            columnList.add(col.name());
        }
        return columnList.toString();
    }

    private static String buildValuesList(List<TypedRow> rows, int start, int end,
                                          TableSchema tableSchema) {
        StringJoiner valuesList = new StringJoiner(", ");
        for (int j = start; j < end; j++) {
            TypedRow row = rows.get(j);
            StringJoiner values = new StringJoiner(", ", "(", ")");
            for (ColumnDef col : tableSchema.columns()) {
                ColumnValue cv = row.get(col.name());
                values.add(formatColumnValue(cv, col.type()));
            }
            valuesList.add(values.toString());
        }
        return valuesList.toString();
    }

    private static String formatColumnValue(ColumnValue cv, ColumnType type) {
        if (cv instanceof ColumnValue.NullValue) {
            return "NULL";
        }
        Object raw = ColumnValue.toRaw(cv);
        return switch (type) {
            case STRING -> "'" + escapeString(raw.toString()) + "'";
            case DATE -> "DATE '" + raw + "'";
            case LONG, DOUBLE, BOOLEAN -> raw.toString();
        };
    }

    private static String escapeString(String value) {
        return value.replace("'", "''");
    }

    private static String toSqlType(ColumnType type) {
        return switch (type) {
            case STRING -> "VARCHAR";
            case LONG -> "BIGINT";
            case DOUBLE -> "DOUBLE PRECISION";
            case DATE -> "DATE";
            case BOOLEAN -> "BOOLEAN";
        };
    }

    /** Minimal ConnectorSession for test data setup operations. */
    private static final class TestConnectorSession implements ConnectorSession {
        @Override public String getUser() { return "test-setup"; }
        @Override public Optional<String> getCatalog() { return Optional.empty(); }
        @Override public Optional<String> getSchema() { return Optional.of(DEFAULT_SCHEMA); }
        @Override public Map<String, String> getProperties() {
            return Map.of("tenant_id", "e2e-tenant");
        }
    }
}
