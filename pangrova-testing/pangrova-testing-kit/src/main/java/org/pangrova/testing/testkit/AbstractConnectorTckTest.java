package org.pangrova.testing.testkit;

import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.TableProcedure;
import org.pangrova.connector.spi.metadata.table.ColumnHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Technology Compatibility Kit (TCK) for the {@link ConnectorMetadata} SPI contract.
 *
 * <p>Concrete subclasses provide a real {@link ConnectorMetadata} implementation
 * and a pre-populated test schema+table. The TCK then exercises the core contract
 * methods that every connector must honor:
 * <ul>
 *   <li>{@code listSchemaNames} returns the test schema</li>
 *   <li>{@code getTableHandle} returns non-null for existing tables, null for missing</li>
 *   <li>{@code getTableMetadata} returns columns with types</li>
 *   <li>{@code getColumnHandles} returns a non-empty map</li>
 *   <li>{@code applyLimit} returns a result</li>
 *   <li>{@code listTableProcedures} returns procedure descriptors</li>
 *   <li>{@code executeProcedure} for unknown procedure throws</li>
 * </ul>
 *
 * <p>This is the Pangrova equivalent of Trino's {@code BaseConnectorTest} /
 * connector TCK pattern, ensuring uniform contract verification across all
 * connector implementations.
 *
 * @see ConnectorMetadata
 */
public abstract class AbstractConnectorTckTest {

    private ConnectorMetadata metadata;
    private ConnectorSession session;

    /**
     * Returns the ConnectorMetadata implementation under test.
     * Called once per test via {@link #setUp()}.
     */
    protected abstract ConnectorMetadata createMetadata();

    /**
     * Returns a ConnectorSession to use for all TCK calls.
     */
    protected abstract ConnectorSession createSession();

    /**
     * Returns the name of the test schema that has been pre-populated
     * by the subclass (created before tests run, e.g., in {@code @BeforeEach}).
     */
    protected abstract String testSchemaName();

    /**
     * Returns the name of the test table within {@link #testSchemaName()}.
     * The table must have at least two columns.
     */
    protected abstract String testTableName();

    /**
     * Hook for subclasses to create the test schema and table before each test.
     * Called before the TCK's own {@link #setUp()}.
     */
    protected abstract void setUpConnector() throws Exception;

    @BeforeEach
    void setUp() throws Exception {
        setUpConnector();
        metadata = createMetadata();
        session = createSession();
    }

    // -- Schema discovery -------------------------------------------------------

    @Test
    void listSchemaNames_containsTestSchema() {
        List<String> schemas = metadata.listSchemaNames(session);
        assertNotNull(schemas, "listSchemaNames must not return null");
        assertTrue(schemas.contains(testSchemaName()),
                "listSchemaNames should contain the test schema '" + testSchemaName()
                        + "', got: " + schemas);
    }

    @Test
    void listTables_containsTestTable() {
        List<SchemaTableName> tables = metadata.listTables(session, Optional.of(testSchemaName()));
        assertNotNull(tables, "listTables must not return null");
        SchemaTableName expected = new SchemaTableName(testSchemaName(), testTableName());
        assertTrue(tables.contains(expected),
                "listTables should contain " + expected + ", got: " + tables);
    }

    // -- Table handle resolution ------------------------------------------------

    @Test
    void getTableHandle_existingTable_returnsNonNull() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(testSchemaName(), testTableName()));
        assertNotNull(handle,
                "getTableHandle must return non-null for an existing table");
    }

    @Test
    void getTableHandle_nonExistingTable_returnsNull() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(testSchemaName(), "nonexistent_table_xyz_999"));
        assertNull(handle,
                "getTableHandle must return null for a non-existing table");
    }

    // -- Table metadata ---------------------------------------------------------

    @Test
    void getTableMetadata_returnsColumnsWithTypes() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(testSchemaName(), testTableName()));
        assertNotNull(handle);

        ConnectorTableMetadata tableMeta = metadata.getTableMetadata(session, handle);
        assertNotNull(tableMeta, "getTableMetadata must not return null");
        assertEquals(new SchemaTableName(testSchemaName(), testTableName()), tableMeta.table(),
                "table name in metadata must match");

        assertNotNull(tableMeta.columns(), "columns list must not be null");
        assertFalse(tableMeta.columns().isEmpty(),
                "columns list must not be empty for a table with columns");

        // If typed columns are provided, they must match column count
        if (!tableMeta.typedColumns().isEmpty()) {
            assertEquals(tableMeta.columns().size(), tableMeta.typedColumns().size(),
                    "typedColumns count must match columns count");
            for (ConnectorTableMetadata.Column col : tableMeta.typedColumns()) {
                assertNotNull(col.name(), "column name must not be null");
                assertNotNull(col.type(), "column type must not be null");
            }
        }
    }

    @Test
    void getColumnHandles_returnsNonEmptyMap() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(testSchemaName(), testTableName()));
        assertNotNull(handle);

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, handle);
        assertNotNull(columns, "getColumnHandles must not return null");
        assertFalse(columns.isEmpty(),
                "getColumnHandles must return at least one column");
    }

    // -- Pushdown: applyLimit ---------------------------------------------------

    @Test
    void applyLimit_returnsOptional() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(testSchemaName(), testTableName()));
        assertNotNull(handle);

        Optional<ConnectorTableHandle> result = metadata.applyLimit(session, handle, 10);
        // The contract says the connector MAY return empty if it does not support limit pushdown.
        // We verify it returns a valid Optional (non-null) and if present, the handle is non-null.
        assertNotNull(result, "applyLimit must not return null");
        result.ifPresent(h -> assertNotNull(h, "applyLimit handle must not be null when present"));
    }

    // -- Procedure support ------------------------------------------------------

    @Test
    void listTableProcedures_returnsNonNullList() {
        List<TableProcedure> procedures = metadata.listTableProcedures();
        assertNotNull(procedures, "listTableProcedures must not return null");
        // Every procedure must have a non-blank name and description
        for (TableProcedure proc : procedures) {
            assertNotNull(proc.name(), "procedure name must not be null");
            assertFalse(proc.name().isBlank(), "procedure name must not be blank");
            assertNotNull(proc.description(), "procedure description must not be null");
            assertFalse(proc.description().isBlank(),
                    "procedure '" + proc.name() + "' description must not be blank");
        }
    }

    @Test
    void executeProcedure_unknownProcedure_throws() {
        assertThrows(Exception.class,
                () -> metadata.executeProcedure(session, "__unknown_tck_proc__", Map.of()),
                "executeProcedure for an unknown procedure must throw");
    }
}
