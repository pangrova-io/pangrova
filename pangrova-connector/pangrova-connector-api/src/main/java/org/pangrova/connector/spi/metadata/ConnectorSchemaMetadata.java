package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Optional;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

/**
 * Schema-level metadata operations for a connector.
 *
 * <p>Extracted from {@link ConnectorMetadata} per Interface Segregation Principle.
 * Connectors that do not support schema management need not implement
 * {@link #createSchema} beyond the default unsupported stub — this mirrors
 * the existing SPI contract where read-only connectors intentionally reject
 * write operations.</p>
 */
@Experimental
public interface ConnectorSchemaMetadata {

    /**
     * Lists all schema names available in this connector.
     *
     * @param session the current session context
     * @return a list of schema names, never null
     */
    List<String> listSchemaNames(ConnectorSession session);

    /**
     * Lists tables in the specified schema, or all schemas if not specified.
     *
     * @param session    the current session context
     * @param schemaName the schema to list tables from, or empty for all schemas
     * @return a list of schema-qualified table names, never null
     */
    List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName);

    /**
     * Creates a new schema/namespace/database in this connector.
     *
     * <p>Read-only connectors intentionally throw {@link UnsupportedOperationException}
     * via this default — this is the permanent SPI contract behavior for connectors
     * that do not support schema creation, not a placeholder.</p>
     *
     * @param session    the current session context
     * @param schemaName the name of the schema to create
     */
    default void createSchema(ConnectorSession session, String schemaName) {
        throw new UnsupportedOperationException("createSchema not supported");
    }
}
