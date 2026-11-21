package org.pangrova.metadata.resolution;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Optional;

/** Metadata lookups using caller-owned connector sessions and handles.
 * These operations do not grant authorization, commit writes, or close borrowed resources.
 */
public final class ConnectorTableLookup {
    private ConnectorTableLookup() {}

    /** Resolves the table or throws METADATA_TABLE_NOT_FOUND; connector failures propagate. */
    public static ConnectorTableHandle requireTableHandle(
            ConnectorMetadata metadata, ConnectorSession session, QualifiedTableName ref) {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table()));
        if (handle == null) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + ref);
        }
        return handle;
    }

    /** Returns null for a missing table only when ifExists is true; connector failures propagate. */
    public static ConnectorTableHandle resolveTableHandle(
            ConnectorMetadata metadata, ConnectorSession session,
            QualifiedTableName ref, boolean ifExists) {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table()));
        if (handle == null && !ifExists) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + ref);
        }
        return handle;
    }

    /** Checks existence through the borrowed connector session without changing authorization. */
    public static boolean tableExists(
            ConnectorMetadata metadata, ConnectorSession session, QualifiedTableName ref) {
        return metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table())) != null;
    }

    /** Obtains metadata for the current catalog using the caller-owned session. */
    public static ConnectorMetadata getMetadata(
            org.pangrova.metadata.catalog.registry.CatalogRegistry registry,
            String catalog, ConnectorSession session) {
        CatalogHandle handle = registry.getCatalog(catalog);
        return handle.getMetadata(session);
    }

    /** Uses the explicit column order when present, otherwise the connector table column order. */
    public static List<String> resolveInsertColumns(
            Optional<List<String>> explicitColumns, ConnectorMetadata metadata,
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        if (explicitColumns.isPresent()) {
            return explicitColumns.get();
        }
        ConnectorTableMetadata tableMeta = metadata.getTableMetadata(session, tableHandle);
        return tableMeta.columns();
    }
}
