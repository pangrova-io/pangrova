package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Optional;

/**
 * Resolves table references and metadata lookups for DML operations.
 */
public final class TableResolver {

    private TableResolver() {}

    public static QualifiedTableName resolveTableRef(List<String> parts) {
        return QualifiedTableName.resolve(parts);
    }

    public static ConnectorSession buildSession(String catalog) {
        return new StatementConnectorSession(catalog);
    }

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

    static ConnectorTableHandle resolveTableHandle(
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

    static boolean tableExists(
            ConnectorMetadata metadata, ConnectorSession session, QualifiedTableName ref) {
        return metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table())) != null;
    }

    static ConnectorMetadata getMetadata(
            org.pangrova.metadata.catalog.CatalogRegistry registry,
            String catalog, ConnectorSession session) {
        CatalogHandle handle = registry.getCatalog(catalog);
        return handle.getMetadata(session);
    }

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
