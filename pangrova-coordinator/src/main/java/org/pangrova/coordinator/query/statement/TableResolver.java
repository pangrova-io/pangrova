package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
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

    public static QualifiedTableName resolveTableRef(
            List<String> parts, CoordinatorSessionContext context) {
        return resolveTableRef(parts, defaultCatalog(context), defaultSchema(context));
    }

    static QualifiedTableName resolveTableRef(
            List<String> parts, String defaultCatalog, String defaultSchema) {
        return switch (parts.size()) {
            case 3 -> new QualifiedTableName(parts.get(0), parts.get(1), parts.get(2));
            case 2 -> new QualifiedTableName(defaultCatalog, parts.get(0), parts.get(1));
            case 1 -> new QualifiedTableName(defaultCatalog, defaultSchema, parts.get(0));
            default -> throw new IllegalArgumentException("Invalid table name: " + parts);
        };
    }

    /** Resolves a rename target within the source catalog. */
    public static QualifiedTableName resolveRenameTarget(List<String> parts, QualifiedTableName source) {
        QualifiedTableName target = resolveTableRef(parts, source.catalog(), source.schema());
        if (!target.catalog().equals(source.catalog())) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ALTER TABLE RENAME cannot move a table between catalogs");
        }
        return target;
    }

    public static String defaultCatalog(CoordinatorSessionContext context) {
        return context.sessionProperties().getOrDefault("catalog", "default");
    }

    public static String defaultSchema(CoordinatorSessionContext context) {
        return context.sessionProperties().getOrDefault("schema", "default");
    }

    public static ConnectorSession buildSession(
            CoordinatorSessionContext context, String catalog, String schema) {
        return SessionContextConverter.toConnectorSession(context, catalog, schema);
    }

    /** Returns the table handle or rejects an unknown table. */
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

    /** Resolves and validates the columns targeted by an insert. */
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
