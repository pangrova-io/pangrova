package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Optional;

/**
 * Resolves table references and metadata lookups for DML operations.
 */
public final class TableResolver {

    private TableResolver() {}

    /** Resolves the literal table argument of a catalog procedure for both admission and execution. */
    public static QualifiedTableName resolveProcedureTableRef(
            org.pangrova.parser.ast.statement.maintenance.CallStatement statement) {
        List<String> procedure = statement.procedureName().parts();
        if (procedure.size() < 2 || procedure.size() > 3) {
            throw new IllegalArgumentException("CALL requires catalog[.schema].procedure_name");
        }
        String first = procedureStringArgument(statement, 0);
        boolean warmup = TableWarmupExecutor.PROCEDURE_NAME.equalsIgnoreCase(
                procedure.get(procedure.size() - 1));
        if (warmup && statement.arguments().size() == 2) {
            return new QualifiedTableName(procedure.get(0), first, procedureStringArgument(statement, 1));
        }
        String[] table = first.split("\\.", -1);
        if (table.length != 2 || table[0].isBlank() || table[1].isBlank()) {
            throw new IllegalArgumentException("CALL table argument must be 'schema.table'");
        }
        return new QualifiedTableName(procedure.get(0), table[0], table[1]);
    }

    private static String procedureStringArgument(
            org.pangrova.parser.ast.statement.maintenance.CallStatement statement, int index) {
        if (statement.arguments().size() > index
                && statement.arguments().get(index) instanceof org.pangrova.parser.ast.Literal literal
                && literal.value() instanceof org.pangrova.parser.ast.LiteralValue.StringValue value
                && !value.value().isBlank()) {
            return value.value();
        }
        throw new IllegalArgumentException("CALL table argument must be a nonempty string literal");
    }

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
            org.pangrova.metadata.core.catalog.registry.CatalogRegistry registry,
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
