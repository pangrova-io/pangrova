package org.pangrova.coordinator.query.statement;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.parser.ast.ShowSchemasStatement;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Executes SHOW and DESCRIBE statements: SHOW SCHEMAS, SHOW TABLES,
 * SHOW CATALOGS, DESCRIBE TABLE.
 * Read-only metadata queries that bypass Calcite planner.
 *
 * <p>When {@link AccessControl} is provided, SHOW TABLES filters results
 * to only include tables the user has SELECT permission on.</p>
 */
public final class ShowDescribeExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ShowDescribeExecutor.class);

    private final CatalogRegistry catalogRegistry;
    private final AccessControl accessControl;

    /** Convenience constructor when access control is not configured. */
    public ShowDescribeExecutor(CatalogRegistry catalogRegistry) {
        this(catalogRegistry, null);
    }

    public ShowDescribeExecutor(CatalogRegistry catalogRegistry, AccessControl accessControl) {
        this.catalogRegistry = catalogRegistry;
        this.accessControl = accessControl;
    }

    /**
     * Executes SHOW SCHEMAS with optional schema-level access control filtering.
     * When {@code user} is non-null and {@code accessControl} is configured,
     * schemas the user cannot access are excluded from the result.
     */
    public List<TypedRow> executeShowSchemas(ShowSchemasStatement stmt, CoordinatorSessionContext context) {
        UserIdentity user = context.userIdentity();
        String catalog = stmt.catalogName();
        ConnectorSession session = TableResolver.buildSession(context, catalog, TableResolver.defaultSchema(context));
        ConnectorMetadata metadata = TableResolver.getMetadata(
                catalogRegistry, catalog, session);
        List<String> schemas = metadata.listSchemaNames(session);
        if (accessControl != null && user != null) {
            schemas = schemas.stream()
                    .filter(s -> canAccessSchema(user, catalog, s))
                    .toList();
        }
        return schemas.stream()
                .map(s -> TypedRow.builder()
                        .putString("schema_name", s).build())
                .toList();
    }

    private boolean canAccessSchema(UserIdentity user, String catalog, String schema) {
        try {
            accessControl.checkCanAccessSchema(user, catalog, schema);
            return true;
        } catch (Exception e) {
            LOG.debug("event=schema_filtered_by_access_control catalog={} schema={}"
                    + " user={}", catalog, schema,
                    user != null ? user.principal() : "anonymous");
            return false;
        }
    }

    /**
     * Executes SHOW TABLES with optional table-level access control filtering.
     * When {@code user} is non-null and {@code accessControl} is configured,
     * tables the user cannot SELECT from are excluded from the result.
     */
    public List<TypedRow> executeShowTables(ShowTablesStatement stmt, CoordinatorSessionContext context) {
        UserIdentity user = context.userIdentity();
        List<String> parts = stmt.qualifiedName();
        String catalog = parts.size() >= 2 ? parts.get(0) : TableResolver.defaultCatalog(context);
        String schema = parts.size() >= 2 ? parts.get(1) : parts.get(0);
        ConnectorSession session = TableResolver.buildSession(context, catalog, schema);
        ConnectorMetadata metadata = TableResolver.getMetadata(
                catalogRegistry, catalog, session);
        List<SchemaTableName> tables = metadata.listTables(
                session, Optional.of(schema));
        if (accessControl != null && user != null) {
            tables = tables.stream()
                    .filter(t -> canSelectTable(user, catalog, schema, t.table()))
                    .toList();
        }
        return tables.stream()
                .map(t -> TypedRow.builder()
                        .putString("table_name", t.table()).build())
                .toList();
    }

    private boolean canSelectTable(UserIdentity user, String catalog,
                                   String schema, String table) {
        try {
            accessControl.checkCanSelectFromTable(user, catalog, schema, table);
            return true;
        } catch (Exception e) {
            LOG.debug("event=table_filtered_by_access_control catalog={} schema={}"
                    + " table={} user={}", catalog, schema, table,
                    user != null ? user.principal() : "anonymous");
            return false;
        }
    }

    public List<TypedRow> executeDescribeTable(DescribeTableStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(
                stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(
                catalogRegistry, ref.catalog(), session);
        ConnectorTableHandle handle = TableResolver.requireTableHandle(
                metadata, session, ref);
        ConnectorTableMetadata tableMeta =
                metadata.getTableMetadata(session, handle);
        return tableMeta.typedColumns().stream()
                .map(col -> TypedRow.builder()
                        .putString("column_name", col.name())
                        .putString("data_type", col.type().name())
                        .build())
                .toList();
    }

    public List<TypedRow> executeShowCatalogs() {
        return catalogRegistry.listCatalogNames().stream()
                .map(name -> TypedRow.builder()
                        .putString("catalog_name", name).build())
                .toList();
    }
}
