package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.statement.dml.DmlExpressionEvaluator;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.parser.ast.CreateCatalogStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes DDL statements: CREATE/DROP/ALTER TABLE, CREATE/DROP SCHEMA, CREATE/DROP CATALOG.
 * Operates on catalog metadata directly, bypassing Calcite planner.
 */
@SuppressWarnings("unused") // TypedRow used via List.of() return
public final class DdlExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DdlExecutor.class);

    private final CatalogRegistry catalogRegistry;
    private volatile org.pangrova.metadata.index.IndexSchemaEvolutionHandler schemaEvolutionHandler;
    private volatile Runnable schemaInvalidator;

    public DdlExecutor(CatalogRegistry catalogRegistry,
                org.pangrova.metadata.index.IndexSchemaEvolutionHandler schemaEvolutionHandler,
                Runnable schemaInvalidator) {
        this.catalogRegistry = catalogRegistry;
        this.schemaEvolutionHandler = schemaEvolutionHandler;
        this.schemaInvalidator = schemaInvalidator;
    }

    /** Convenience constructor for minimal configuration (catalog only). */
    public DdlExecutor(CatalogRegistry catalogRegistry) {
        this(catalogRegistry, null, null);
    }

    /**
     * Replaces the schema evolution handler used for index invalidation on ALTER/DROP TABLE.
     * Thread-safe: field is volatile.
     */
    public void setSchemaEvolutionHandler(
            org.pangrova.metadata.index.IndexSchemaEvolutionHandler handler) {
        this.schemaEvolutionHandler = handler;
    }

    /**
     * Replaces the schema cache invalidator called after DDL operations.
     * Thread-safe: field is volatile.
     */
    public void setSchemaInvalidator(Runnable invalidator) {
        this.schemaInvalidator = invalidator;
    }

    private void invalidateSchemaCache() {
        Runnable inv = schemaInvalidator;
        if (inv != null) {
            inv.run();
            LOG.info("event=schema_cache_invalidated");
        }
    }

    public List<TypedRow> executeCreateTable(
            CreateTableStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);
        if (stmt.ifNotExists() && TableResolver.tableExists(metadata, session, ref)) {
            LOG.info("event=table_exists_skip table={}", ref);
            return List.of();
        }
        createTableInternal(metadata, session, ref, stmt);
        return List.of();
    }

    private void createTableInternal(ConnectorMetadata metadata, ConnectorSession session,
                                     QualifiedTableName ref, CreateTableStatement stmt) {
        List<ConnectorTableMetadata.Column> typedColumns = stmt.columns().stream()
                .map(col -> new ConnectorTableMetadata.Column(
                        col.name(), DdlTypeMapper.map(col.type()),
                        java.util.OptionalInt.empty(),
                        DdlTypeMapper.parameterizedType(col.type())))
                .toList();
        List<String> columnNames = stmt.columns().stream()
                .map(CreateTableStatement.ColumnDefinition::name).toList();
        Map<String, String> props = mergeStructuralProperties(stmt);
        DdlTypeMapper.injectStructuralProperties(stmt.columns(), props);
        GeneratedColumnDefinitionCompiler.compile(stmt.columns(), props);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                new SchemaTableName(ref.schema(), ref.table()),
                columnNames, typedColumns, props);
        metadata.createTable(session, tableMeta);
        invalidateSchemaCache();
        LOG.info("event=table_created table={}", ref);
    }

    private static PangrovaException invalidGeneratedColumn(String message) {
        return new PangrovaException(
                PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    private static Map<String, String> mergeStructuralProperties(CreateTableStatement stmt) {
        Map<String, String> props = new LinkedHashMap<>(stmt.properties());
        if (!stmt.primaryKeyColumns().isEmpty()) {
            props.putIfAbsent("primary-key", String.join(",", stmt.primaryKeyColumns()));
        }
        if (!stmt.partitionColumns().isEmpty()) {
            props.putIfAbsent("partition-by", String.join(",", stmt.partitionColumns()));
        }
        if (!stmt.sortColumns().isEmpty()) {
            List<String> sortEntries = stmt.sortColumns().stream()
                    .map(sc -> sc.columnName() + (sc.ascending() ? ":asc" : ":desc"))
                    .toList();
            props.putIfAbsent("sorted-by", String.join(",", sortEntries));
            if (stmt.useZOrder()) {
                props.putIfAbsent("sort-method", "zorder");
            }
        }
        return props;
    }

    public List<TypedRow> executeDropTable(
            DropTableStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);

        ConnectorTableHandle handle = TableResolver.resolveTableHandle(metadata, session, ref, stmt.ifExists());
        if (handle == null) return List.of();

        metadata.dropTable(session, handle);
        invalidateSchemaCache();
        fireDropTableEvent(context.tenantId(), ref);
        LOG.info("event=table_dropped table={}", ref);
        return List.of();
    }

    public List<TypedRow> executeAlterTable(
            AlterTableStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);
        ConnectorTableHandle handle = TableResolver.requireTableHandle(metadata, session, ref);
        validateGeneratedColumnEvolution(metadata, session, handle, stmt);
        applyAlterOperation(metadata, session, handle, stmt);
        invalidateSchemaCache();
        fireSchemaChangeEvent(context.tenantId(), ref, stmt);
        LOG.info("event=table_altered table={} operation={}", ref, stmt.getClass().getSimpleName());
        return List.of();
    }

    /**
     * Generated-column metadata and the connector schema must change in one
     * atomic catalog operation. The current connector SPI cannot provide that
     * transaction for ALTER TABLE, so affected changes fail before touching
     * the connector instead of leaving an unrecoverable half-update.
     */
    private static void validateGeneratedColumnEvolution(
            ConnectorMetadata metadata,
            ConnectorSession session,
            ConnectorTableHandle handle,
            AlterTableStatement statement) {
        if (statement instanceof AlterTableStatement.AddColumn add
                && add.embeddingSpec().isPresent()) {
            throw invalidGeneratedColumn(
                    "ALTER TABLE ADD generated column requires atomic schema and "
                            + "generated-metadata mutation; create it with CREATE TABLE "
                            + "until the connector transaction is available");
        }

        GeneratedColumnSet definitions = GeneratedColumnMetadataCodec.decode(
                metadata.getTableMetadata(session, handle).properties());
        if (definitions.getDefinitionsCount() == 0) {
            return;
        }
        if (statement instanceof AlterTableStatement.DropColumn drop) {
            rejectReferencedColumn(definitions, drop.columnName(), "drop");
        } else if (statement instanceof AlterTableStatement.RenameColumn rename) {
            rejectReferencedColumn(definitions, rename.oldName(), "rename");
        } else if (statement instanceof AlterTableStatement.ChangeColumnType change) {
            rejectReferencedColumn(definitions, change.columnName(), "change type of");
        }
    }

    private static void rejectReferencedColumn(
            GeneratedColumnSet definitions, String columnName, String operation) {
        boolean referenced = definitions.getDefinitionsList().stream()
                .anyMatch(definition ->
                        definition.getTargetColumn().equalsIgnoreCase(columnName)
                                || definition.getSourceColumnsList().stream()
                                        .anyMatch(source -> source.equalsIgnoreCase(columnName)));
        if (referenced) {
            throw invalidGeneratedColumn(
                    "cannot " + operation + " column '" + columnName
                            + "' while a generated-column definition references it");
        }
    }

    private static final Map<Class<? extends AlterTableStatement>,
            AlterHandler> ALTER_DISPATCH = Map.of(
            AlterTableStatement.DropColumn.class,
            (m, s, h, st) -> m.dropColumn(s, h, ((AlterTableStatement.DropColumn) st).columnName()),
            AlterTableStatement.RenameColumn.class,
            (m, s, h, st) -> {
                var r = (AlterTableStatement.RenameColumn) st;
                m.renameColumn(s, h, r.oldName(), r.newName());
            },
            AlterTableStatement.ChangeColumnType.class,
            (m, s, h, st) -> {
                var c = (AlterTableStatement.ChangeColumnType) st;
                m.changeColumnType(s, h, c.columnName(), DdlTypeMapper.map(c.newType()));
            },
            AlterTableStatement.CreateBranch.class,
            (m, s, h, st) -> m.createBranch(s, h, ((AlterTableStatement.CreateBranch) st).branchName()),
            AlterTableStatement.DropBranch.class,
            (m, s, h, st) -> m.dropBranch(s, h, ((AlterTableStatement.DropBranch) st).branchName()),
            AlterTableStatement.CreateTag.class,
            (m, s, h, st) -> m.createTag(s, h, ((AlterTableStatement.CreateTag) st).tagName()),
            AlterTableStatement.DropTag.class,
            (m, s, h, st) -> m.dropTag(s, h, ((AlterTableStatement.DropTag) st).tagName())
    );

    private static void applyAlterOperation(
            ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle, AlterTableStatement stmt) {
        if (stmt instanceof AlterTableStatement.AddColumn add) {
            applyAddColumn(metadata, session, handle, add);
            return;
        }
        if (stmt instanceof AlterTableStatement.SetPartitionSpec spec) {
            applySetPartitionSpec(metadata, session, handle, spec);
            return;
        }
        if (stmt instanceof AlterTableStatement.SetColumnTag setTag) {
            // Column-level tags are governance metadata stored outside connector layer.
            // ConnectorMetadata does not expose column-level properties; tag persistence
            // is handled by MetadataService.setTag() at a higher layer.
            LOG.debug("event=set_column_tag column={} tag={}={}", setTag.columnName(),
                    setTag.tagKey(), setTag.tagValue());
            return;
        }
        if (stmt instanceof AlterTableStatement.UnsetColumnTag unsetTag) {
            LOG.debug("event=unset_column_tag column={} tag={}", unsetTag.columnName(),
                    unsetTag.tagKey());
            return;
        }
        if (stmt instanceof AlterTableStatement.RenameTable rename) {
            var source = TableResolver.resolveTableRef(rename.tableName().parts(),
                    session.getCatalog().orElseThrow(), session.getSchema().orElseThrow());
            var newRef = TableResolver.resolveRenameTarget(rename.newName().parts(), source);
            metadata.renameTable(session, handle, newRef.schema(), newRef.table());
            return;
        }
        var handler = ALTER_DISPATCH.get(stmt.getClass());
        if (handler != null) {
            handler.apply(metadata, session, handle, stmt);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported ALTER TABLE operation: " + stmt.getClass().getSimpleName());
        }
    }

    private static void applySetPartitionSpec(
            ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle, AlterTableStatement.SetPartitionSpec spec) {
        List<ConnectorMetadata.PartitionTransform> transforms = spec.transforms().stream()
                .map(t -> new ConnectorMetadata.PartitionTransform(
                        t.functionName(), t.arguments()))
                .toList();
        metadata.setPartitionSpec(session, handle, transforms);
    }

    private static void applyAddColumn(
            ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle, AlterTableStatement.AddColumn add) {
        // Preserve the converted literal's Java type (Long/Integer/Double/String/Boolean/...)
        // so connectors can build typed defaults. Iceberg, for example, requires a Long
        // literal for BIGINT columns; stringifying everything causes
        // "Cannot cast default value to long: "10"".
        Object defaultVal = add.defaultValue()
                .map(DmlExpressionEvaluator::convertLiteralValue)
                .orElse(null);
        metadata.addColumn(session, handle, add.columnName(),
                PangrovaType.fromSqlTypeName(add.columnType().toCanonicalString()), defaultVal);
    }

    @FunctionalInterface
    private interface AlterHandler {
        void apply(ConnectorMetadata m, ConnectorSession s,
                   ConnectorTableHandle h, AlterTableStatement stmt);
    }

    private void fireSchemaChangeEvent(
            String tenantId, QualifiedTableName ref, AlterTableStatement stmt) {
        var handler = schemaEvolutionHandler;
        if (handler == null) {
            return;
        }
        org.pangrova.metadata.index.model.SchemaChangeEvent event =
                toSchemaChangeEvent(tenantId, ref, stmt);
        if (event != null) {
            handler.onSchemaChange(event);
        }
    }

    private static org.pangrova.metadata.index.model.SchemaChangeEvent toSchemaChangeEvent(
            String tenantId, QualifiedTableName ref, AlterTableStatement stmt) {
        if (stmt instanceof AlterTableStatement.AddColumn add) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.addColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    add.columnName(), add.columnType().toCanonicalString());
        }
        if (stmt instanceof AlterTableStatement.DropColumn drop) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.dropColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(), drop.columnName());
        }
        if (stmt instanceof AlterTableStatement.RenameColumn rename) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.renameColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    rename.oldName(), rename.newName());
        }
        if (stmt instanceof AlterTableStatement.ChangeColumnType change) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.modifyColumnType(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    change.columnName(), null, change.newType().toCanonicalString());
        }
        return null;
    }

    private void fireDropTableEvent(String tenantId, QualifiedTableName ref) {
        var handler = schemaEvolutionHandler;
        if (handler == null) {
            return;
        }
        handler.onSchemaChange(org.pangrova.metadata.index.model.SchemaChangeEvent.dropTable(
                tenantId, ref.catalog(), ref.schema(), ref.table()));
    }

    public List<TypedRow> executeCreateSchema(
            CreateSchemaStatement stmt, CoordinatorSessionContext context) {
        List<String> parts = stmt.schemaName().parts();
        if (parts.isEmpty() || parts.size() > 2) {
            throw new IllegalArgumentException("Invalid schema name: " + parts);
        }
        String catalog = parts.size() == 2 ? parts.get(0) : TableResolver.defaultCatalog(context);
        String schema = parts.size() >= 2 ? parts.get(1) : parts.get(0);
        ConnectorSession session = TableResolver.buildSession(context, catalog, schema);
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, catalog, session);
        metadata.createSchema(session, schema);
        invalidateSchemaCache();
        LOG.info("event=schema_created catalog={} schema={}", catalog, schema);
        return List.of();
    }

    public List<TypedRow> executeCreateCatalog(CreateCatalogStatement stmt) {
        Map<String, String> props = DmlExpressionEvaluator.convertProperties(stmt.properties());
        // Resolve provider: explicit USING clause > 'connector' property > fallback 'unknown'
        String provider = stmt.connectorType()
                .or(() -> java.util.Optional.ofNullable(props.get("connector")))
                .orElse("unknown");
        CatalogDescriptor descriptor = new CatalogDescriptor(
                stmt.name(), CatalogType.CONNECTOR, provider,
                CatalogState.ACTIVE, props, "", java.util.Set.of());
        catalogRegistry.register(descriptor);
        LOG.info("event=catalog_created catalog={} provider={}", stmt.name(), provider);
        return List.of();
    }

    public List<TypedRow> executeDropCatalog(DropCatalogStatement stmt) {
        catalogRegistry.unregister(stmt.name());
        LOG.info("event=catalog_dropped catalog={}", stmt.name());
        return List.of();
    }

    /**
     * Executes SET session property. Currently a no-op (properties are accepted but not persisted).
     */
    public List<TypedRow> executeSetSession(org.pangrova.parser.ast.SetSessionStatement stmt) {
        LOG.info("event=set_session property={} value={}", stmt.propertyName(), stmt.value());
        return List.of();
    }

}
