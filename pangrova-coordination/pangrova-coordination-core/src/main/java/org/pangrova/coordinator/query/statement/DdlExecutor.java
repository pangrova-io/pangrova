package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.statement.dml.DmlExpressionEvaluator;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.parser.ast.CreateCatalogStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.value.TypedRow;
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
    private final TableControlDdlOperations tableControlOperations;
    private volatile org.pangrova.indexing.store.IndexSchemaEvolutionHandler schemaEvolutionHandler;
    private volatile Runnable schemaInvalidator;

    /** Creates the executor with optional index invalidation hooks. */
    public DdlExecutor(CatalogRegistry catalogRegistry,
                org.pangrova.indexing.store.IndexSchemaEvolutionHandler schemaEvolutionHandler,
                Runnable schemaInvalidator) {
        this.catalogRegistry = catalogRegistry;
        this.tableControlOperations = new TableControlDdlOperations(catalogRegistry);
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
            org.pangrova.indexing.store.IndexSchemaEvolutionHandler handler) {
        this.schemaEvolutionHandler = handler;
    }

    /**
     * Replaces the schema cache invalidator called after DDL operations.
     * Thread-safe: field is volatile.
     */
    public void setSchemaInvalidator(Runnable invalidator) {
        this.schemaInvalidator = invalidator;
    }

    /** Enables {@code ADOPT WITH (...)} maintenance policy properties. */
    public void setMaintenancePolicyService(
            org.pangrova.table.maintenance.TableControlMaintenanceService service) {
        tableControlOperations.setMaintenancePolicyService(service);
    }

    /** Records ADOPT/UNLINK in the durable audit trail. */
    public void setTableControlAudit(
            org.pangrova.coordinator.query.audit.TableControlAuditEmitter audit) {
        tableControlOperations.setAudit(audit);
    }

    /** Purges table_id-scoped governance bindings when the control record retires. */

    /** Owns durable column-tag persistence and governance notification. */
    public void setMetadataService(MetadataService metadataService) {
        tableControlOperations.setMetadataService(metadataService);
    }

    /**
     * Name-keyed column masks enforced by the read path. Without it, a rename
     * would silently stop masking the moved table and a drop would leave its
     * masks behind for the next table that reuses the name.
     */
    public void setColumnMaskStore(org.pangrova.persistence.api.security.mask.ColumnMaskStore store) {
        tableControlOperations.setColumnMaskStore(store);
    }

    private void invalidateSchemaCache() {
        Runnable inv = schemaInvalidator;
        if (inv != null) {
            inv.run();
            LOG.info("event=schema_cache_invalidated");
        }
    }

    /** Creates a table using the resolved catalog and session. */
    public List<TypedRow> executeCreateTable(
            CreateTableStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);
        catalogRegistry.tableControl().requireCreatableName(ref.catalog(), ref.schema(), ref.table());
        if (stmt.ifNotExists() && TableResolver.tableExists(metadata, session, ref)) {
            LOG.info("event=table_exists_skip table={}", ref);
            return List.of();
        }
        createTableInternal(metadata, session, ref, stmt);
        registerCreatedTable(metadata, session, ref, context);
        return List.of();
    }

    private void registerCreatedTable(ConnectorMetadata metadata, ConnectorSession session,
            QualifiedTableName ref, CoordinatorSessionContext context) {
        var tableControl = catalogRegistry.tableControl();
        if (!tableControl.enabled()) {
            return;
        }
        String owner = context.userIdentity() == null ? "" : context.userIdentity().principal();
        try {
            tableControl.registerCreatedFromConnector(metadata, session,
                    ref.catalog(), ref.schema(), ref.table(), owner);
        } catch (RuntimeException failure) {
            tableControl.registerOrphanedManaged(ref.catalog(), ref.schema(), ref.table(),
                    owner, java.util.Optional.empty());
            throw new PangrovaException(PangrovaErrorCode.METADATA_METASTORE_ERROR,
                    "Table was created but its control record could not be registered: " + ref,
                    failure);
        }
    }

    private void createTableInternal(ConnectorMetadata metadata, ConnectorSession session,
                                     QualifiedTableName ref, CreateTableStatement stmt) {
        ConnectorTableMetadata tableMeta = declaredTableMetadata(new SchemaTableName(ref.schema(), ref.table()), stmt);
        metadata.createTable(session, tableMeta);
        invalidateSchemaCache();
        LOG.info("event=table_created table={}", ref);
    }

    /** Compiles the exact CREATE declaration before any catalog mutation or downstream planning. */
    public static ConnectorTableMetadata declaredTableMetadata(
            SchemaTableName tableName, CreateTableStatement stmt) {
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
        return new ConnectorTableMetadata(
                tableName,
                columnNames, typedColumns, props);
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

    /** Drops a table using the resolved catalog and session. */
    public List<TypedRow> executeDropTable(
            DropTableStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);

        ConnectorTableHandle handle = TableResolver.resolveTableHandle(metadata, session, ref, stmt.ifExists());
        if (handle == null) return List.of();

        var tableControl = catalogRegistry.tableControl();
        tableControl.requireDroppable(ref.catalog(), ref.schema(), ref.table(), "DROP TABLE");
        if (tableControl.enforcing()) {
            tableControl.requireDroppable(ref.catalog(), ref.schema(), ref.table(), "DROP TABLE",
                    metadata.getTableIdentity(session, handle));
        }
        var record = tableControl.find(ref.catalog(), ref.schema(), ref.table());
        if (tableControl.enabled()) {
            record.ifPresent(captured -> tableControl.requireCurrentCatalog(captured, "DROP TABLE"));
        }
        List<String> columns = tableColumns(metadata, session, handle);
        metadata.dropTable(session, handle);
        record.ifPresent(tableControl::markDropped);
        record.ifPresent(target -> tableControlOperations.purgeTableState(target, context));
        record.ifPresent(target -> tableControlOperations.purgeColumnMasks(target, columns));
        invalidateSchemaCache();
        fireDropTableEvent(context.tenantId(), ref);
        LOG.info("event=table_dropped table={}", ref);
        return List.of();
    }

    /** Alters a table using the resolved catalog and session. */
    public List<TypedRow> executeAlterTable(
            AlterTableStatement stmt, CoordinatorSessionContext context) {
        if (stmt instanceof AlterTableStatement.Adopt adopt) {
            return tableControlOperations.adopt(adopt, context);
        }
        if (stmt instanceof AlterTableStatement.Unlink unlink) {
            return tableControlOperations.unlink(unlink, context);
        }
        if (stmt instanceof AlterTableStatement.SetColumnTag setTag) {
            return executeSetColumnTag(setTag, context);
        }
        if (stmt instanceof AlterTableStatement.UnsetColumnTag unsetTag) {
            return executeUnsetColumnTag(unsetTag, context);
        }
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);
        var tableControl = catalogRegistry.tableControl();
        tableControl.requireManagedForDdl(ref.catalog(), ref.schema(), ref.table(), "ALTER TABLE");
        ConnectorTableHandle handle = TableResolver.requireTableHandle(metadata, session, ref);
        if (tableControl.enforcing()) {
            tableControl.requireManagedForDdl(ref.catalog(), ref.schema(), ref.table(), "ALTER TABLE",
                    metadata.getTableIdentity(session, handle));
        }
        validateGeneratedColumnEvolution(metadata, session, handle, stmt);
        var renameTarget = resolveRenameTarget(stmt, ref);
        List<String> columns = renameTarget == null ? List.of() : tableColumns(metadata, session, handle);
        // Move ownership before the physical rename so a target that already has a
        // live control record is rejected while the catalog is still untouched.
        org.pangrova.table.api.control.TableControlRecord movedControl = null;
        if (renameTarget != null && tableControl.enabled()) {
            var sourceControl = tableControl.find(ref.catalog(), ref.schema(), ref.table());
            if (sourceControl.isPresent()) {
                movedControl = tableControl.rename(sourceControl.get(),
                        renameTarget.schema(), renameTarget.table());
            }
        }
        // An exception can follow a committed or still-running connector rename.
        // Keep both names reserved until reconciliation proves the outcome.
        applyAlterOperation(metadata, session, handle, stmt);
        if (renameTarget != null) {
            tableControlOperations.moveNameScopedGovernance(ref, renameTarget, columns, context);
            if (movedControl != null) {
                var renamed = TableResolver.requireTableHandle(metadata, session, renameTarget);
                tableControl.completeRename(movedControl, metadata.getTableIdentity(session, renamed),
                        metadata.observeVersion(session, renamed));
            }
        }
        invalidateSchemaCache();
        fireSchemaChangeEvent(context.tenantId(), ref, stmt);
        LOG.info("event=table_altered table={} operation={}", ref, stmt.getClass().getSimpleName());
        return List.of();
    }

    /**
     * Assigns a governance tag. The tag is durably stored, the governance
     * binding is recorded against the stable table_id, and policy readers derive
     * masks directly from that durable authority. Events are cache hints.
     */
    private List<TypedRow> executeSetColumnTag(
            AlterTableStatement.SetColumnTag stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        MetadataService service = requireTagMetadataService();
        service.setTag(SessionContextConverter.toMetadataSessionContext(context),
                new QualifiedTableRef(ref.catalog(), ref.schema(), ref.table()),
                stmt.columnName(), stmt.tagKey(), stmt.tagValue());
        invalidateSchemaCache();
        LOG.info("event=column_tag_set table={} column={} tagKey={}",
                ref, stmt.columnName(), stmt.tagKey());
        return List.of();
    }

    /** Removes a governance tag and the derived policy it produced. */
    private List<TypedRow> executeUnsetColumnTag(
            AlterTableStatement.UnsetColumnTag stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        MetadataService service = requireTagMetadataService();
        service.unsetTag(SessionContextConverter.toMetadataSessionContext(context),
                new QualifiedTableRef(ref.catalog(), ref.schema(), ref.table()),
                stmt.columnName(), stmt.tagKey());
        invalidateSchemaCache();
        LOG.info("event=column_tag_unset table={} column={} tagKey={}",
                ref, stmt.columnName(), stmt.tagKey());
        return List.of();
    }

    private MetadataService requireTagMetadataService() {
        MetadataService service = tableControlOperations.metadataService();
        if (service == null) {
            throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                    "ALTER TABLE column tags require the metadata service");
        }
        return service;
    }

    private static List<String> tableColumns(ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle) {
        try {
            return metadata.getTableMetadata(session, handle).columns();
        } catch (RuntimeException unavailable) {
            LOG.warn("event=table_columns_unavailable error={}", unavailable.toString());
            return List.of();
        }
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
            (m, s, h, st) -> {
                var branch = (AlterTableStatement.CreateBranch) st;
                var age = branch.retentionDays().isPresent()
                        ? java.util.OptionalLong.of(Math.multiplyExact(branch.retentionDays().getAsLong(),
                                java.time.Duration.ofDays(1).toMillis()))
                        : java.util.OptionalLong.empty();
                m.createBranch(s, h, branch.branchName(),
                        new org.pangrova.spi.connector.metadata.BranchCreationOptions(
                                java.util.OptionalLong.empty(), java.util.Optional.empty(), age));
            },
            AlterTableStatement.DropBranch.class,
            (m, s, h, st) -> m.dropBranch(s, h, ((AlterTableStatement.DropBranch) st).branchName()),
            AlterTableStatement.CreateTag.class,
            (m, s, h, st) -> m.createTag(s, h, ((AlterTableStatement.CreateTag) st).tagName()),
            AlterTableStatement.DropTag.class,
            (m, s, h, st) -> m.dropTag(s, h, ((AlterTableStatement.DropTag) st).tagName())
    );

    private static QualifiedTableName resolveRenameTarget(
            AlterTableStatement stmt, QualifiedTableName ref) {
        if (!(stmt instanceof AlterTableStatement.RenameTable rename)) {
            return null;
        }
        return TableResolver.resolveRenameTarget(rename.newName().parts(), ref);
    }

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
        org.pangrova.indexing.store.SchemaChangeEvent event =
                toSchemaChangeEvent(tenantId, ref, stmt);
        if (event != null) {
            handler.onSchemaChange(event);
        }
    }

    private static org.pangrova.indexing.store.SchemaChangeEvent toSchemaChangeEvent(
            String tenantId, QualifiedTableName ref, AlterTableStatement stmt) {
        if (stmt instanceof AlterTableStatement.AddColumn add) {
            return org.pangrova.indexing.store.SchemaChangeEvent.addColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    add.columnName(), add.columnType().toCanonicalString());
        }
        if (stmt instanceof AlterTableStatement.DropColumn drop) {
            return org.pangrova.indexing.store.SchemaChangeEvent.dropColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(), drop.columnName());
        }
        if (stmt instanceof AlterTableStatement.RenameColumn rename) {
            return org.pangrova.indexing.store.SchemaChangeEvent.renameColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    rename.oldName(), rename.newName());
        }
        if (stmt instanceof AlterTableStatement.ChangeColumnType change) {
            return org.pangrova.indexing.store.SchemaChangeEvent.modifyColumnType(
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
        handler.onSchemaChange(org.pangrova.indexing.store.SchemaChangeEvent.dropTable(
                tenantId, ref.catalog(), ref.schema(), ref.table()));
    }

    /** Creates a schema in the resolved catalog. */
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

    /** Creates a catalog from the statement properties. */
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
