package org.pangrova.coordinator.query.statement;

import org.pangrova.parser.api.ast.statement.AlterCatalogStatement;
import org.pangrova.query.session.SessionTableNames;
import org.pangrova.metadata.resolution.ConnectorTableLookup;
import org.pangrova.metadata.resolution.QualifiedTableNames;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.statement.dml.DmlExpressionEvaluator;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.parser.api.ast.CreateCatalogStatement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropCatalogStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.planner.metadata.DdlTypeMapper;
import org.pangrova.planner.metadata.TableDeclarationCompiler;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Executes table mutations, schema creation and catalog lifecycle SQL against their metadata authorities.
 */
@SuppressWarnings("unused") // TypedRow used via List.of() return
public final class DdlExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DdlExecutor.class);

    private final CatalogRegistry catalogRegistry;
    private final TableControlDdlOperations tableControlOperations;
    private volatile org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler schemaEvolutionHandler;
    private final Runnable schemaInvalidator;

    /** Creates the executor with optional index invalidation hooks. */
    public DdlExecutor(CatalogRegistry catalogRegistry,
                org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler schemaEvolutionHandler,
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
            org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler handler) {
        this.schemaEvolutionHandler = handler;
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

    /** Owns durable column-tag persistence and governance notification. */
    public void setMetadataService(MetadataService metadataService) {
        tableControlOperations.setMetadataService(metadataService);
    }

    /**
     * Name-keyed column masks enforced by the read path. Without it, a rename
     * would silently stop masking the moved table and a drop would leave its
     * masks behind for the next table that reuses the name.
     */
    public void setColumnMaskStore(org.pangrova.persistence.security.api.mask.ColumnMaskStore store) {
        tableControlOperations.setColumnMaskStore(store);
    }

    private void invalidateSchemaCache() {
        if (schemaInvalidator != null) {
            schemaInvalidator.run();
            LOG.info("event=schema_cache_invalidated");
        }
    }

    /** Creates a table using the resolved catalog and session. */
    public List<TypedRow> executeCreateTable(
            CreateTableStatement stmt, QuerySession context) {
        QualifiedTableName ref = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = SessionContextConverter.toConnectorSession(context, ref.catalog(), ref.schema());
        var catalogHandle = catalogRegistry.getCatalog(ref.catalog());
        ConnectorMetadata metadata = catalogHandle.getMetadata(session);
        catalogRegistry.tableControl().registrations().requireCreatableName(ref.catalog(), ref.schema(), ref.table());
        if (stmt.ifNotExists() && ConnectorTableLookup.tableExists(metadata, session, ref)) {
            LOG.info("event=table_exists_skip table={}", ref);
            return List.of();
        }
        var descriptor = catalogHandle.getDescriptor();
        metadata.createTable(session, TableDeclarationCompiler.compile(new SchemaTableName(ref.schema(), ref.table()),
                stmt));
        registerCreatedTable(descriptor, metadata, session, ref, context);
        invalidateSchemaCache();
        LOG.info("event=table_created table={}", ref);
        return List.of();
    }

    private void registerCreatedTable(CatalogDescriptor descriptor, ConnectorMetadata metadata,
            ConnectorSession session,
            QualifiedTableName ref, QuerySession context) {
        var tableControl = catalogRegistry.tableControl();
        String owner = context.userIdentity() == null ? "" : context.userIdentity().principal();
        try {
            tableControl.registrations().registerCreatedFromConnector(metadata, session,
                    ref.catalog(), ref.schema(), ref.table(), owner,
                    new org.pangrova.table.api.identity.CatalogDeploymentIdentity(
                            descriptor.incarnationId(), descriptor.revision()));
        } catch (RuntimeException failure) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_METASTORE_ERROR,
                    "Table was created but its control record could not be registered: " + ref,
                    failure);
        }
    }

    private static PangrovaException invalidGeneratedColumn(String message) {
        return new PangrovaException(
                PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    /** Drops a table using the resolved catalog and session. */
    public List<TypedRow> executeDropTable(
            DropTableStatement stmt, QuerySession context) {
        QualifiedTableName ref = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = SessionContextConverter.toConnectorSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = ConnectorTableLookup.getMetadata(catalogRegistry, ref.catalog(), session);

        ConnectorTableHandle handle = ConnectorTableLookup.resolveTableHandle(metadata, session, ref, stmt.ifExists());
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
        java.util.UUID schemaOperation = prepareDropTableEvent(context.tenantId(), ref, metadata, session, handle);
        try {
            metadata.dropTable(session, handle);
        } catch (RuntimeException | Error failure) {
            unknownSchemaChange(schemaOperation, failure);
            throw failure;
        }
        commitSchemaChange(schemaOperation, java.util.Optional.empty());
        record.ifPresent(tableControl.registrations()::markDropped);
        record.ifPresent(target -> tableControlOperations.purgeTableState(target, context));
        record.ifPresent(target -> tableControlOperations.purgeColumnMasks(target, columns));
        invalidateSchemaCache();
        LOG.info("event=table_dropped table={}", ref);
        return List.of();
    }

    /** Alters a table using the resolved catalog and session. */
    public List<TypedRow> executeAlterTable(
            AlterTableStatement stmt, QuerySession context) {
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
        QualifiedTableName ref = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = SessionContextConverter.toConnectorSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = ConnectorTableLookup.getMetadata(catalogRegistry, ref.catalog(), session);
        var tableControl = catalogRegistry.tableControl();
        tableControl.requireManagedForDdl(ref.catalog(), ref.schema(), ref.table(), "ALTER TABLE");
        ConnectorTableHandle handle = ConnectorTableLookup.requireTableHandle(metadata, session, ref);
        if (tableControl.enforcing()) {
            tableControl.requireManagedForDdl(ref.catalog(), ref.schema(), ref.table(), "ALTER TABLE",
                    metadata.getTableIdentity(session, handle));
        }
        validateGeneratedColumnEvolution(metadata, session, handle, stmt);
        var renameTarget = resolveRenameTarget(stmt, ref);
        List<String> columns = renameTarget == null ? List.of() : tableColumns(metadata, session, handle);
        java.util.UUID schemaOperation = prepareSchemaChangeEvent(
                context.tenantId(), ref, stmt, metadata, session, handle);
        // Reserve the index schema operation before changing logical rename ownership.
        // A control failure here is provably before the connector DDL starts.
        org.pangrova.table.api.ownership.TableControlRecord movedControl = null;
        try {
            if (renameTarget != null && tableControl.enabled()) {
                var sourceControl = tableControl.find(ref.catalog(), ref.schema(), ref.table());
                if (sourceControl.isPresent()) {
                    movedControl = tableControl.renames().begin(sourceControl.get(),
                            renameTarget.schema(), renameTarget.table());
                }
            }
        } catch (RuntimeException | Error failure) {
            abortSchemaChange(schemaOperation, failure);
            throw failure;
        }
        try {
            applyAlterOperation(metadata, session, handle, stmt);
        } catch (RuntimeException | Error failure) {
            unknownSchemaChange(schemaOperation, failure);
            throw failure;
        }
        if (schemaOperation != null) {
            var target = renameTarget == null ? ref : renameTarget;
            commitSchemaChange(schemaOperation, observeCommittedIdentity(metadata, session, target));
        }
        if (renameTarget != null) {
            tableControlOperations.moveNameScopedGovernance(ref, renameTarget, columns, context);
            if (movedControl != null) {
                var renamed = ConnectorTableLookup.requireTableHandle(metadata, session, renameTarget);
                tableControl.renames().complete(movedControl, metadata.getTableIdentity(session, renamed),
                        metadata.observeVersion(session, renamed));
            }
        }
        invalidateSchemaCache();
        LOG.info("event=table_altered table={} operation={}", ref, stmt.getClass().getSimpleName());
        return List.of();
    }

    /**
     * Assigns a governance tag. The tag is durably stored, the governance
     * binding is recorded against the stable table_id, and policy readers derive
     * masks directly from that durable authority. Events are cache hints.
     */
    private List<TypedRow> executeSetColumnTag(
            AlterTableStatement.SetColumnTag stmt, QuerySession context) {
        QualifiedTableName ref = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
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
            AlterTableStatement.UnsetColumnTag stmt, QuerySession context) {
        QualifiedTableName ref = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
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
                        new org.pangrova.connector.api.metadata.BranchCreationOptions(
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
        return QualifiedTableNames.resolveRenameTarget(rename.newName().parts(), ref);
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
            var source = QualifiedTableName.resolve(rename.tableName().parts(),
                    session.getCatalog().orElseThrow(), session.getSchema().orElseThrow());
            var newRef = QualifiedTableNames.resolveRenameTarget(rename.newName().parts(), source);
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

    private java.util.UUID prepareSchemaChangeEvent(
            String tenantId, QualifiedTableName ref, AlterTableStatement stmt,
            ConnectorMetadata metadata, ConnectorSession session, ConnectorTableHandle handle) {
        var handler = schemaEvolutionHandler;
        if (handler == null) {
            return null;
        }
        var event = toSchemaChangeEvent(tenantId, ref, stmt);
        return event == null ? null : prepareAndVerifySchemaChange(handler, event, metadata, session, handle);
    }

    private static org.pangrova.indexing.api.evolution.SchemaChangeEvent toSchemaChangeEvent(
            String tenantId, QualifiedTableName ref, AlterTableStatement stmt) {
        if (stmt instanceof AlterTableStatement.AddColumn add) {
            return org.pangrova.indexing.api.evolution.SchemaChangeEvent.addColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    add.columnName(), add.columnType().toCanonicalString());
        }
        if (stmt instanceof AlterTableStatement.DropColumn drop) {
            return org.pangrova.indexing.api.evolution.SchemaChangeEvent.dropColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(), drop.columnName());
        }
        if (stmt instanceof AlterTableStatement.RenameColumn rename) {
            return org.pangrova.indexing.api.evolution.SchemaChangeEvent.renameColumn(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    rename.oldName(), rename.newName());
        }
        if (stmt instanceof AlterTableStatement.ChangeColumnType change) {
            return org.pangrova.indexing.api.evolution.SchemaChangeEvent.modifyColumnType(
                    tenantId, ref.catalog(), ref.schema(), ref.table(),
                    change.columnName(), null, change.newType().toCanonicalString());
        }
        if (stmt instanceof AlterTableStatement.RenameTable rename) {
            var target = QualifiedTableNames.resolveRenameTarget(rename.newName().parts(), ref);
            return org.pangrova.indexing.api.evolution.SchemaChangeEvent.renameTable(
                    tenantId, ref.catalog(), ref.schema(), ref.table(), target.schema(), target.table());
        }
        return null;
    }

    private java.util.UUID prepareDropTableEvent(String tenantId, QualifiedTableName ref,
            ConnectorMetadata metadata, ConnectorSession session, ConnectorTableHandle handle) {
        var handler = schemaEvolutionHandler;
        return handler == null ? null : prepareAndVerifySchemaChange(handler,
                org.pangrova.indexing.api.evolution.SchemaChangeEvent.dropTable(
                        tenantId, ref.catalog(), ref.schema(), ref.table()), metadata, session, handle);
    }

    private java.util.UUID prepareAndVerifySchemaChange(
            org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler handler,
            org.pangrova.indexing.api.evolution.SchemaChangeEvent event,
            ConnectorMetadata metadata, ConnectorSession session, ConnectorTableHandle handle) {
        var identity = metadata.getTableIdentity(session, handle);
        java.util.UUID operationId = handler.prepare(event, identity);
        try {
            if (identity.isPresent() && !identity.equals(metadata.getTableIdentity(session, handle))) {
                throw new IllegalStateException("Index schema operation " + operationId
                        + " source identity changed before the connector mutation");
            }
        } catch (RuntimeException | Error failure) {
            abortSchemaChange(operationId, failure);
            throw failure;
        }
        return operationId;
    }

    private static java.util.Optional<org.pangrova.connector.api.metadata.table.TableIdentity>
            observeCommittedIdentity(ConnectorMetadata metadata, ConnectorSession session, QualifiedTableName target) {
        try {
            var handle = metadata.getTableHandle(session,
                    new org.pangrova.connector.api.metadata.table.SchemaTableName(target.schema(), target.table()));
            return handle == null ? java.util.Optional.empty() : metadata.getTableIdentity(session, handle);
        } catch (RuntimeException failure) {
            // The connector DDL already returned success, but the post-state is
            // unknown. Preserve the durable success receipt without inventing identity.
            LOG.warn("event=index_schema_post_identity_unavailable table={} error={}", target, failure.getMessage());
            return java.util.Optional.empty();
        }
    }

    private void commitSchemaChange(java.util.UUID operationId,
            java.util.Optional<org.pangrova.connector.api.metadata.table.TableIdentity> targetIdentity) {
        if (operationId != null) {
            try {
                schemaEvolutionHandler.committed(operationId, targetIdentity);
            } catch (RuntimeException | Error failure) {
                unknownSchemaChange(operationId, failure);
                throw failure;
            }
        }
    }

    private void unknownSchemaChange(java.util.UUID operationId, Throwable failure) {
        if (operationId != null) {
            try {
                schemaEvolutionHandler.unknown(operationId);
            } catch (RuntimeException unknownFailure) {
                failure.addSuppressed(unknownFailure);
            }
        }
    }

    private void abortSchemaChange(java.util.UUID operationId, Throwable failure) {
        if (operationId != null) {
            try {
                schemaEvolutionHandler.aborted(operationId);
            } catch (RuntimeException abortFailure) {
                failure.addSuppressed(abortFailure);
            }
        }
    }

    /** Creates a schema in the resolved catalog. */
    public List<TypedRow> executeCreateSchema(
            CreateSchemaStatement stmt, QuerySession context) {
        List<String> parts = stmt.schemaName().parts();
        if (parts.isEmpty() || parts.size() > 2) {
            throw new IllegalArgumentException("Invalid schema name: " + parts);
        }
        String catalog = parts.size() == 2 ? parts.get(0) : SessionTableNames.defaultCatalog(context);
        String schema = parts.size() >= 2 ? parts.get(1) : parts.get(0);
        ConnectorSession session = SessionContextConverter.toConnectorSession(context, catalog, schema);
        ConnectorMetadata metadata = ConnectorTableLookup.getMetadata(catalogRegistry, catalog, session);
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

    /** Mutates only connector properties of the current tenant-visible deployment. */
    public void alterCatalog(AlterCatalogStatement statement, QuerySession session) {
        catalogRegistry.alterProperties(statement.name(), session.tenantId(),
                DmlExpressionEvaluator.convertProperties(statement.properties()));
        invalidateSchemaCache();
    }

    /** Unregisters the tenant catalog and invalidates cached schemas. */
    public List<TypedRow> executeDropCatalog(DropCatalogStatement stmt, QuerySession session) {
        catalogRegistry.unregister(stmt.name(), session.tenantId());
        invalidateSchemaCache();
        LOG.info("event=catalog_dropped catalog={}", stmt.name());
        return List.of();
    }
}
