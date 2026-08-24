package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.format.NativeFormatCapabilities;
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
import org.pangrova.parser.ast.type.BaseType;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.RowField;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.data.type.primitive.BigIntType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimestampType;
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

    /**
     * The fixed STRUCT schema that FILE type expands to.
     * Schema: STRUCT(uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at TIMESTAMP).
     */
    private static final RowType FILE_STRUCT_TYPE = new RowType(List.of(
            new RowField("uri", VarcharType.UNBOUNDED),
            new RowField("mime_type", VarcharType.UNBOUNDED),
            new RowField("size", BigIntType.INSTANCE),
            new RowField("checksum", VarcharType.UNBOUNDED),
            new RowField("created_at", TimestampType.WITHOUT_TIMEZONE)
    ));

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

    public List<TypedRow> executeCreateTable(CreateTableStatement stmt) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
        ConnectorSession session = TableResolver.buildSession(ref.catalog());
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
                        col.name(), mapDataType(col.type()),
                        java.util.OptionalInt.empty(), buildParameterizedType(col.type())))
                .toList();
        List<String> columnNames = stmt.columns().stream()
                .map(CreateTableStatement.ColumnDefinition::name).toList();
        Map<String, String> props = mergeStructuralProperties(stmt);
        rejectLanceIfUnavailable(props, ref);
        injectVectorDimensionProperties(stmt.columns(), props);
        ConnectorTableMetadata tableMeta = new ConnectorTableMetadata(
                new SchemaTableName(ref.schema(), ref.table()),
                columnNames, typedColumns, props);
        metadata.createTable(session, tableMeta);
        invalidateSchemaCache();
        LOG.info("event=table_created table={}", ref);
    }

    /**
     * Rejects CREATE TABLE when the requested file format is Lance but the native
     * binary was built without Lance C FFI support. Checks both Iceberg
     * ({@code write.format.default}) and Paimon ({@code file.format}) properties.
     *
     * @throws PangrovaException with {@link PangrovaErrorCode#AI_LANCE_FORMAT_UNAVAILABLE}
     *         if Lance format is requested but unavailable
     */
    private static void rejectLanceIfUnavailable(Map<String, String> props, QualifiedTableName ref) {
        if (NativeFormatCapabilities.isAvailable()) {
            return;
        }
        String icebergFormat = props.get("write.format.default");
        String paimonFormat = props.get("file.format");
        if (isLanceFormat(icebergFormat) || isLanceFormat(paimonFormat)) {
            throw new PangrovaException(PangrovaErrorCode.AI_LANCE_FORMAT_UNAVAILABLE,
                    "Cannot create table '" + ref + "' with Lance file format: "
                            + "native binary was built without Lance C FFI support. "
                            + "Rebuild with PANGROVA_LANCE_AVAILABLE=1 or use a different format.");
        }
    }

    private static boolean isLanceFormat(String format) {
        return format != null && format.equalsIgnoreCase("lance");
    }

    /**
     * Extracts VECTOR dimension and ARRAY element type from structured DataTypeSpec
     * and injects them as table properties for connector-level schema mapping.
     */
    private static void injectVectorDimensionProperties(
            List<CreateTableStatement.ColumnDefinition> columns, Map<String, String> props) {
        for (CreateTableStatement.ColumnDefinition col : columns) {
            DataTypeSpec typeSpec = col.type();
            if (typeSpec instanceof DataTypeSpec.VectorType vector) {
                props.put("pangrova.vector.dimension." + col.name(), String.valueOf(vector.dimension()));
            }
            if (typeSpec instanceof DataTypeSpec.ArrayType array) {
                props.put("pangrova.array.element." + col.name(), array.elementType().toCanonicalString());
            }
            if (typeSpec instanceof DataTypeSpec.FileType) {
                props.put("pangrova.file.column." + col.name(), "true");
            }
        }
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

    public List<TypedRow> executeDropTable(DropTableStatement stmt) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
        ConnectorSession session = TableResolver.buildSession(ref.catalog());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);

        ConnectorTableHandle handle = TableResolver.resolveTableHandle(metadata, session, ref, stmt.ifExists());
        if (handle == null) return List.of();

        metadata.dropTable(session, handle);
        invalidateSchemaCache();
        fireDropTableEvent(ref);
        LOG.info("event=table_dropped table={}", ref);
        return List.of();
    }

    public List<TypedRow> executeAlterTable(AlterTableStatement stmt) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
        ConnectorSession session = TableResolver.buildSession(ref.catalog());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);
        ConnectorTableHandle handle = TableResolver.requireTableHandle(metadata, session, ref);
        applyAlterOperation(metadata, session, handle, stmt);
        invalidateSchemaCache();
        fireSchemaChangeEvent(ref, stmt);
        LOG.info("event=table_altered table={} operation={}", ref, stmt.getClass().getSimpleName());
        return List.of();
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
                m.changeColumnType(s, h, c.columnName(), mapDataType(c.newType()));
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
            var newRef = TableResolver.resolveTableRef(rename.newName().parts());
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

    private void fireSchemaChangeEvent(QualifiedTableName ref, AlterTableStatement stmt) {
        var handler = schemaEvolutionHandler;
        if (handler == null) {
            return;
        }
        org.pangrova.metadata.index.model.SchemaChangeEvent event = toSchemaChangeEvent(ref, stmt);
        if (event != null) {
            handler.onSchemaChange(event);
        }
    }

    private static org.pangrova.metadata.index.model.SchemaChangeEvent toSchemaChangeEvent(
            QualifiedTableName ref, AlterTableStatement stmt) {
        if (stmt instanceof AlterTableStatement.AddColumn add) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.addColumn(
                    ref.catalog(), ref.schema(), ref.table(),
                    add.columnName(), add.columnType().toCanonicalString());
        }
        if (stmt instanceof AlterTableStatement.DropColumn drop) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.dropColumn(
                    ref.catalog(), ref.schema(), ref.table(), drop.columnName());
        }
        if (stmt instanceof AlterTableStatement.RenameColumn rename) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.renameColumn(
                    ref.catalog(), ref.schema(), ref.table(),
                    rename.oldName(), rename.newName());
        }
        if (stmt instanceof AlterTableStatement.ChangeColumnType change) {
            return org.pangrova.metadata.index.model.SchemaChangeEvent.modifyColumnType(
                    ref.catalog(), ref.schema(), ref.table(),
                    change.columnName(), null, change.newType().toCanonicalString());
        }
        return null;
    }

    private void fireDropTableEvent(QualifiedTableName ref) {
        var handler = schemaEvolutionHandler;
        if (handler == null) {
            return;
        }
        handler.onSchemaChange(org.pangrova.metadata.index.model.SchemaChangeEvent.dropTable(
                ref.catalog(), ref.schema(), ref.table()));
    }

    public List<TypedRow> executeCreateSchema(CreateSchemaStatement stmt) {
        List<String> parts = stmt.schemaName().parts();
        String catalog = parts.size() >= 2 ? parts.get(0) : "default";
        String schema = parts.size() >= 2 ? parts.get(1) : parts.get(0);
        ConnectorSession session = TableResolver.buildSession(catalog);
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, catalog, session);
        metadata.createSchema(session, schema);
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

    /**
     * Converts a structured DataTypeSpec to the SPI PangrovaType enum.
     * Uses instanceof pattern matching on the sealed type hierarchy for precise mapping,
     * avoiding String serialization/deserialization round-trip.
     */
    private static PangrovaType mapDataType(DataTypeSpec spec) {
        if (spec instanceof DataTypeSpec.PrimitiveType p) {
            return mapBaseType(p.baseType());
        } else if (spec instanceof DataTypeSpec.DecimalType) {
            return PangrovaType.DECIMAL;
        } else if (spec instanceof DataTypeSpec.ArrayType) {
            return PangrovaType.ARRAY;
        } else if (spec instanceof DataTypeSpec.MapType) {
            return PangrovaType.MAP;
        } else if (spec instanceof DataTypeSpec.StructType) {
            return PangrovaType.STRUCT;
        } else if (spec instanceof DataTypeSpec.VectorType) {
            return PangrovaType.VECTOR;
        } else if (spec instanceof DataTypeSpec.FileType) {
            return PangrovaType.STRUCT;
        }
        throw new IllegalArgumentException("Unknown DataTypeSpec variant: " + spec.getClass());
    }

    private static org.pangrova.spi.connector.data.type.Type buildParameterizedType(DataTypeSpec spec) {
        if (spec instanceof DataTypeSpec.StructType st) {
            List<RowField> fields = st.fields().stream()
                    .map(f -> new RowField(f.name(), buildParameterizedType(f.type())))
                    .toList();
            return new RowType(fields);
        } else if (spec instanceof DataTypeSpec.ArrayType at) {
            return new ArrayType(buildParameterizedType(at.elementType()));
        } else if (spec instanceof DataTypeSpec.MapType mt) {
            return new MapType(buildParameterizedType(mt.keyType()), buildParameterizedType(mt.valueType()));
        } else if (spec instanceof DataTypeSpec.FileType) {
            return FILE_STRUCT_TYPE;
        } else if (spec instanceof DataTypeSpec.DecimalType dt) {
            // Preserve declared precision/scale; the enum fallback below would
            // erase them to DECIMAL(38,18).
            return new org.pangrova.spi.connector.data.type.primitive.DecimalType(
                    dt.precision(), dt.scale());
        }
        return PangrovaTypeMapping.toType(mapDataType(spec));
    }

    private static PangrovaType mapBaseType(BaseType baseType) {
        return switch (baseType) {
            case BOOLEAN -> PangrovaType.BOOLEAN;
            case TINYINT -> PangrovaType.TINYINT;
            case SMALLINT -> PangrovaType.SMALLINT;
            case INTEGER -> PangrovaType.INTEGER;
            case BIGINT -> PangrovaType.BIGINT;
            case REAL, FLOAT -> PangrovaType.FLOAT;
            case DOUBLE -> PangrovaType.DOUBLE;
            case VARCHAR, CHAR, STRING -> PangrovaType.VARCHAR;
            case VARBINARY, BLOB -> PangrovaType.VARBINARY;
            case DATE -> PangrovaType.DATE;
            case TIME -> PangrovaType.TIME;
            case TIMESTAMP -> PangrovaType.TIMESTAMP;
            case TIMESTAMP_NS -> PangrovaType.TIMESTAMP_NS;
            case INTERVAL -> PangrovaType.BIGINT;
            case UUID -> PangrovaType.UUID;
            case VARIANT -> PangrovaType.VARIANT;
            case GEOMETRY -> PangrovaType.GEOMETRY;
            case GEOGRAPHY -> PangrovaType.GEOGRAPHY;
        };
    }
}
