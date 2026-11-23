package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.connector.api.enums.MetadataTableKind;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.common.api.annotation.Stable;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.types.enums.PangrovaType;

/**
 * Provides metadata operations for a connector.
 *
 * <p>The engine uses this interface to discover schemas, tables, columns,
 * and table statistics from the underlying data source.</p>
 */
@Stable(since = "1.0")
public interface ConnectorMetadata
        extends ConnectorWriteMetadata, ConnectorIndexMetadata,
        ConnectorSchemaMetadata, ConnectorQueryLifecycle, ConnectorSnapshotMetadata,
        ConnectorPushdownMetadata, ConnectorTableControlMetadata {

    /**
     * Returns a table handle for the specified table, or {@code null} if the
     * table does not exist.
     *
     * @param session   the current session context
     * @param tableName the schema-qualified table name
     * @return a table handle, or null if the table does not exist
     */
    ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName);

    /** Finds a named read source, or returns null if the table does not exist. */
    default ConnectorReadDiscovery discoverRead(ConnectorSession session, SchemaTableName table) {
        var handle = getTableHandle(session, table);
        return handle == null ? null : new ConnectorReadDiscovery(this, handle,
                (bindingSession, selector, check) -> check == null ? bindRead(bindingSession, selector)
                        : bindReadWithIdentityCheck(bindingSession, handle, selector, check));
    }

    /** Whether an ordinary named lookup can only return the exact catalog spelling. */
    default boolean supportsExactTableNameLookup() {
        return false;
    }

    /**
     * Returns the metadata for the specified table.
     *
     * @param session the current session context
     * @param table   the table handle obtained from {@link #getTableHandle}
     * @return the table metadata, never null
     */
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table);

    /** Describes the current named table without requiring the caller to retain its scan handle. */
    default Optional<ConnectorTableMetadata> describeNamedTable(ConnectorSession session, SchemaTableName table) {
        var handle = getTableHandle(session, table);
        return handle == null ? Optional.empty() : Optional.of(getTableMetadata(session, handle));
    }

    /**
     * Returns read metadata with explicit declaration evidence. Historical consumers require EXACT;
     * physical schema availability alone does not prove that a snapshot had no generated columns.
     * Connectors without versioned declaration evidence report UNAVAILABLE, never fabricate absence.
     */
    default ConnectorTableReadMetadata getTableReadMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableReadMetadata(getTableMetadata(session, table),
                TableDeclarationEvidence.UNAVAILABLE);
    }

    /**
     * Binds one read to its immutable handle, declaration and same-source statistics.
     * Snapshot-capable connectors override this method; the default claims no
     * snapshot or cardinality evidence and does not grant native-read capability.
     */
    default org.pangrova.connector.api.metadata.table.ConnectorReadBinding bindRead(
            ConnectorSession session, ConnectorTableHandle table) {
        return new org.pangrova.connector.api.metadata.table.ConnectorReadBinding(table,
                getTableReadMetadata(session, table), TableStatistics.UNKNOWN,
                org.pangrova.connector.api.enums.ReadSnapshotState.UNVERSIONED,
                java.util.OptionalLong.empty(), "", java.util.Optional.empty(), java.util.Optional.empty());
    }

    /** Checks the current physical identity before binding the selected snapshot. */
    default ConnectorReadBinding bindReadWithIdentityCheck(ConnectorSession session, ConnectorTableHandle liveTable,
            ConnectorTableHandle selector, Consumer<Optional<TableIdentity>> beforeSnapshotBinding) {
        beforeSnapshotBinding.accept(getTableIdentity(session, liveTable));
        return bindRead(session, selector);
    }

    /**
     * Validates the selected source before enumerating splits, including an empty view.
     * The default has no snapshot evidence to validate; native readability remains
     * the connector's split-manager capability, not an inference from statistics.
     */
    default void validateReadBinding(ConnectorSession session,
            org.pangrova.connector.api.metadata.table.ConnectorReadBinding binding) {
        if (binding.state() != org.pangrova.connector.api.enums.ReadSnapshotState.UNVERSIONED) {
            throw new UnsupportedOperationException("Connector cannot validate selected read evidence");
        }
    }

    /**
     * Returns column handles for all columns in the specified table.
     *
     * @param session the current session context
     * @param table   the table handle
     * @return a map of column name to column handle, never null
     */
    Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table);

    /**
     * Returns statistics for the specified table.
     *
     * @param session the current session context
     * @param table   the table handle
     * @return the table statistics, never null
     */
    TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table);

    // --- Write operations inherited from ConnectorWriteMetadata ---
    // createTable, beginInsert, beginDelete, supportsTargetedDelete,
    // beginOverwrite, dropTable, finishDistributedWrite defaults are
    // provided by ConnectorWriteMetadata. Connectors override as needed.

    /**
     * Adds a column to an existing table.
     *
     * <p>Compatibility: additive default method — no migration required for existing connectors.
     *
     * @param session    the current session context
     * @param table      the table handle
     * @param columnName the name of the column to add
     * @param columnType the type of the column
     */
    default void addColumn(ConnectorSession session, ConnectorTableHandle table,
                           String columnName, PangrovaType columnType) {
        throw new UnsupportedOperationException("addColumn not supported");
    }

    /**
     * Adds a column with an optional default value to an existing table.
     *
     * <p>Compatibility: additive default method — delegates to {@link #addColumn(ConnectorSession,
     * ConnectorTableHandle, String, PangrovaType)} when no default is provided. Existing connectors
     * need not implement this overload.
     *
     * @param session      the current session context
     * @param table        the table handle
     * @param columnName   the name of the column to add
     * @param columnType   the type of the column
     * @param defaultValue the default value for existing rows, or null if none
     */
    default void addColumn(ConnectorSession session, ConnectorTableHandle table,
                           String columnName, PangrovaType columnType, Object defaultValue) {
        addColumn(session, table, columnName, columnType);
    }

    /**
     * Drops a column from an existing table.
     *
     * <p>Compatibility: additive default method — no migration required for existing connectors.
     *
     * @param session    the current session context
     * @param table      the table handle
     * @param columnName the name of the column to drop
     */
    default void dropColumn(ConnectorSession session, ConnectorTableHandle table,
                            String columnName) {
        throw new UnsupportedOperationException("dropColumn not supported");
    }

    /**
     * Renames a column in an existing table.
     *
     * <p>Compatibility: additive default method — no migration required for existing connectors.
     *
     * @param session the current session context
     * @param table   the table handle
     * @param oldName the current column name
     * @param newName the new column name
     */
    default void renameColumn(ConnectorSession session, ConnectorTableHandle table,
                              String oldName, String newName) {
        throw new UnsupportedOperationException("renameColumn not supported");
    }

    default void renameTable(ConnectorSession session, ConnectorTableHandle table,
                             String newSchemaName, String newTableName) {
        throw new UnsupportedOperationException("renameTable not supported");
    }

    /**
     * Changes the type of a column in an existing table.
     *
     * <p>Compatibility: additive default method — no migration required for existing connectors.
     *
     * @param session    the current session context
     * @param table      the table handle
     * @param columnName the column name
     * @param newType    the new column type
     */
    default void changeColumnType(ConnectorSession session, ConnectorTableHandle table,
                                  String columnName, PangrovaType newType) {
        throw new UnsupportedOperationException("changeColumnType not supported");
    }

    /**
     * A partition transform definition used in {@link #setPartitionSpec}.
     *
     * <p>Identity transforms use {@code null} as the function name and a single-element
     * list with the column name. Single-arg transforms like year(ts) have one argument.
     * Multi-arg transforms like bucket(col1, col2, 16) have multiple arguments.</p>
     *
     * @param functionName the transform function name (null for identity)
     * @param arguments    the transform arguments (column names and/or literal values)
     */
    record PartitionTransform(String functionName, List<String> arguments) {
        public PartitionTransform {
            arguments = arguments == null ? List.of() : List.copyOf(arguments);
        }

        /** Creates an identity partition transform for the given column. */
        public static PartitionTransform identity(String columnName) {
            return new PartitionTransform(null, List.of(columnName));
        }
    }

    /**
     * Replaces the partition spec of an existing table.
     *
     * <p>Compatibility: additive default method — no migration required for existing connectors.
     *
     * @param session    the current session context
     * @param table      the table handle
     * @param transforms list of partition transforms defining the new partition spec
     */
    default void setPartitionSpec(ConnectorSession session, ConnectorTableHandle table,
                                  List<PartitionTransform> transforms) {
        throw new UnsupportedOperationException("setPartitionSpec not supported");
    }

    /**
     * Creates a named branch on the table's version history.
     *
     * @param session the connector session
     * @param table   the table handle
     * @param branchName the branch name to create
     * @param options exact source and reference lifetime; unsupported options must fail before mutation
     */
    default void createBranch(ConnectorSession session, ConnectorTableHandle table, String branchName,
                              BranchCreationOptions options) {
        throw new UnsupportedOperationException("createBranch not supported");
    }

    /**
     * Drops a named branch from the table's version history.
     *
     * @param session the connector session
     * @param table   the table handle
     * @param branchName the branch name to drop
     */
    default void dropBranch(ConnectorSession session, ConnectorTableHandle table, String branchName) {
        throw new UnsupportedOperationException("dropBranch not supported");
    }

    /**
     * Creates a named tag (immutable snapshot reference) on the table.
     *
     * @param session the connector session
     * @param table   the table handle
     * @param tagName the tag name to create
     */
    default void createTag(ConnectorSession session, ConnectorTableHandle table, String tagName) {
        throw new UnsupportedOperationException("createTag not supported");
    }

    /**
     * Drops a named tag from the table.
     *
     * @param session the connector session
     * @param table   the table handle
     * @param tagName the tag name to drop
     */
    default void dropTag(ConnectorSession session, ConnectorTableHandle table, String tagName) {
        throw new UnsupportedOperationException("dropTag not supported");
    }

    // --- Index support (additive defaults — no migration required) ---

    /**
     * Returns the file system accessor for this connector's storage layer.
     * Used by the indexing module to read/write index files.
     *
     * @return the file system accessor, or empty if not supported
     */
    default java.util.Optional<org.pangrova.connector.api.index.FileSystemAccessor> getFileSystemAccessor() {
        return java.util.Optional.empty();
    }

    /**
     * Returns the column value reader for extracting column data from data files.
     * Used by the indexing module during index build.
     *
     * @return the column value reader, or empty if not supported
     */
    default java.util.Optional<org.pangrova.connector.api.index.ColumnValueReader> getColumnValueReader() {
        return java.util.Optional.empty();
    }

    /**
     * Returns the native index evaluator for L1 (format-level) index evaluation.
     *
     * @return the native index evaluator, or empty if not supported
     */
    default java.util.Optional<org.pangrova.connector.api.index.NativeIndexEvaluator> getNativeIndexEvaluator() {
        return java.util.Optional.empty();
    }

    /**
     * Returns native index capabilities declared by this connector.
     *
     * @return list of native index capabilities, empty if none
     */
    default java.util.List<org.pangrova.connector.api.index.NativeIndexCapability> nativeIndexCapabilities() {
        return java.util.List.of();
    }

    /**
     * Lists current data file paths for the given table.
     *
     * <p>Used by the index garbage collector to detect orphaned index
     * file mappings. Returns all active data files in a single batch
     * (not per-file HEAD requests).</p>
     *
     * @param catalogName catalog name
     * @param schemaName  schema name
     * @param tableName   table name
     * @return list of data file paths; empty if not supported
     */
    default java.util.List<String> listDataFiles(String catalogName, String schemaName, String tableName) {
        return java.util.List.of();
    }

    /**
     * Returns the consolidated index support interface for this connector.
     *
     * <p>Connectors that support Pangrova's independent indexing layer override
     * this to return their {@link org.pangrova.connector.api.index.ConnectorIndexSupport}
     * implementation, which bundles all index-related capabilities.</p>
     *
     * @return the index support, or empty if not supported
     */
    default java.util.Optional<org.pangrova.connector.api.index.ConnectorIndexSupport> getIndexSupport() {
        return java.util.Optional.empty();
    }

    /**
     * Returns the clustering capability for this connector.
     *
     * <p>Connectors that support data recluster (Iceberg RewriteDataFiles,
     * Paimon CompactAction) override this to return their implementation.</p>
     *
     * @return the clustering capability, or empty if not supported
     */
    default java.util.Optional<org.pangrova.connector.api.clustering.ClusteringCapability> getClusteringCapability() {
        return java.util.Optional.empty();
    }

    /** Binds governed clustering maintenance to the exact selected table. */
    default java.util.Optional<org.pangrova.connector.api.clustering.ClusteringMaintenance> clusteringMaintenance(
            ConnectorSession session, ConnectorTableHandle table) {
        return java.util.Optional.empty();
    }

    /**
     * Returns snapshot-aware metadata for snapshot-diff based incremental indexing
     * for the specified table.
     *
     * <p>Connectors that maintain snapshot history (Iceberg, Paimon) override this
     * to enable efficient incremental builds via file-level diffs between snapshots.</p>
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @return the snapshot-aware metadata, or empty if not supported
     */
    default java.util.Optional<org.pangrova.connector.api.index.SnapshotAwareMetadata> getSnapshotAwareMetadata(
            String schemaName, String tableName) {
        return java.util.Optional.empty();
    }

    /**
     * Returns column-level statistics for the specified data files.
     * Used for L0 partition pruning before L1 index evaluation.
     *
     * @param catalogName catalog name
     * @param schemaName  schema name
     * @param tableName   table name
     * @param dataFilePaths data file paths to get statistics for
     * @return statistics per file, empty list if not supported
     */
    default java.util.List<org.pangrova.connector.api.index.statistics.DataFileStatistics> getFileStatistics(
            String catalogName, String schemaName, String tableName,
            java.util.List<String> dataFilePaths) {
        return java.util.List.of();
    }

    // --- Column access validation (additive default — no migration required) ---

    /**
     * Validates that the current session has access to the projected columns.
     * Connectors that support column-level encryption or access control override
     * this to enforce key accessibility before split enumeration begins.
     *
     * @param session          the current session context
     * @param table            the table handle
     * @param projectedColumns column names that the query will read
     * @throws org.pangrova.common.exception.PangrovaException if access is denied
     */
    default void validateColumnAccess(ConnectorSession session, ConnectorTableHandle table,
                                      java.util.List<String> projectedColumns) {
        // no-op by default — connectors without column-level access control skip validation
    }

    // --- System table support (additive defaults — no migration required) ---

    /**
     * Declares a utility listing over a Native metadata relation. Unsupported
     * kinds remain explicit; this never authorizes a read or supplies Java rows.
     */
    default java.util.Optional<MetadataTableListing> getMetadataListing(
            ConnectorSession session, MetadataTableKind requested) {
        return java.util.Optional.empty();
    }

    /** Returns the legacy control-plane row SPI; SQL listings use Native bindings. */
    default java.util.Optional<ConnectorSystemTable> getSystemTable(
            ConnectorSession session, ConnectorTableHandle table, String systemTableName) {
        return java.util.Optional.empty();
    }

    // --- Procedure support (additive defaults — no migration required) ---

    /**
     * Executes a connector-specific procedure.
     *
     * <p>Procedures are invoked via {@code CALL catalog.procedure_name(args...)}.
     * The coordinator extracts the catalog prefix and delegates to this method.</p>
     *
     * @param session       the current session context
     * @param procedureName the procedure name (case-insensitive)
     * @param arguments     procedure arguments as string key-value pairs
     * @return result rows from the procedure execution, or empty list for void procedures
     */
    default java.util.List<org.pangrova.types.value.TypedRow> executeProcedure(
            ConnectorSession session, String procedureName,
            java.util.Map<String, String> arguments) {
        throw new UnsupportedOperationException(
                "Procedure '" + procedureName + "' is not supported by this connector");
    }

    // --- View support ---

    /**
     * Creates a view with the given name and SQL definition using an atomic conflict decision.
     * A check followed by an unconditional write does not implement CREATE or IF_NOT_EXISTS.
     * Completion makes the definition visible within the connector's declared catalog scope;
     * this contract alone does not imply durability across connector restarts.
     *
     * @param session  the current session context
     * @param viewName the schema-qualified view name
     * @param sql      the view definition SQL
     * @param mode     required atomic creation/replacement semantics
     */
    default void createView(ConnectorSession session,
                            org.pangrova.connector.api.metadata.table.SchemaTableName viewName,
                            String sql, org.pangrova.connector.api.enums.ViewCreateMode mode) {
        throw new UnsupportedOperationException("createView not supported");
    }

    /**
     * Atomically drops a view, failing if absent unless {@code ifExists} is true.
     *
     * @param session  the current session context
     * @param viewName the schema-qualified view name
     * @param ifExists tolerate an absent definition
     */
    default void dropView(ConnectorSession session,
                          org.pangrova.connector.api.metadata.table.SchemaTableName viewName, boolean ifExists) {
        throw new UnsupportedOperationException("dropView not supported");
    }

    /**
     * Lists all views in the given schema.
     *
     * @param session    the current session context
     * @param schemaName the schema name, or empty to list across all schemas
     * @return list of view names, never null
     */
    default java.util.List<org.pangrova.connector.api.metadata.table.SchemaTableName> listViews(
            ConnectorSession session, java.util.Optional<String> schemaName) {
        return java.util.List.of();
    }

    /**
     * Returns the SQL definition of a view.
     *
     * @param session  the current session context
     * @param viewName the schema-qualified view name
     * @return the view SQL definition, or empty if the view does not exist
     */
    default java.util.Optional<String> getViewDefinition(
            ConnectorSession session,
            org.pangrova.connector.api.metadata.table.SchemaTableName viewName) {
        return java.util.Optional.empty();
    }

    /**
     * Lists table procedures supported by this connector.
     *
     * @return list of table procedure descriptors, empty if none
     */
    default java.util.List<TableProcedure> listTableProcedures() {
        return java.util.List.of();
    }

    /**
     * Resolves a branch or tag reference name to a snapshot ID.
     *
     * @param session   the current session context
     * @param tableName the schema-qualified table name
     * @param refType   "BRANCH" or "TAG"
     * @param refName   the branch or tag name
     * @return the snapshot ID for the ref, or empty if unsupported or ref not found
     */
    default java.util.OptionalLong resolveRefSnapshotId(
            ConnectorSession session,
            org.pangrova.connector.api.metadata.table.SchemaTableName tableName,
            String refType, String refName) {
        return java.util.OptionalLong.empty();
    }

}
