package org.pangrova.coordinator.index.ddl;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.index.BuildMappingPersister;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.metadata.exception.IndexAlreadyExistsException;
import org.pangrova.parser.ast.statement.AlterIndexStatement;
import org.pangrova.parser.ast.statement.CreateIndexStatement;
import org.pangrova.parser.ast.statement.DropIndexStatement;
import org.pangrova.parser.ast.statement.ShowIndexesStatement;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Executes index DDL statements: CREATE/DROP/SHOW/ALTER INDEX.
 * Delegates metadata persistence to IndexMetadataStore and async build
 * to IndexBuildCoordinator.
 */
public final class IndexDdlExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDdlExecutor.class);

    private final IndexDdlContext deps;
    private final IndexCache indexCache;
    private final AccessControl accessControl;
    private final ExecutorService callbackExecutor;

    public IndexDdlExecutor(IndexDdlContext deps, IndexCache indexCache,
                     AccessControl accessControl) {
        this.deps = deps;
        this.indexCache = indexCache;
        this.accessControl = accessControl;
        this.callbackExecutor = org.pangrova.common.concurrent.ManagedExecutorFactory
                .newFixedThreadPool("index-build-callback", 4);
    }

    @Override
    public void close() {
        ResourceCloser.shutdownExecutor(callbackExecutor, java.time.Duration.ofSeconds(30));
    }

    /**
     * Checks access control for index DDL operations.
     * CREATE/DROP/ALTER INDEX → ALTER permission on the target table.
     * SHOW INDEXES → SELECT permission on the target table.
     *
     * @param tableRef the target table
     * @param user     the user identity; null skips the check
     * @param requireAlter true for CREATE/DROP/ALTER, false for SHOW (SELECT)
     */
    private void checkAccess(QualifiedTableName tableRef, UserIdentity user, boolean requireAlter) {
        if (accessControl == null) {
            return;
        }
        if (user == null) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "Anonymous access denied for index DDL on " + tableRef);
        }
        if (requireAlter) {
            accessControl.checkCanAlterTable(
                    user, tableRef.catalog(), tableRef.schema(), tableRef.table());
        } else {
            accessControl.checkCanSelectFromTable(
                    user, tableRef.catalog(), tableRef.schema(), tableRef.table());
        }
    }

    public List<TypedRow> executeCreateIndex(CreateIndexStatement stmt) {
        return executeCreateIndex(stmt, null);
    }

    public List<TypedRow> executeCreateIndex(CreateIndexStatement stmt, UserIdentity user) {
        QualifiedTableName tableRef = resolveTableRef(stmt.tableName().parts());
        checkAccess(tableRef, user, true);
        IndexValidationHelper.validateTableExists(tableRef, deps.catalogRegistry());
        IndexType indexType = parseIndexType(stmt.indexType());
        IndexValidationHelper.validateColumns(tableRef, stmt.columnNames(), indexType, deps.catalogRegistry());
        IndexValidationHelper.validateMultiColumnConstraints(indexType, stmt.columnNames());
        IndexValidationHelper.checkNativeCapabilityOverlap(
                tableRef.catalog(), stmt.columnNames(), indexType, stmt.force(), deps.accessorResolver());

        IndexDescriptor descriptor = buildDescriptor(
                stmt.indexName(), tableRef, stmt.columnNames(), indexType, stmt.properties());

        List<TypedRow> earlyResult = registerIndexOrSkip(descriptor, stmt);
        if (earlyResult != null) {
            return earlyResult;
        }
        submitAsyncBuild(descriptor, tableRef);
        LOG.info("action=create_index indexId={} index={} table={} type={}",
                descriptor.indexId(), stmt.indexName(), tableRef, indexType);
        return buildCreateResult(descriptor.indexId(), IndexState.BUILDING);
    }

    private List<TypedRow> registerIndexOrSkip(IndexDescriptor descriptor, CreateIndexStatement stmt) {
        try {
            deps.indexMetadataStore().registerIndex(descriptor);
            return null;
        } catch (IndexAlreadyExistsException e) {
            if (stmt.ifNotExists()) {
                LOG.info("action=create_index_skip_exists index={} table={}",
                        stmt.indexName(), descriptor.tableName());
                return buildCreateResult(descriptor.indexId(), IndexState.BUILDING);
            }
            throw e;
        }
    }

    public List<TypedRow> executeDropIndex(DropIndexStatement stmt) {
        return executeDropIndex(stmt, null);
    }

    public List<TypedRow> executeDropIndex(DropIndexStatement stmt, UserIdentity user) {
        QualifiedTableName tableRef = resolveTableRef(stmt.tableName().parts());
        checkAccess(tableRef, user, true);
        IndexDescriptor target = findActiveIndex(stmt.indexName(), tableRef);

        if (target == null && stmt.ifExists()) {
            return List.of();
        }
        if (target == null) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_NOT_FOUND,
                    "Index '" + stmt.indexName() + "' not found on table " + tableRef);
        }

        performDrop(target, tableRef, stmt.indexName());
        return List.of();
    }

    private IndexDescriptor findActiveIndex(String indexName, QualifiedTableName tableRef) {
        return deps.indexMetadataStore()
                .findIndexesByTable(tableRef.catalog(), tableRef.schema(), tableRef.table())
                .stream()
                .filter(d -> d.indexName().equals(indexName))
                .filter(d -> d.state() != IndexState.DROPPED)
                .findFirst()
                .orElse(null);
    }

    private void performDrop(IndexDescriptor target, QualifiedTableName tableRef, String indexName) {
        // Drop ALL generations of this index (not just the first found)
        List<IndexDescriptor> allGenerations = deps.indexMetadataStore()
                .findIndexesByTable(tableRef.catalog(), tableRef.schema(), tableRef.table())
                .stream()
                .filter(d -> d.indexName().equals(indexName))
                .filter(d -> d.state() != IndexState.DROPPED)
                .toList();

        for (IndexDescriptor descriptor : allGenerations) {
            deps.buildCoordinator().cancelBuild(descriptor.indexId());
            deps.indexMetadataStore().dropIndex(descriptor.indexId(), Instant.now());
            if (indexCache != null) {
                indexCache.invalidateIndex(descriptor.indexId());
            }
        }
        emitCacheEvent("DROP", target.indexId(), tableRef);
        LOG.info("action=drop_index indexId={} index={} table={} generationsDropped={}",
                target.indexId(), indexName, tableRef, allGenerations.size());
    }

    private void emitCacheEvent(String eventType, String indexId, QualifiedTableName tableRef) {
        try {
            deps.indexMetadataStore().insertCacheEvent(eventType, indexId,
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), Instant.now());
        } catch (Exception e) {
            LOG.warn("action=cache_event_insert_failed eventType={} indexId={} error={}",
                    eventType, indexId, e.getMessage());
        }
    }

    public List<TypedRow> executeShowIndexes(ShowIndexesStatement stmt) {
        return executeShowIndexes(stmt, null);
    }

    public List<TypedRow> executeShowIndexes(ShowIndexesStatement stmt, UserIdentity user) {
        QualifiedTableName tableRef = resolveTableRef(stmt.tableName().parts());
        checkAccess(tableRef, user, false);
        List<IndexDescriptor> indexes = deps.indexMetadataStore()
                .findIndexesByTable(tableRef.catalog(), tableRef.schema(), tableRef.table());

        return indexes.stream()
                .filter(d -> d.state() != IndexState.DROPPED)
                .map(IndexDdlExecutor::descriptorToRow)
                .toList();
    }

    public List<TypedRow> executeAlterIndex(AlterIndexStatement stmt) {
        return executeAlterIndex(stmt, null);
    }

    public List<TypedRow> executeAlterIndex(AlterIndexStatement stmt, UserIdentity user) {
        QualifiedTableName tableRef = resolveTableRef(stmt.tableName().parts());
        checkAccess(tableRef, user, true);
        IndexDescriptor target = findRebuildableIndex(tableRef, stmt.indexName());

        IndexDescriptor rebuilt = prepareRebuild(target);
        submitAsyncBuild(rebuilt, tableRef);
        emitRebuildCacheEvent(target, tableRef);
        LOG.info("action=alter_index_rebuild indexId={} index={} generation={}",
                target.indexId(), stmt.indexName(), rebuilt.generation());
        return buildCreateResult(target.indexId(), IndexState.BUILDING);
    }

    private IndexDescriptor findRebuildableIndex(QualifiedTableName tableRef, String indexName) {
        List<IndexDescriptor> indexes = deps.indexMetadataStore()
                .findIndexesByTable(tableRef.catalog(), tableRef.schema(), tableRef.table());
        IndexDescriptor target = indexes.stream()
                .filter(d -> d.indexName().equals(indexName))
                .filter(d -> d.state() != IndexState.DROPPED)
                .findFirst()
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_INDEX_NOT_FOUND,
                        "Index '" + indexName + "' not found on table " + tableRef));
        return target;
    }

    private IndexDescriptor prepareRebuild(IndexDescriptor target) {
        // Retry with fresh read on optimistic lock failure. CREATE INDEX completes
        // asynchronously and may update updatedAt between findRebuildableIndex()
        // and the CAS below — a benign race that we resolve by re-reading.
        IndexDescriptor current = target;
        for (int attempt = 0; attempt < 3; attempt++) {
            Instant now = Instant.now();
            int newGeneration = current.generation() + 1;
            IndexDescriptor rebuilt = current.withGeneration(newGeneration, now)
                    .withState(IndexState.BUILDING, now);
            boolean updated = deps.indexMetadataStore().updateIndexState(
                    current.indexId(), IndexState.BUILDING.name(), now, current.updatedAt());
            if (updated) {
                return rebuilt;
            }
            LOG.debug("action=rebuild_retry attempt={} indexId={}", attempt + 1, current.indexId());
            // Re-read fresh descriptor for next attempt
            String indexId = current.indexId();
            String indexName = current.indexName();
            List<IndexDescriptor> indexes = deps.indexMetadataStore()
                    .findIndexesByTable(current.catalogName(), current.schemaName(), current.tableName());
            current = indexes.stream()
                    .filter(d -> d.indexId().equals(indexId))
                    .filter(d -> d.state() != IndexState.DROPPED)
                    .findFirst()
                    .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_INDEX_NOT_FOUND,
                            "Index '" + indexName + "' was dropped during rebuild"));
        }
        throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                "ALTER INDEX REBUILD failed: concurrent modification on index '"
                        + current.indexName() + "'");
    }

    private void emitRebuildCacheEvent(IndexDescriptor target, QualifiedTableName tableRef) {
        try {
            deps.indexMetadataStore().insertCacheEvent("REBUILD", target.indexId(),
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), Instant.now());
        } catch (Exception e) {
            LOG.warn("action=cache_event_insert_failed eventType=REBUILD indexId={} error={}",
                    target.indexId(), e.getMessage());
        }
    }

    // --- Private helpers ---

    private void submitAsyncBuild(IndexDescriptor descriptor, QualifiedTableName tableRef) {
        try {
            var fsAccessor = deps.accessorResolver().resolve(tableRef.catalog());
            var columnReader = deps.accessorResolver().resolveColumnValueReader(tableRef.catalog());
            List<org.pangrova.indexing.build.DataFileInfo> dataFiles = resolveDataFiles(tableRef);

            if (dataFiles.isEmpty()) {
                markIndexReadyForEmptyTable(descriptor, tableRef);
                return;
            }

            java.util.function.Consumer<List<org.pangrova.indexing.build.BuildFileMapping>> mappingConsumer =
                    batch -> BuildMappingPersister.persist(
                            deps.indexMetadataStore(), batch, descriptor.generation());

            Future<?> future = deps.buildCoordinator().submitBuildTask(
                    descriptor, dataFiles, fsAccessor, columnReader, mappingConsumer);
            registerBuildCompletionHandler(
                    descriptor.indexId(), descriptor.generation(), future);
        } catch (Exception e) {
            LOG.warn("action=async_build_submit_failed indexId={} error={}",
                    descriptor.indexId(), e.getMessage());
        }
    }

    private List<org.pangrova.indexing.build.DataFileInfo> resolveDataFiles(QualifiedTableName tableRef) {
        List<String> dataFilePaths = deps.accessorResolver()
                .listDataFiles(tableRef.catalog(), tableRef.schema(), tableRef.table());
        return dataFilePaths.stream()
                .map(f -> new org.pangrova.indexing.build.DataFileInfo(f, 0, 0, "unknown"))
                .toList();
    }

    private void markIndexReadyForEmptyTable(IndexDescriptor descriptor, QualifiedTableName tableRef) {
        deps.indexMetadataStore().updateIndexState(
                descriptor.indexId(), IndexState.READY.name(), Instant.now());
        LOG.info("action=create_index_empty_table indexId={} table={}",
                descriptor.indexId(), tableRef);
    }

    /**
     * Registers a completion handler that updates index state after build finishes.
     * Implements DROP-before-READY guard: if the index was dropped during build,
     * the state update is skipped. Uses optimistic locking to prevent concurrent
     * state corruption in multi-coordinator deployments.
     */
    private void registerBuildCompletionHandler(
            String indexId, int expectedGeneration, Future<?> future) {
        callbackExecutor.submit(() -> handleBuildCompletion(indexId, expectedGeneration, future));
    }

    private void handleBuildCompletion(String indexId, int expectedGeneration, Future<?> future) {
        try {
            var result = (org.pangrova.indexing.build.IndexBuildResult) future.get();
            processBuildResult(indexId, expectedGeneration, result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.CancellationException e) {
            LOG.info("event=build_cancelled indexId={}", indexId);
        } catch (Exception e) {
            LOG.warn("event=build_completion_handler_error indexId={} error={}",
                    indexId, e.getMessage());
            try {
                String reason = e.getMessage() != null
                        ? e.getMessage().substring(0, Math.min(e.getMessage().length(), 200))
                        : "build_error";
                deps.indexMetadataStore().updateIndexState(
                        indexId, IndexState.STALE.name(), Instant.now(), reason);
            } catch (Exception stateErr) {
                LOG.warn("event=build_error_state_update_failed indexId={} error={}",
                        indexId, stateErr.getMessage());
            }
        }
    }

    private void processBuildResult(String indexId, int expectedGeneration,
                                    org.pangrova.indexing.build.IndexBuildResult result) {
        Optional<IndexDescriptor> current = deps.indexMetadataStore().findIndex(indexId);
        if (current.isEmpty() || current.get().state() == IndexState.DROPPED) {
            LOG.info("event=build_completed_index_dropped indexId={}", indexId);
            return;
        }
        if (current.get().generation() != expectedGeneration) {
            LOG.info("event=build_generation_mismatch indexId={} expected={} actual={}",
                    indexId, expectedGeneration, current.get().generation());
            return;
        }
        IndexState newState = result.isFullySuccessful() ? IndexState.READY : IndexState.STALE;
        boolean updated = deps.indexMetadataStore().updateIndexState(
                indexId, newState.name(), Instant.now(), current.get().updatedAt());
        if (!updated) {
            LOG.info("event=build_completion_concurrent_update indexId={}", indexId);
        }
        LOG.info("event=build_completed indexId={} generation={} success={} failed={} state={}",
                indexId, expectedGeneration, result.successCount(),
                result.failureCount(), newState);
    }

    private static IndexDescriptor buildDescriptor(
            String indexName, QualifiedTableName ref,
            List<String> columns, IndexType indexType, Map<String, String> properties) {
        Instant now = Instant.now();
        return new IndexDescriptor(
                UUID.randomUUID().toString(), indexName,
                ref.catalog(), ref.schema(), ref.table(),
                columns, indexType, properties,
                IndexState.BUILDING, null, 0, null, null, now, now, null);
    }

    private static IndexType parseIndexType(String typeStr) {
        try {
            return IndexType.valueOf(typeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Unknown index type: " + typeStr);
        }
    }

    private static QualifiedTableName resolveTableRef(List<String> parts) {
        return QualifiedTableName.resolve(parts);
    }

    private static List<TypedRow> buildCreateResult(String indexId, IndexState state) {
        return List.of(TypedRow.builder()
                .putString("index_id", indexId)
                .putString("state", state.name())
                .build());
    }

    private static TypedRow descriptorToRow(IndexDescriptor d) {
        var builder = TypedRow.builder()
                .putString("index_name", d.indexName())
                .putString("index_type", d.indexType().name())
                .putString("columns", String.join(", ", d.columnNames()))
                .putString("state", d.state().name())
                .putString("stale_reason", d.staleReason())
                .putString("created_at", d.createdAt() != null ? d.createdAt().toString() : "");
        // VECTOR-specific metadata in SHOW INDEXES output
        if (d.indexType() == IndexType.VECTOR) {
            builder.putString("algorithm", d.properties().getOrDefault("algorithm", ""));
            builder.putString("metric", d.properties().getOrDefault("metric", ""));
        }
        return builder.build();
    }
}
