package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.indexing.BuildMappingPersister;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.coordinator.indexing.IndexGenerationPublisher;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.metadata.exception.IndexAlreadyExistsException;
import org.pangrova.parser.ast.statement.AlterIndexStatement;
import org.pangrova.parser.ast.statement.CreateIndexStatement;
import org.pangrova.parser.ast.statement.DropIndexStatement;
import org.pangrova.parser.ast.statement.ShowIndexesStatement;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotDataFile;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Executes index DDL statements: CREATE/DROP/SHOW/ALTER INDEX.
 * Delegates non-AI data-skipping indexes to the JVM build plane and AI index
 * artifacts to the durable Native work plane.
 */
public final class IndexDdlExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDdlExecutor.class);
    private static final java.time.Duration CALLBACK_SHUTDOWN_TIMEOUT = java.time.Duration.ofSeconds(30);

    private final IndexDdlContext deps;
    private final IndexCache indexCache;
    private final AccessControl accessControl;
    private final ExecutorService callbackExecutor;

    /** Creates an index DDL executor with durable metadata and callback dependencies. */
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
        ResourceCloser.shutdownExecutor(callbackExecutor, CALLBACK_SHUTDOWN_TIMEOUT);
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

    /** Index builders read physical source columns without query-time row filters or masks. */
    private void checkRawBuildRead(QualifiedTableName tableRef, UserIdentity user, List<String> columns) {
        if (accessControl == null) {
            return;
        }
        if (user == null) {
            throw new AccessDeniedException("Anonymous source read denied for index build on " + tableRef);
        }
        accessControl.checkCanSelectFromColumns(
                user, tableRef.catalog(), tableRef.schema(), tableRef.table(), Set.copyOf(columns));
        if (accessControl.getRowFilter(user, tableRef.catalog(), tableRef.schema(), tableRef.table()).isPresent()) {
            throw new AccessDeniedException("Index build cannot bypass a source row filter on " + tableRef);
        }
        for (String column : columns) {
            if (accessControl.getColumnMask(
                    user, tableRef.catalog(), tableRef.schema(), tableRef.table(), column).isPresent()) {
                throw new AccessDeniedException("Index build cannot bypass a source column mask on " + tableRef);
            }
        }
    }

    /** Validates and submits a CREATE INDEX request. */
    public List<TypedRow> executeCreateIndex(
            CreateIndexStatement stmt, CoordinatorSessionContext context) {
        String tenantId = context.tenantId();
        UserIdentity user = context.userIdentity();
        requireTenantId(tenantId);
        QualifiedTableName tableRef = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        checkAccess(tableRef, user, true);
        var session = TableResolver.buildSession(context, tableRef.catalog(), tableRef.schema());
        var accessors = deps.accessorResolver().forSession(session);
        IndexValidationHelper.validateTableExists(tableRef, deps.catalogRegistry(), session);
        IndexType indexType = parseIndexType(stmt.indexType());
        List<String> columns = IndexValidationHelper.validateColumns(
                tableRef, stmt.columnNames(), indexType, deps.catalogRegistry(), session);
        checkRawBuildRead(tableRef, user, columns);
        IndexValidationHelper.validateMultiColumnConstraints(indexType, columns);
        IndexValidationHelper.checkNativeCapabilityOverlap(
                tableRef.catalog(), columns, indexType, stmt.force(), accessors);

        boolean nativeArtifact = requiresNativeArtifact(indexType);
        IndexBuildInput buildInput = resolveBuildInput(tableRef, !nativeArtifact, context);
        IndexDescriptor descriptor = buildDescriptor(
                stmt.indexName(), tableRef, columns, indexType,
                IndexBuildProperties.canonicalize(indexType, stmt.properties()),
                buildInput.sourceSnapshot(), tenantId);
        requireNativeBuildSupport(descriptor);

        List<TypedRow> earlyResult = registerIndexOrSkip(descriptor, stmt, user);
        if (earlyResult != null) {
            return earlyResult;
        }
        submitAsyncBuild(descriptor, tableRef, buildInput.dataFiles(), context);
        LOG.info("action=create_index indexId={} index={} table={} type={}",
                descriptor.indexId(), stmt.indexName(), tableRef, indexType);
        return buildCreateResult(descriptor.indexId(), IndexState.BUILDING);
    }

    private List<TypedRow> registerIndexOrSkip(
            IndexDescriptor descriptor, CreateIndexStatement stmt, UserIdentity user) {
        try {
            if (requiresNativeArtifact(descriptor.indexType())) {
                deps.indexMetadataStore().registerIndex(descriptor);
            } else {
                if (user == null || user.principal() == null || user.principal().isBlank()) {
                    throw new AccessDeniedException("Scalar index build requires an authenticated subject");
                }
                deps.indexMetadataStore().registerIndexWithBuildAuthority(descriptor, user.principal());
            }
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

    /** Drops the named index and schedules cleanup of its artifacts. */
    public List<TypedRow> executeDropIndex(
            DropIndexStatement stmt, CoordinatorSessionContext context) {
        String tenantId = context.tenantId();
        UserIdentity user = context.userIdentity();
        requireTenantId(tenantId);
        QualifiedTableName tableRef = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        checkAccess(tableRef, user, true);
        IndexDescriptor target = findActiveIndex(stmt.indexName(), tableRef, tenantId);

        if (target == null && stmt.ifExists()) {
            return List.of();
        }
        if (target == null) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_NOT_FOUND,
                    "Index '" + stmt.indexName() + "' not found on table " + tableRef);
        }

        performDrop(target, tableRef, stmt.indexName(), tenantId);
        return List.of();
    }

    private IndexDescriptor findActiveIndex(
            String indexName, QualifiedTableName tableRef, String tenantId) {
        return deps.indexMetadataStore()
                .findIndexesByTable(
                        tenantId, tableRef.catalog(), tableRef.schema(), tableRef.table())
                .stream()
                .filter(d -> d.indexName().equals(indexName))
                .filter(d -> d.state() != IndexState.DROPPED)
                .findFirst()
                .orElse(null);
    }

    private void performDrop(
            IndexDescriptor target, QualifiedTableName tableRef,
            String indexName, String tenantId) {
        // Drop ALL generations of this index (not just the first found)
        List<IndexDescriptor> allGenerations = deps.indexMetadataStore()
                .findIndexesByTable(
                        tenantId, tableRef.catalog(), tableRef.schema(), tableRef.table())
                .stream()
                .filter(d -> d.indexName().equals(indexName))
                .filter(d -> d.state() != IndexState.DROPPED)
                .toList();

        for (IndexDescriptor descriptor : allGenerations) {
            deps.buildServices().dataSkipping().cancelBuild(descriptor.indexId());
            if (deps.buildServices().aiNative() != null) {
                deps.buildServices().aiNative().cancel(
                        descriptor.indexId(), tenantId, Instant.now());
            }
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

    /** Returns visible index descriptors for the requested table. */
    public List<TypedRow> executeShowIndexes(
            ShowIndexesStatement stmt, CoordinatorSessionContext context) {
        String tenantId = context.tenantId();
        UserIdentity user = context.userIdentity();
        requireTenantId(tenantId);
        QualifiedTableName tableRef = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        checkAccess(tableRef, user, false);
        List<IndexDescriptor> indexes = deps.indexMetadataStore()
                .findIndexesByTable(
                        tenantId, tableRef.catalog(), tableRef.schema(), tableRef.table());

        return indexes.stream()
                .filter(d -> d.state() != IndexState.DROPPED)
                .map(IndexDdlExecutor::descriptorToRow)
                .toList();
    }

    /** Submits a rebuild for an existing index. */
    public List<TypedRow> executeAlterIndex(
            AlterIndexStatement stmt, CoordinatorSessionContext context) {
        String tenantId = context.tenantId();
        UserIdentity user = context.userIdentity();
        requireTenantId(tenantId);
        QualifiedTableName tableRef = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        checkAccess(tableRef, user, true);
        IndexDescriptor target = findRebuildableIndex(
                tableRef, stmt.indexName(), tenantId);
        var session = TableResolver.buildSession(context, tableRef.catalog(), tableRef.schema());
        IndexValidationHelper.validateTableExists(tableRef, deps.catalogRegistry(), session);
        List<String> columns = IndexValidationHelper.validateColumns(
                tableRef, target.columnNames(), target.indexType(), deps.catalogRegistry(), session);
        checkRawBuildRead(tableRef, user, columns);

        boolean nativeArtifact = requiresNativeArtifact(target.indexType());
        IndexBuildInput buildInput = resolveBuildInput(tableRef, !nativeArtifact, context);
        IndexDescriptor rebuilt = prepareRebuild(target, buildInput.sourceSnapshot());
        requireNativeBuildSupport(rebuilt);
        submitAsyncBuild(rebuilt, tableRef, buildInput.dataFiles(), context);
        emitRebuildCacheEvent(target, tableRef);
        LOG.info("action=alter_index_rebuild indexId={} index={} generation={}",
                target.indexId(), stmt.indexName(), rebuilt.generation());
        return buildCreateResult(target.indexId(), IndexState.BUILDING);
    }

    private IndexDescriptor findRebuildableIndex(
            QualifiedTableName tableRef, String indexName, String tenantId) {
        List<IndexDescriptor> indexes = deps.indexMetadataStore()
                .findIndexesByTable(
                        tenantId, tableRef.catalog(), tableRef.schema(), tableRef.table());
        IndexDescriptor target = indexes.stream()
                .filter(d -> d.indexName().equals(indexName))
                .filter(d -> d.state() != IndexState.DROPPED)
                .findFirst()
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_INDEX_NOT_FOUND,
                        "Index '" + indexName + "' not found on table " + tableRef));
        return target;
    }

    private IndexDescriptor prepareRebuild(
            IndexDescriptor target, String sourceSnapshot) {
        // Retry with fresh read on optimistic lock failure. CREATE INDEX completes
        // asynchronously and may update updatedAt between findRebuildableIndex()
        // and the CAS below — a benign race that we resolve by re-reading.
        IndexDescriptor current = target;
        String tenantId = target.tenantId();
        for (int attempt = 0; attempt < 3; attempt++) {
            Instant now = Instant.now();
            int newGeneration = current.generation() + 1;
            IndexDescriptor rebuilt = current.withGeneration(newGeneration, now)
                    .withBuildSnapshotId(sourceSnapshot, now)
                    .withState(IndexState.BUILDING, now);
            boolean updated = deps.indexMetadataStore().beginIndexGeneration(
                    current.indexId(), newGeneration, sourceSnapshot,
                    now, current.updatedAt(), current.state(), current.generation());
            if (updated) {
                return rebuilt;
            }
            LOG.debug("action=rebuild_retry attempt={} indexId={}", attempt + 1, current.indexId());
            // Re-read fresh descriptor for next attempt
            String indexId = current.indexId();
            String indexName = current.indexName();
            List<IndexDescriptor> indexes = deps.indexMetadataStore()
                    .findIndexesByTable(
                            tenantId, current.catalogName(), current.schemaName(), current.tableName());
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

    private void submitAsyncBuild(
            IndexDescriptor descriptor,
            QualifiedTableName tableRef,
            List<org.pangrova.indexing.build.DataFileInfo> dataFiles,
            CoordinatorSessionContext context) {
        var accessors = deps.accessorResolver().forSession(
                TableResolver.buildSession(context, tableRef.catalog(), tableRef.schema()));
        try {
            if (requiresNativeArtifact(descriptor.indexType())) {
                DurableIndexBuildService nativeBuild = deps.buildServices().aiNative();
                if (nativeBuild == null || !DurableIndexBuildService.supports(descriptor)) {
                    throw new IllegalStateException(
                            "AI Native index capability is unavailable for "
                                    + descriptor.indexType() + "/"
                                    + descriptor.properties().getOrDefault(
                                            "algorithm", "default"));
                }
                String jobId = nativeBuild.submit(descriptor, tableRef, context);
                LOG.info("event=native_index_build_queued indexId={} generation={} jobId={}",
                        descriptor.indexId(), descriptor.generation(), jobId);
                return;
            }
            if (dataFiles.isEmpty()) {
                // An empty generation still depends on its source snapshot. Route it
                // through the same pin-and-publication barrier as a nonempty build.
                registerBuildCompletionHandler(descriptor,
                        deps.buildServices().dataSkipping().submitBuildTask(
                                descriptor, dataFiles, null, null));
                return;
            }
            var fsAccessor = accessors.resolve(tableRef.catalog());
            var columnReader = accessors.resolveColumnValueReader(tableRef.catalog());

            java.util.function.Consumer<List<org.pangrova.indexing.build.BuildFileMapping>> mappingConsumer =
                    batch -> BuildMappingPersister.persist(
                            deps.indexMetadataStore(), batch, descriptor.generation(),
                            descriptor.buildSnapshotId());

            Future<?> future = deps.buildServices().dataSkipping().submitBuildTask(
                    descriptor, dataFiles, fsAccessor, columnReader, mappingConsumer);
            registerBuildCompletionHandler(descriptor, future);
        } catch (Exception e) {
            recordBuildFailure(descriptor, e);
            throw new PangrovaException(
                    PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Index build could not be submitted for '" + descriptor.indexName() + "'",
                    e);
        }
    }

    /**
     * Registers a completion handler that updates index state after build finishes.
     * Implements DROP-before-READY guard: if the index was dropped during build,
     * the state update is skipped. Uses optimistic locking to prevent concurrent
     * state corruption in multi-coordinator deployments.
     */
    private void registerBuildCompletionHandler(IndexDescriptor descriptor, Future<?> future) {
        callbackExecutor.submit(() -> handleBuildCompletion(descriptor, future));
    }

    private void handleBuildCompletion(IndexDescriptor descriptor, Future<?> future) {
        String indexId = descriptor.indexId();
        try {
            var result = (org.pangrova.indexing.build.IndexBuildResult) future.get();
            processBuildResult(descriptor, result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            recordBuildFailure(descriptor, e);
        } catch (java.util.concurrent.CancellationException e) {
            LOG.info("event=build_cancelled indexId={}", indexId);
            recordBuildFailure(descriptor, e);
        } catch (Exception e) {
            LOG.warn("event=build_completion_handler_error indexId={} error={}",
                    indexId, e.getMessage());
            recordBuildFailure(descriptor, e);
        }
    }

    private void processBuildResult(IndexDescriptor building,
                                    org.pangrova.indexing.build.IndexBuildResult result) {
        String indexId = building.indexId();
        int expectedGeneration = building.generation();
        Optional<IndexDescriptor> current = deps.indexMetadataStore().findIndex(indexId);
        if (current.isEmpty() || current.get().state() == IndexState.DROPPED) {
            LOG.info("event=build_completed_index_dropped indexId={}", indexId);
            return;
        }
        boolean initialBuild = current.get().state() == IndexState.BUILDING
                && current.get().generation() == expectedGeneration;
        boolean onlineBuild = (current.get().state() == IndexState.READY
                || current.get().state() == IndexState.STALE)
                && current.get().generation() == expectedGeneration - 1;
        if (!initialBuild && !onlineBuild) {
            LOG.info("event=build_generation_mismatch indexId={} expected={} actual={}",
                    indexId, expectedGeneration, current.get().generation());
            return;
        }
        IndexState newState;
        if (result.isFullySuccessful()) {
            IndexGenerationPublisher.publish(
                    deps.indexMetadataStore(), building, result.successCount());
            newState = IndexState.READY;
        } else {
            newState = IndexState.STALE;
            recordBuildFailure(building, new IllegalStateException(
                    "Incomplete index build: " + result));
        }
        LOG.info("event=build_completed indexId={} generation={} success={} failed={} state={}",
                indexId, expectedGeneration, result.successCount(),
                result.failureCount(), newState);
    }

    private void recordBuildFailure(IndexDescriptor building, Exception failure) {
        try {
            String reason = failure.getMessage() == null
                    ? "build_error" : failure.getMessage();
            boolean onlineFailed = deps.indexMetadataStore().failIndexGeneration(
                    building.indexId(), building.generation(), building.updatedAt(),
                    Instant.now(), reason);
            if (onlineFailed) {
                return;
            }
            Optional<IndexDescriptor> current =
                    deps.indexMetadataStore().findIndex(building.indexId());
            if (current.isPresent()
                    && current.get().state() == IndexState.BUILDING
                    && current.get().generation() == building.generation()) {
                deps.indexMetadataStore().updateIndexState(
                        building.indexId(), IndexState.STALE.name(), Instant.now(), reason);
            }
        } catch (Exception stateFailure) {
            LOG.warn("event=build_error_state_update_failed indexId={} error={}",
                    building.indexId(), stateFailure.getMessage());
        }
    }

    private static IndexDescriptor buildDescriptor(
            String indexName, QualifiedTableName ref,
            List<String> columns, IndexType indexType,
            Map<String, String> properties, String sourceSnapshot,
            String tenantId) {
        Instant now = Instant.now();
        return new IndexDescriptor(
                UUID.randomUUID().toString(), indexName,
                ref.catalog(), ref.schema(), ref.table(),
                columns, indexType, properties,
                IndexState.BUILDING, null, 0, sourceSnapshot, null, now, now, tenantId);
    }

    private static void requireTenantId(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new PangrovaException(
                    PangrovaErrorCode.ACCESS_DENIED,
                    "Tenant identity is required for index DDL");
        }
    }

    static boolean requiresNativeArtifact(IndexType indexType) {
        return indexType == IndexType.VECTOR || indexType == IndexType.INVERTED;
    }

    private void requireNativeBuildSupport(IndexDescriptor descriptor) {
        if (!requiresNativeArtifact(descriptor.indexType())) {
            return;
        }
        DurableIndexBuildService nativeBuild = deps.buildServices().aiNative();
        if (nativeBuild == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "AI Native index build service is not configured");
        }
        if (!DurableIndexBuildService.supports(descriptor)) {
            throw new PangrovaException(
                    PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "AI Native index algorithm is not implemented: "
                            + descriptor.properties().getOrDefault(
                                    "algorithm", "default"));
        }
    }

    private IndexBuildInput resolveBuildInput(
            QualifiedTableName tableRef, boolean materializeDataFiles, CoordinatorSessionContext context) {
        SnapshotAwareMetadata metadata = deps.accessorResolver().forSession(
                TableResolver.buildSession(context, tableRef.catalog(), tableRef.schema()))
                .resolveSnapshotAwareMetadata(
                        tableRef.catalog(), tableRef.schema(), tableRef.table());
        if (metadata == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Index builds require immutable snapshot metadata for " + tableRef);
        }
        Optional<String> snapshot = metadata.currentSnapshotId();
        if (snapshot.isEmpty()) {
            return new IndexBuildInput(
                    materializeDataFiles ? ""
                            : org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner
                                    .EMPTY_SNAPSHOT,
                    List.of());
        }
        if (!materializeDataFiles) {
            return new IndexBuildInput(snapshot.get(), List.of());
        }
        List<org.pangrova.indexing.build.DataFileInfo> files = metadata
                .listFilesAtSnapshot(snapshot.get()).stream()
                .map(IndexDdlExecutor::toDataFileInfo)
                .toList();
        return new IndexBuildInput(snapshot.get(), files);
    }

    private static org.pangrova.indexing.build.DataFileInfo toDataFileInfo(
            SnapshotDataFile file) {
        return new org.pangrova.indexing.build.DataFileInfo(
                file.path(), file.rowCount(), file.sizeBytes(), file.format());
    }

    private record IndexBuildInput(
            String sourceSnapshot,
            List<org.pangrova.indexing.build.DataFileInfo> dataFiles) {

        private IndexBuildInput {
            sourceSnapshot = sourceSnapshot == null ? "" : sourceSnapshot;
            dataFiles = List.copyOf(dataFiles);
        }
    }

    private static IndexType parseIndexType(String typeStr) {
        try {
            return IndexType.valueOf(typeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Unknown index type: " + typeStr);
        }
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
