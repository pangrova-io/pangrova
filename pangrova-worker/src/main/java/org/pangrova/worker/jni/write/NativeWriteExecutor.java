package org.pangrova.worker.jni.write;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.worker.exchange.transport.PageDecoder;
import org.pangrova.worker.jni.NativeRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Adapts {@link NativeWriter} calls to {@link ConnectorPageSink} SPI.
 *
 * <p>Native workers call this adapter via JNI to write data through Java
 * connector page sink implementations. Each write session is keyed by an
 * opaque handle and backed by a {@link ConnectorPageSink} resolved from
 * the catalog name in the {@link WriteDescriptor}.</p>
 *
 * <p>Thread safety: uses {@link ConcurrentHashMap} for active sessions and
 * {@link AtomicLong} for handle generation. Individual sessions are accessed
 * by a single native thread (the native operator thread that owns the handle),
 * so no per-session synchronization is needed.</p>
 */
@ThreadSafe
public final class NativeWriteExecutor implements NativeWriter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeWriteExecutor.class);

    /**
     * Stores commit metadata produced by commitWrite(), keyed by taskId.
     * TaskStatusCollector reads this via drainCommitMetadataForTask(taskId)
     * when building the FINISHED status report for write tasks.
     *
     * <p>Per-task isolation: the taskId is resolved from WriteDescriptor.properties
     * (key "taskId") at beginWrite() time. When the native side does not pass
     * a taskId (legacy path), a synthetic key derived from the write handle is
     * used and the global fallback drain still works.</p>
     */
    private final ConcurrentHashMap<String, byte[]> pendingCommitMetadata =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long, String> handleToTaskId = new ConcurrentHashMap<>();

    /**
     * Aggregate INFO log interval for per-batch write progress.
     * Emits one INFO log every N batches to avoid log flooding while
     * still providing visibility into long-running write tasks.
     */
    static final int BATCH_LOG_INTERVAL = 1000;

    /** Nanoseconds per millisecond, for elapsed-time conversion. */
    private static final long NANOS_PER_MILLI = 1_000_000;

    private final CatalogResourceResolver catalogLookup;
    private final ConcurrentHashMap<Long, WriteSession> activeSessions = new ConcurrentHashMap<>();
    private final AtomicLong handleCounter = new AtomicLong();
    /** Shared allocator for columnar IPC decoding, reused across batches to avoid
     *  per-batch RootAllocator creation/destruction overhead and GC pressure. */
    private final BufferAllocator sharedAllocator = new RootAllocator(64L * 1024 * 1024);

    private record WriteSession(
            ConnectorPageSink sink,
            List<String> columnNames,
            long startTimeNanos,
            String catalogName,
            AtomicLong rowsAppended,
            AtomicLong batchCount) {}

    /**
     * Creates a new adapter with the given catalog lookup function.
     *
     * @param catalogLookup resolves catalog names to connector resources
     */
    public NativeWriteExecutor(CatalogResourceResolver catalogLookup) {
        this.catalogLookup = checkNotNull(catalogLookup, "catalogLookup");
    }

    @Override
    public long beginWrite(WriteDescriptor descriptor) {
        checkNotNull(descriptor, "descriptor");
        String catalogName = descriptor.catalogName();
        LOG.info("event=begin_write catalog={} schema={} table={} mode={}",
                catalogName, descriptor.schemaName(), descriptor.tableName(),
                descriptor.dmlOperation());

        ConnectorPageSink sink = createSinkForDescriptor(descriptor);
        // Resolve column names from table metadata instead of native RowVector names.
        // The native engine's ProjectNode generates synthetic names (proj_0, proj_1, ...) for
        // computed expressions (e.g., CASE WHEN in UPDATE). The table schema has
        // the correct column names that Iceberg expects for writing.
        List<String> columnNames = resolveWriteColumnNames(descriptor);

        long handle = handleCounter.incrementAndGet();
        String taskId = descriptor.properties().getOrDefault("taskId", "");
        if (taskId.isEmpty()) {
            LOG.warn("event=missing_task_id_in_descriptor handle={} catalog={} table={}.{} "
                    + "using_fallback=true",
                    handle, catalogName, descriptor.schemaName(), descriptor.tableName());
            taskId = "__handle_" + handle;
        }
        handleToTaskId.put(handle, taskId);
        activeSessions.put(handle, new WriteSession(
                sink, columnNames, System.nanoTime(),
                catalogName, new AtomicLong(0), new AtomicLong(0)));

        LOG.info("event=write_session_created handle={} catalog={} table={}.{} columns={}",
                handle, catalogName, descriptor.schemaName(), descriptor.tableName(), columnNames);
        return handle;
    }

    /**
     * Resolves write column names from table metadata when descriptor columns
     * contain synthetic native engine names (proj_N). Falls back to descriptor columns
     * when they already match the table schema.
     */
    private List<String> resolveWriteColumnNames(WriteDescriptor descriptor) {
        List<String> descriptorCols = descriptor.columns().stream()
                .map(WriteDescriptor.ColumnSchema::name)
                .toList();
        boolean hasSyntheticNames = descriptorCols.stream()
                .anyMatch(name -> name.startsWith("proj_"));
        if (!hasSyntheticNames) {
            return descriptorCols;
        }
        // Resolve actual column names from table metadata
        try {
            CatalogResourceResolver.CatalogResources resources =
                    catalogLookup.resolve(descriptor.catalogName());
            ConnectorTableHandle tableHandle = resolveTableHandle(
                    resources.metadata(), resources.session(),
                    descriptor.schemaName(), descriptor.tableName());
            var tableMeta = resources.metadata().getTableMetadata(
                    resources.session(), tableHandle);
            List<String> schemaColumns = tableMeta.columns();
            if (schemaColumns.size() == descriptorCols.size()) {
                LOG.info("event=resolve_write_columns synthetic={} resolved={}",
                        descriptorCols, schemaColumns);
                return schemaColumns;
            }
        } catch (Exception e) {
            LOG.warn("event=resolve_write_columns_failed error={}", e.getMessage());
        }
        return descriptorCols;
    }

    private ConnectorPageSink createSinkForDescriptor(WriteDescriptor descriptor) {
        CatalogResourceResolver.CatalogResources resources =
                catalogLookup.resolve(descriptor.catalogName());
        ConnectorMetadata metadata = resources.metadata();

        ConnectorTableHandle tableHandle = resolveTableHandle(
                metadata, resources.session(),
                descriptor.schemaName(), descriptor.tableName());

        ConnectorOutputTableHandle outputHandle = beginOperation(
                metadata, resources.session(), tableHandle, descriptor.dmlOperation());

        return createPageSink(
                resources.pageSinkProvider(), resources.session(),
                outputHandle, descriptor.dmlOperation());
    }

    /**
     * Legacy columnar data interface append path (deprecated).
     * Retained solely so that native JNI method-ID resolution succeeds during
     * NativeWriteExecutor initialization. The active write path uses
     * {@link #appendSerializedBatch} via native {@code appendBatchData()}.
     *
     * @param writeHandle    handle returned by {@link #beginWrite}
     * @param arrowArrayPtr  native pointer to ArrowArray struct (unused)
     * @param arrowSchemaPtr native pointer to ArrowSchema struct (unused)
     */
    @SuppressWarnings("unused") // called via JNI from native NativeWriteExecutor
    public void appendBatch(long writeHandle, long arrowArrayPtr, long arrowSchemaPtr) {
        throw new UnsupportedOperationException(
                "appendBatch via columnar data interface pointers is deprecated; use appendSerializedBatch");
    }

    @Override
    public void appendSerializedBatch(long writeHandle, byte[] data, int rowCount, int columnCount) {
        WriteSession ws = requireSession(writeHandle);

        try (BufferAllocator allocator = sharedAllocator.newChildAllocator(
                "batch-decode-" + writeHandle, 0, 16L * 1024 * 1024)) {
            Page decodedPage = PageDecoder.decode(data, allocator);
            // Per-batch detail at DEBUG — no row data (PII risk).
            LOG.debug("event=decoded_batch handle={} columns={} columnCount={}",
                    writeHandle, decodedPage.columns(), decodedPage.columns().size());
            Page projectedPage = WritePageProjector.projectToWriteSchema(decodedPage, ws.columnNames());
            ws.sink().appendPage(projectedPage);
            ws.rowsAppended().addAndGet(rowCount);
        } catch (java.io.IOException e) {
            throw new NativeRuntimeException(PangrovaErrorCode.NATIVE_DATA_INTERFACE_ERROR,
                    "Failed to decode serialized batch: handle=" + writeHandle
                            + " bytes=" + data.length + " error=" + e.getMessage());
        }

        LOG.debug("event=serialized_batch_appended handle={} catalog={} rows={} bytes={}",
                writeHandle, ws.catalogName(), rowCount, data.length);

        // Periodic aggregate INFO log every BATCH_LOG_INTERVAL batches.
        long currentBatch = ws.batchCount().incrementAndGet();
        if (currentBatch % BATCH_LOG_INTERVAL == 0) {
            LOG.info("event=write_progress handle={} catalog={} batchesSoFar={} totalRows={}",
                    writeHandle, ws.catalogName(), currentBatch, ws.rowsAppended().get());
        }
    }

    /**
     * L4-3A: Zero-copy write path using DirectByteBuffer.
     * The native side wraps its Arrow IPC serialization buffer as a DirectByteBuffer
     * via JNI NewDirectByteBuffer(), eliminating the memcpy from native to JVM heap.
     */
    @Override
    @SuppressWarnings("unused") // called via JNI from native NativeWriteExecutor
    public void appendDirectBatch(long writeHandle, java.nio.ByteBuffer data,
                                  int rowCount, int columnCount) {
        WriteSession ws = requireSession(writeHandle);
        int size = data.remaining();

        try (BufferAllocator allocator = sharedAllocator.newChildAllocator(
                "batch-decode-direct-" + writeHandle, 0, 16L * 1024 * 1024)) {
            Page decodedPage = PageDecoder.decodeDirect(data, allocator);
            LOG.debug("event=decoded_direct_batch handle={} columns={} columnCount={}",
                    writeHandle, decodedPage.columns(), decodedPage.columns().size());
            Page projectedPage = WritePageProjector.projectToWriteSchema(decodedPage, ws.columnNames());
            ws.sink().appendPage(projectedPage);
            ws.rowsAppended().addAndGet(rowCount);
        } catch (java.io.IOException e) {
            throw new NativeRuntimeException(PangrovaErrorCode.NATIVE_DATA_INTERFACE_ERROR,
                    "Failed to decode direct batch: handle=" + writeHandle
                            + " bytes=" + size + " error=" + e.getMessage());
        }

        LOG.debug("event=direct_batch_appended handle={} catalog={} rows={} bytes={}",
                writeHandle, ws.catalogName(), rowCount, size);

        long currentBatch = ws.batchCount().incrementAndGet();
        if (currentBatch % BATCH_LOG_INTERVAL == 0) {
            LOG.info("event=write_progress handle={} catalog={} batchesSoFar={} totalRows={}",
                    writeHandle, ws.catalogName(), currentBatch, ws.rowsAppended().get());
        }
    }

    @Override
    public WriteCommitResult commitWrite(long writeHandle) {
        WriteSession ws = activeSessions.remove(writeHandle);
        if (ws == null) {
            throw new NativeRuntimeException(PangrovaErrorCode.NATIVE_INVALID_HANDLE,
                    "No active write session for handle=" + writeHandle);
        }

        byte[] commitMetadata;
        try {
            commitMetadata = ws.sink().finishWithMetadata();
        } finally {
            ws.sink().close();
        }

        long elapsed = System.nanoTime() - ws.startTimeNanos();
        long totalRows = ws.rowsAppended().get();
        LOG.info("event=write_metadata_collected handle={} catalog={} rowsWritten={} metadataBytes={} elapsedMs={}",
                writeHandle, ws.catalogName(), totalRows, commitMetadata.length, elapsed / NANOS_PER_MILLI);

        // Store commit metadata for TaskStatusCollector to include in the
        // FINISHED status report. Without this, the coordinator never receives
        // commit metadata and finishDistributedWrite() is called with empty
        // fragments — data is never committed to the catalog.
        if (commitMetadata.length > 0) {
            String taskId = handleToTaskId.remove(writeHandle);
            String key = (taskId != null && !taskId.isEmpty()) ? taskId : "__handle_" + writeHandle;
            pendingCommitMetadata.put(key, commitMetadata.clone());
            LOG.debug("event=commit_metadata_pending_put handle={} key={} bytes={} pendingSize={}",
                    writeHandle, key, commitMetadata.length, pendingCommitMetadata.size());
        } else {
            LOG.warn("event=commit_metadata_empty handle={} catalog={}",
                    writeHandle, ws.catalogName());
        }

        return new WriteCommitResult(totalRows, 0L, List.of(), commitMetadata);
    }

    @Override
    public void abortWrite(long writeHandle) {
        WriteSession ws = activeSessions.remove(writeHandle);
        if (ws == null) {
            LOG.warn("event=abort_no_session handle={}", writeHandle);
            return;
        }

        try {
            ws.sink().abort();
        } catch (RuntimeException e) {
            LOG.warn("event=abort_failed handle={} catalog={} error={}",
                    writeHandle, ws.catalogName(), e.getMessage(), e);
        } finally {
            ws.sink().close();
        }

        LOG.info("event=write_aborted handle={} catalog={}", writeHandle, ws.catalogName());
    }

    /**
     * Returns the number of currently active write sessions.
     * Useful for observability and leak detection.
     */
    public int activeSessionCount() {
        return activeSessions.size();
    }

    /**
     * Drains commit metadata for the specified task. Returns the metadata bytes
     * if found, or falls back to draining the first pending entry when the
     * native side did not pass a taskId (legacy compatibility).
     *
     * @param taskId the coordinator-assigned task identifier
     * @return the commit metadata bytes, or empty array if none pending
     */
    public byte[] drainCommitMetadataForTask(String taskId) {
        if (pendingCommitMetadata.isEmpty()) {
            return new byte[0];
        }
        if (taskId != null && !taskId.isEmpty()) {
            byte[] metadata = pendingCommitMetadata.remove(taskId);
            if (metadata != null) {
                LOG.debug("event=commit_metadata_drain_hit taskId={} bytes={}",
                        taskId, metadata.length);
                return metadata;
            }
        }
        // Fallback: native side did not pass taskId — drain first entry
        // (preserves single-writer-per-worker behavior).
        var iterator = pendingCommitMetadata.entrySet().iterator();
        if (!iterator.hasNext()) {
            return new byte[0];
        }
        var entry = iterator.next();
        iterator.remove();
        LOG.debug("event=commit_metadata_drain_fallback taskId={} fallbackKey={} bytes={}",
                taskId, entry.getKey(), entry.getValue().length);
        return entry.getValue();
    }

    /**
     * Aborts and closes all active write sessions, then clears the session map.
     *
     * <p>Intended for cleanup when the native side crashes or the executor is
     * being shut down. Each session's sink is aborted (best-effort) and then
     * closed to release resources. Failures on individual sessions are logged
     * but do not prevent cleanup of remaining sessions.</p>
     */
    public void closeAllSessions() {
        int count = activeSessions.size();
        if (count == 0) {
            return;
        }
        for (var entry : activeSessions.entrySet()) {
            cleanupSession(entry.getKey(), entry.getValue());
        }
        activeSessions.clear();
        LOG.info("event=leaked_sessions_cleaned count={}", count);
    }

    private void cleanupSession(long handle, WriteSession ws) {
        try {
            ws.sink().abort();
        } catch (RuntimeException e) {
            LOG.warn("event=session_abort_failed handle={} catalog={} error={}",
                    handle, ws.catalogName(), e.getMessage(), e);
        }
        try {
            ws.sink().close();
        } catch (RuntimeException e) {
            LOG.warn("event=session_close_failed handle={} catalog={} error={}",
                    handle, ws.catalogName(), e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        closeAllSessions();
        sharedAllocator.close();
    }

    private WriteSession requireSession(long writeHandle) {
        WriteSession ws = activeSessions.get(writeHandle);
        if (ws == null) {
            throw new NativeRuntimeException(PangrovaErrorCode.NATIVE_INVALID_HANDLE,
                    "No active write session for handle=" + writeHandle);
        }
        return ws;
    }

    private static ConnectorTableHandle resolveTableHandle(
            ConnectorMetadata metadata,
            org.pangrova.spi.connector.ConnectorSession session,
            String schemaName,
            String tableName) {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(schemaName, tableName));
        if (handle == null) {
            throw new NativeRuntimeException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + schemaName + "." + tableName);
        }
        return handle;
    }

    private static ConnectorOutputTableHandle beginOperation(
            ConnectorMetadata metadata,
            org.pangrova.spi.connector.ConnectorSession session,
            ConnectorTableHandle tableHandle,
            WriteDescriptor.DmlOperation dmlOperation) {
        return switch (dmlOperation) {
            case INSERT -> metadata.beginInsert(session, tableHandle);
            case OVERWRITE -> metadata.beginOverwrite(session, tableHandle);
            case DELETE -> metadata.beginDelete(session, tableHandle);
        };
    }

    private static ConnectorPageSink createPageSink(
            ConnectorPageSinkProvider sinkProvider,
            org.pangrova.spi.connector.ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            WriteDescriptor.DmlOperation dmlOperation) {
        return switch (dmlOperation) {
            case INSERT -> sinkProvider.createPageSink(
                    ConnectorTransactionHandle.EMPTY, session, outputHandle);
            case OVERWRITE -> sinkProvider.createOverwritePageSink(
                    ConnectorTransactionHandle.EMPTY, session, outputHandle);
            case DELETE -> sinkProvider.createDeletePageSink(
                    ConnectorTransactionHandle.EMPTY, session, outputHandle);
        };
    }


}
