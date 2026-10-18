package org.pangrova.worker.jni.write;

import org.pangrova.worker.jni.write.catalog.CatalogResourceResolver;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.protocol.batch.ColumnarPageSink;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.worker.jni.NativeRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Adapts {@link NativeWriter} calls to {@link ConnectorPageSink} SPI.
 *
 * <p>Native workers call this adapter via JNI to write data through Java connector page sink
 * implementations. Each write session is keyed by an opaque handle and backed by a {@link
 * ConnectorPageSink} resolved from the catalog name in the {@link WriteDescriptor}.
 *
 * <p>Thread safety: uses {@link ConcurrentHashMap} for active sessions and {@link AtomicLong} for
 * handle generation. Individual sessions are accessed by the native operator and cleanup threads.
 * Short ownership claims exclude concurrent sink operations without holding a monitor during
 * connector IO. Cleanup rejects in-flight operations and retains owners for retry; global shutdown
 * seals new-session admission.
 */
@ThreadSafe
public final class NativeWriteExecutor implements NativeWriter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeWriteExecutor.class);

    /**
     * Stores commit metadata produced by commitWrite(), keyed by taskId. TaskStatusCollector reads
     * this via drainCommitMetadataForTask(taskId) when building the FINISHED status report for
     * write tasks.
     *
     * <p>Per-task isolation: the canonical taskId is mandatory in {@link
     * WriteDescriptor#properties()}. Metadata is only ever drained by an exact task identifier;
     * cross-task fallback would violate commit fencing.
     */
    private final ConcurrentHashMap<String, byte[]> pendingCommitMetadata =
            new ConcurrentHashMap<>();

    /**
     * Aggregate INFO log interval for per-batch write progress. Emits one INFO log every N batches
     * to avoid log flooding while still providing visibility into long-running write tasks.
     */
    static final int BATCH_LOG_INTERVAL = 1000;

    /** Nanoseconds per millisecond, for elapsed-time conversion. */
    private static final long NANOS_PER_MILLI = 1_000_000;

    private final CatalogResourceResolver catalogLookup;
    private final ConcurrentHashMap<Long, NativeWriteSession> activeSessions =
            new ConcurrentHashMap<>();
    private final AtomicLong handleCounter = new AtomicLong();

    /**
     * Shared allocator for columnar IPC decoding, reused across batches to avoid per-batch
     * RootAllocator creation/destruction overhead and GC pressure.
     */
    private final BufferAllocator sharedAllocator = new RootAllocator(64L * 1024 * 1024);

    private final NativeWriteAdmission admission = new NativeWriteAdmission();
    private boolean allocatorClosed;
    private boolean catalogClosed;

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
        String taskId = requireTaskId(descriptor);
        requireConnectorManagedWriteFormat(descriptor);
        admission.enter(taskId);
        try {
            return openSession(descriptor, taskId);
        } finally {
            admission.leave(taskId);
        }
    }

    private long openSession(WriteDescriptor descriptor, String taskId) {
        String catalogName = descriptor.catalogName();
        LOG.info(
                "event=begin_write catalog={} schema={} table={} mode={}",
                catalogName,
                descriptor.schemaName(),
                descriptor.tableName(),
                descriptor.dmlOperation());

        // Physical task-local ordinals are not connector-owned logical column names.
        // Resolve them only against the output handle already verified for this task.
        CatalogResourceResolver.CatalogResources resources =
                catalogLookup.resolve(taskId, catalogName);
        boolean physicalColumns =
                WritePageProjector.hasCanonicalPhysicalColumns(
                        descriptor.columns().stream()
                                .map(WriteDescriptor.ColumnSchema::name)
                                .toList());
        List<String> columnNames = resolveWriteColumnNames(descriptor, resources, physicalColumns);
        long handle = handleCounter.incrementAndGet();
        if (handle <= 0) {
            throw new IllegalStateException("Native write handles are exhausted");
        }
        NativeWriteSession session =
                new NativeWriteSession(taskId, columnNames, catalogName, physicalColumns);
        // Publication precedes allocation; close rejects the live admission until initialization
        // exits.
        activeSessions.put(handle, session);
        try {
            session.initialize(() -> createSinkForDescriptor(descriptor, resources));
            admission.requireOpen();
        } catch (RuntimeException | Error failure) {
            failWrite(handle, session, failure);
            throw failure;
        }

        LOG.info(
                "event=write_session_created handle={} catalog={} table={}.{} columns={}",
                handle,
                catalogName,
                descriptor.schemaName(),
                descriptor.tableName(),
                columnNames);
        return handle;
    }

    /** Resolves canonical task-local ordinals against the verified output handle's column order. */
    private List<String> resolveWriteColumnNames(
            WriteDescriptor descriptor,
            CatalogResourceResolver.CatalogResources resources,
            boolean physicalColumns) {
        List<String> descriptorCols =
                descriptor.columns().stream().map(WriteDescriptor.ColumnSchema::name).toList();
        List<String> schemaColumns = resources.writeColumnNames();
        if (!physicalColumns) {
            List<String> logicalColumns =
                    schemaColumns.containsAll(descriptorCols)
                            ? List.copyOf(new java.util.LinkedHashSet<>(descriptorCols))
                            : WritePageProjector.resolveWriteColumns(descriptorCols);
            if (!schemaColumns.containsAll(logicalColumns)) {
                throw new IllegalArgumentException(
                        "Logical write columns differ from verified output columns");
            }
            return logicalColumns;
        }
        if (schemaColumns.size() != descriptorCols.size()) {
            throw new IllegalArgumentException(
                    "Native projection width does not match verified output columns: projection="
                            + descriptorCols.size()
                            + " table="
                            + schemaColumns.size());
        }
        LOG.info(
                "event=resolve_write_columns physical={} resolved={}",
                descriptorCols,
                schemaColumns);
        return schemaColumns;
    }

    private ConnectorPageSink createSinkForDescriptor(
            WriteDescriptor descriptor, CatalogResourceResolver.CatalogResources resources) {
        return createPageSink(
                resources.pageSinkProvider(), resources.session(),
                resources.outputHandle(), descriptor.dmlOperation());
    }

    /** Pre-binds the exact catalog deployment and output handle before Native starts. */
    public void bindTask(String taskId, org.pangrova.protocol.v2.TaskRequest request) {
        admission.enter(taskId);
        boolean bound = false;
        try {
            catalogLookup.bindTask(taskId, request);
            bound = true;
            admission.requireOpen();
        } catch (RuntimeException | Error failure) {
            if (bound) {
                try {
                    catalogLookup.releaseTask(taskId);
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) {
                        failure.addSuppressed(cleanup);
                    }
                }
            }
            throw failure;
        } finally {
            admission.leave(taskId);
        }
    }

    /** Releases all task-scoped connector bindings. */
    public void releaseTask(String taskId) {
        admission.beginRelease(taskId);
        try {
            closeSessions(taskId);
            catalogLookup.releaseTask(taskId);
        } finally {
            admission.endRelease(taskId);
        }
    }

    /**
     * Borrows the native buffer synchronously, avoiding a payload-sized heap copy. Decoding still
     * allocates column buffers. No reader or view escapes this call.
     */
    @Override
    @SuppressWarnings("unused") // called via JNI from native NativeWriteExecutor
    public void appendDirectBatch(
            long writeHandle, java.nio.ByteBuffer data, int rowCount, int columnCount) {
        appendDecodedBatch(writeHandle, data, rowCount, columnCount);
    }

    private void appendDecodedBatch(
            long writeHandle, ByteBuffer data, int rowCount, int columnCount) {
        NativeWriteSession ws = requireSession(writeHandle);
        ws.beginAppend();
        try {
            try {
                appendDecodedBatch(writeHandle, data, rowCount, columnCount, ws);
            } catch (RuntimeException | Error failure) {
                ws.stop();
                throw failure;
            } finally {
                ws.endOperation();
            }
        } catch (RuntimeException | Error failure) {
            failWrite(writeHandle, ws, failure);
            throw failure;
        }
    }

    private void appendDecodedBatch(
            long writeHandle,
            ByteBuffer data,
            int rowCount,
            int columnCount,
            NativeWriteSession ws) {
        try (BufferAllocator allocator =
                        sharedAllocator.newChildAllocator(
                                "batch-decode-" + writeHandle, 0, 16L * 1024 * 1024);
                ColumnarBatch decoded = ColumnarBatch.open(data, allocator, rowCount, columnCount);
                ColumnarBatch projected =
                        WritePageProjector.projectToWriteSchema(
                                decoded, ws.columnNames(), ws.physicalColumns())) {
            appendToSink(ws.sink(), projected);
            ws.rowsAppended().addAndGet(decoded.rowCount());
        } catch (IOException e) {
            LOG.error("event=write_batch_decode_failed handle={} rows={} columns={} bytes={}",
                    writeHandle, rowCount, columnCount, data == null ? 0 : data.remaining(), e);
            NativeRuntimeException failure =
                    new NativeRuntimeException(
                            PangrovaErrorCode.NATIVE_DATA_INTERFACE_ERROR,
                            "Failed to decode write batch: handle=" + writeHandle,
                            e);
            throw failure;
        }
        long currentBatch = ws.batchCount().incrementAndGet();
        if (currentBatch % BATCH_LOG_INTERVAL == 0) {
            LOG.info(
                    "event=write_progress handle={} catalog={} batchesSoFar={} totalRows={}",
                    writeHandle,
                    ws.catalogName(),
                    currentBatch,
                    ws.rowsAppended().get());
        }
    }

    private static void appendToSink(ConnectorPageSink sink, ColumnarBatch batch) {
        if (sink instanceof ColumnarPageSink columnarSink) {
            columnarSink.appendColumnarBatch(batch);
            return;
        }
        // Row-only sinks use their declared SPI, never an unsupported native API.
        // Keep conversion bounded to one row, rather than retaining a whole batch.
        List<String> columns = batch.columns();
        if (batch.rowCount() == 0) {
            sink.appendPage(new Page(columns, List.of()));
        }
        for (int row = 0; row < batch.rowCount(); row++) {
            TypedRow.Builder builder = TypedRow.builder();
            for (int column = 0; column < columns.size(); column++) {
                builder.putRaw(columns.get(column), batch.value(row, column));
            }
            sink.appendPage(new Page(columns, List.of(builder.build())));
        }
    }

    private void failWrite(long handle, NativeWriteSession session, Throwable failure) {
        try {
            session.abort();
            activeSessions.remove(handle, session);
        } catch (RuntimeException | Error cleanup) {
            if (cleanup != failure) {
                failure.addSuppressed(cleanup);
                for (Throwable secondary : cleanup.getSuppressed()) {
                    if (secondary != failure) {
                        failure.addSuppressed(secondary);
                    }
                }
            }
        }
    }

    @Override
    public WriteCommitResult commitWrite(long writeHandle) {
        NativeWriteSession ws = requireSession(writeHandle);
        try {
            return finishSession(writeHandle, ws);
        } catch (RuntimeException | Error failure) {
            failWrite(writeHandle, ws, failure);
            throw failure;
        }
    }

    private WriteCommitResult finishSession(long writeHandle, NativeWriteSession ws) {
        byte[] commitMetadata = ws.finish();
        String taskId = ws.taskId();
        long elapsed = System.nanoTime() - ws.startTimeNanos();
        long totalRows = ws.rowsAppended().get();
        LOG.info(
                "event=write_metadata_collected handle={} catalog={} rowsWritten={} metadataBytes={} elapsedMs={}",
                writeHandle,
                ws.catalogName(),
                totalRows,
                commitMetadata.length,
                elapsed / NANOS_PER_MILLI);

        // Store commit metadata for TaskStatusCollector to include in the
        // FINISHED status report. Without this, the coordinator never receives
        // commit metadata and finishDistributedWrite() is called with empty
        // fragments — data is never committed to the catalog.
        if (commitMetadata.length > 0) {
            publishCommitMetadataForTask(taskId, commitMetadata);
            LOG.debug(
                    "event=commit_metadata_pending_put handle={} key={} bytes={} pendingSize={}",
                    writeHandle,
                    taskId,
                    commitMetadata.length,
                    pendingCommitMetadata.size());
        } else {
            LOG.warn(
                    "event=commit_metadata_empty handle={} catalog={}",
                    writeHandle,
                    ws.catalogName());
        }

        var result = new WriteCommitResult(totalRows, 0L, List.of(), commitMetadata);
        activeSessions.remove(writeHandle, ws);
        return result;
    }

    @Override
    public void abortWrite(long writeHandle) {
        NativeWriteSession ws = activeSessions.get(writeHandle);
        if (ws == null) {
            LOG.warn("event=abort_no_session handle={}", writeHandle);
            return;
        }

        ws.abort();
        activeSessions.remove(writeHandle, ws);
        LOG.info("event=write_aborted handle={} catalog={}", writeHandle, ws.catalogName());
    }

    /**
     * Returns the number of currently active write sessions. Useful for observability and leak
     * detection.
     */
    public int activeSessionCount() {
        return activeSessions.size();
    }

    /**
     * Publishes a native writer's immutable commit fragment for its canonical task. An identical
     * retry is harmless; a conflicting fragment is rejected before a task can report success with
     * ambiguous catalog input.
     *
     * @param taskId coordinator-assigned task identifier
     * @param metadata nonempty serialized commit fragment
     */
    @SuppressWarnings("unused") // called by the native writer through JNI
    public void publishCommitMetadataForTask(String taskId, byte[] metadata) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException(
                    "Canonical taskId is required for write commit metadata");
        }
        if (metadata == null || metadata.length == 0) {
            throw new IllegalArgumentException("Nonempty write commit metadata is required");
        }
        byte[] previous = pendingCommitMetadata.putIfAbsent(taskId, metadata.clone());
        if (previous != null && !Arrays.equals(previous, metadata)) {
            throw new IllegalStateException(
                    "Conflicting write commit metadata for taskId=" + taskId);
        }
    }

    /**
     * Drains commit metadata for the specified task. Returns the metadata bytes if found. No
     * cross-task fallback is permitted.
     *
     * @param taskId the coordinator-assigned task identifier
     * @return the commit metadata bytes, or empty array if none pending
     */
    public byte[] drainCommitMetadataForTask(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException(
                    "Canonical taskId is required to drain write commit metadata");
        }
        byte[] metadata = pendingCommitMetadata.remove(taskId);
        if (metadata == null) {
            return new byte[0];
        }
        LOG.debug("event=commit_metadata_drain_hit taskId={} bytes={}", taskId, metadata.length);
        return metadata;
    }

    /**
     * Aborts and closes all active write sessions, retaining each failed owner.
     *
     * <p>Intended for cleanup when the native side crashes or the executor is being shut down. Each
     * session is removed only after its actual abort and close return. Failures do not prevent
     * trying remaining sessions, but are propagated to prevent early resource or catalog-lease
     * release.
     */
    public void closeAllSessions() {
        admission.beginCleanup(false);
        try {
            closeSessions(null);
        } finally {
            admission.endCleanup();
        }
    }

    private void closeSessions(String taskId) {
        Throwable failure = null;
        for (var entry : activeSessions.entrySet()) {
            NativeWriteSession ws = entry.getValue();
            if (taskId != null && !taskId.equals(ws.taskId())) {
                continue;
            }
            try {
                ws.abort();
                activeSessions.remove(entry.getKey(), ws);
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
    }

    @Override
    public void close() {
        admission.beginCleanup(true);
        try {
            closeOwnedResources();
        } finally {
            admission.endCleanup();
        }
    }

    private void closeOwnedResources() {
        closeSessions(null);
        if (!allocatorClosed) {
            sharedAllocator.close();
            allocatorClosed = true;
        }
        if (!catalogClosed && catalogLookup instanceof AutoCloseable closeable) {
            try {
                closeable.close();
                catalogClosed = true;
            } catch (Exception failure) {
                throw new IllegalStateException("Failed to close Worker catalog runtime", failure);
            }
        }
    }

    private NativeWriteSession requireSession(long writeHandle) {
        NativeWriteSession ws = activeSessions.get(writeHandle);
        if (ws == null) {
            throw new NativeRuntimeException(
                    PangrovaErrorCode.NATIVE_INVALID_HANDLE,
                    "No active write session for handle=" + writeHandle);
        }
        return ws;
    }

    private static String requireTaskId(WriteDescriptor descriptor) {
        String taskId = descriptor.properties().get("taskId");
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException(
                    "Native write descriptor requires a non-empty canonical taskId");
        }
        return taskId;
    }

    private static void requireConnectorManagedWriteFormat(WriteDescriptor descriptor) {
        String format = descriptor.properties().get(WriteDescriptor.FILE_FORMAT_PROPERTY);
        if (format != null && "LANCE".equalsIgnoreCase(format)) {
            throw new IllegalArgumentException(
                    "Lance writes require Native output allocation and external-file commit metadata; "
                            + "the connector PageSink path cannot write Lance");
        }
    }

    private static ConnectorPageSink createPageSink(
            ConnectorPageSinkProvider sinkProvider,
            org.pangrova.spi.connector.ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            WriteDescriptor.DmlOperation dmlOperation) {
        return switch (dmlOperation) {
            case INSERT ->
                    sinkProvider.createPageSink(
                            ConnectorTransactionHandle.EMPTY, session, outputHandle);
            case OVERWRITE ->
                    sinkProvider.createOverwritePageSink(
                            ConnectorTransactionHandle.EMPTY, session, outputHandle);
            case DELETE ->
                    sinkProvider.createDeletePageSink(
                            ConnectorTransactionHandle.EMPTY, session, outputHandle);
        };
    }
}
