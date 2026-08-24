package org.pangrova.coordinator.streaming;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcFormat;
import org.pangrova.spi.streaming.CdcRow;
import org.pangrova.spi.streaming.CdcWriteResult;
import org.pangrova.spi.streaming.CdcWriteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Writes CDC data through the standard connector PageSink path, with
 * threshold-based routing to native workers for large batches.
 *
 * <p>Batches exceeding {@code nativeTransformThreshold} rows are compiled
 * to a Substrait plan (ReadRel → TransformRel → WriteRel) and dispatched
 * to a native worker for processing in Arrow columnar memory. Small batches
 * are processed locally on the coordinator (acceptable overhead).</p>
 *
 * <p>CDC format detection routes to the appropriate native parser via
 * {@link CdcFormat} in the WriteDescriptor properties.</p>
 *
 * <p>Concurrency strategy: this class holds no mutable state. Each
 * {@link #writeBatch} call creates its own page sink, making
 * concurrent calls safe.</p>
 */
@ThreadSafe
public final class ConnectorPageSinkCdcWriter implements CdcWriteSink {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorPageSinkCdcWriter.class);

    /** Default threshold: batches above this size are dispatched to native workers. */
    public static final int DEFAULT_NATIVE_TRANSFORM_THRESHOLD = 1000;

    /** Conservative per-row byte estimate for metrics until schema-aware sizing is added. */
    private static final long ESTIMATED_BYTES_PER_ROW = 256L;

    private final PageSinkStreamWriterContext deps;
    private final int nativeTransformThreshold;
    private final CdcBatchDispatchFunction nativeTransform;
    private final CdcFormat cdcFormat;

    /**
     * Full constructor with native transform support and CDC format.
     *
     * @param deps                     aggregated dependencies
     * @param nativeTransformThreshold row count threshold for native dispatch
     * @param nativeTransform          native dispatch function (nullable)
     * @param cdcFormat                CDC envelope format for native parsing
     */
    public ConnectorPageSinkCdcWriter(PageSinkStreamWriterContext deps,
                                      int nativeTransformThreshold,
                                      CdcBatchDispatchFunction nativeTransform,
                                      CdcFormat cdcFormat) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.nativeTransformThreshold = nativeTransformThreshold;
        this.nativeTransform = nativeTransform;
        this.cdcFormat = cdcFormat != null ? cdcFormat : CdcFormat.DEBEZIUM;
    }

    /**
     * Constructor without native transform (coordinator-side processing only).
     */
    public ConnectorPageSinkCdcWriter(PageSinkStreamWriterContext deps) {
        this(deps, DEFAULT_NATIVE_TRANSFORM_THRESHOLD, null, CdcFormat.DEBEZIUM);
    }

    @Override
    public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
        if (nativeTransform != null && envelopes.size() > nativeTransformThreshold) {
            return dispatchNativeTransform(envelopes);
        }
        return processLocally(envelopes);
    }

    /**
     * Dispatches large CDC batches to a native worker for processing.
     * The worker parses envelopes according to the CDC format, transforms
     * rows in Arrow columnar memory, and writes directly to the target table.
     */
    private CdcWriteResult dispatchNativeTransform(List<CdcEnvelope> envelopes) {
        LOG.info("event=stream_write_native_dispatch target={} rows={} format={} threshold={}",
                deps.targetTable(), envelopes.size(), cdcFormat.formatName(),
                nativeTransformThreshold);
        return nativeTransform.apply(envelopes, deps.targetTable(), cdcFormat);
    }

    /**
     * Processes small CDC batches locally on the coordinator.
     * Routes each envelope by operation type (F-CHAIN-STREAM-02):
     * <ul>
     *   <li>INSERT: {@code beginInsert} + {@code createPageSink} (append)</li>
     *   <li>DELETE: {@code beginDelete} + {@code createDeletePageSink} (equality delete via before-image)</li>
     *   <li>UPDATE: delete of before-image + insert of after-image</li>
     * </ul>
     */
    private CdcWriteResult processLocally(List<CdcEnvelope> envelopes) {
        // writeBatch (non-2PC / single-shot) path: stage then immediately publish.
        StagedBatch staged = stageLocal(envelopes);
        try {
            return publishLocal(staged);
        } catch (RuntimeException e) {
            // A-EOS-02: commit (publish) failed after staging — discard the staged files so a failed
            // single-shot write leaks nothing (the sink is already finished+closed at stage time).
            discard(staged);
            throw e;
        }
    }

    // --- A-EOS-02: staged two-phase commit (stage → publish/discard) ---

    @Override
    public CdcWriteSink.StagedWrite stage(List<CdcEnvelope> envelopes) {
        if (envelopes.size() >= nativeTransformThreshold && nativeTransform != null) {
            // Native remote transform cannot stage files locally; fall back to buffered staging
            // (publish will dispatch it). The local path below is true staged 2PC.
            return CdcWriteSink.super.stage(envelopes);
        }
        return stageLocal(envelopes);
    }

    @Override
    public CdcWriteResult publish(CdcWriteSink.StagedWrite staged) {
        if (staged instanceof StagedBatch batch) {
            return publishLocal(batch);
        }
        return CdcWriteSink.super.publish(staged);
    }

    @Override
    public void discard(CdcWriteSink.StagedWrite staged) {
        if (staged instanceof StagedBatch batch) {
            for (PhaseStage phase : batch.phases) {
                deps.metadata().abortDistributedWrite(deps.session(), phase.handle(), List.of(phase.fragment()));
            }
            LOG.debug("event=stream_write_staged_discarded target={} phases={}",
                    deps.targetTable(), batch.phases.size());
        } else {
            CdcWriteSink.super.discard(staged);
        }
    }

    /**
     * A-EOS-02 prepare: write each phase's data + finalize its sink to fragments, WITHOUT publishing
     * to the catalog. The written files are staged (on storage, unreferenced by any snapshot).
     */
    private StagedBatch stageLocal(List<CdcEnvelope> envelopes) {
        validateEnvelopes(envelopes);

        List<CdcRow> deleteRows = new ArrayList<>();
        List<CdcRow> insertRows = new ArrayList<>();
        for (CdcEnvelope envelope : envelopes) {
            switch (envelope.operation()) {
                case INSERT -> insertRows.add(envelope.after());
                case DELETE -> deleteRows.add(envelope.before());
                case UPDATE -> {
                    deleteRows.add(envelope.before());
                    insertRows.add(envelope.after());
                }
                default -> throw new IllegalStateException(
                        "Unhandled CDC operation: " + envelope.operation());
            }
        }

        var tableName = toSchemaTableName(deps.targetTable());
        List<PhaseStage> phases = new ArrayList<>(2);
        if (!deleteRows.isEmpty()) {
            var handle = deps.metadata().beginDelete(deps.session(),
                    deps.metadata().getTableHandle(deps.session(), tableName));
            ConnectorPageSink sink = deps.pageSinkProvider()
                    .createDeletePageSink(ConnectorTransactionHandle.EMPTY, deps.session(), handle);
            phases.add(stagePhase(sink, cdcRowsToPage(deleteRows), handle));
        }
        if (!insertRows.isEmpty()) {
            var handle = deps.metadata().beginInsert(deps.session(),
                    deps.metadata().getTableHandle(deps.session(), tableName));
            ConnectorPageSink sink = deps.pageSinkProvider()
                    .createPageSink(ConnectorTransactionHandle.EMPTY, deps.session(), handle);
            phases.add(stagePhase(sink, cdcRowsToPage(insertRows), handle));
        }
        return new StagedBatch(phases, envelopes.size());
    }

    private PhaseStage stagePhase(ConnectorPageSink sink, Page page,
                                  org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle handle) {
        try {
            sink.appendPage(page);
            byte[] metadata = sink.finishWithMetadata();
            return new PhaseStage(handle, metadata);
        } catch (Exception e) {
            sink.abort();
            throw e;
        } finally {
            sink.close();
        }
    }

    /** A-EOS-02 commit: atomically publish every staged phase to the catalog. */
    private CdcWriteResult publishLocal(StagedBatch staged) {
        for (PhaseStage phase : staged.phases) {
            deps.metadata().finishDistributedWrite(deps.session(), phase.handle(), List.of(phase.fragment()));
        }
        LOG.debug("event=stream_write_batch_committed target={} rows={} phases={}",
                deps.targetTable(), staged.rowCount, staged.phases.size());
        return new CdcWriteResult(staged.rowCount,
                (long) staged.rowCount * ESTIMATED_BYTES_PER_ROW, staged.phases.size(), deps.targetTable());
    }

    /** A-EOS-02: one staged write phase (INSERT or DELETE) — its handle + unpublished fragment. */
    private record PhaseStage(
            org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle handle, byte[] fragment) {}

    /** A-EOS-02: a staged (written-but-unpublished) batch for the local two-phase-commit path. */
    private static final class StagedBatch implements CdcWriteSink.StagedWrite {
        private final List<PhaseStage> phases;
        private final int rowCount;

        StagedBatch(List<PhaseStage> phases, int rowCount) {
            this.phases = phases;
            this.rowCount = rowCount;
        }
    }

    /**
     * Resolves the fully-qualified CDC target ({@code catalog.schema.table},
     * {@code schema.table}, or a bare {@code table}) into a
     * {@link org.pangrova.spi.connector.metadata.table.SchemaTableName}.
     *
     * <p>F-STREAM-001: the schema was previously hardcoded to {@code "default"}, so a CDC stream
     * targeting any non-default schema silently wrote to {@code default.TABLE} instead of the
     * intended table. The schema is now taken from the qualified target; the catalog segment is
     * ignored here because it is bound to the connector session/metadata.</p>
     */
    private static org.pangrova.spi.connector.metadata.table.SchemaTableName toSchemaTableName(String targetTable) {
        String[] parts = targetTable.split("\\.");
        if (parts.length >= 3) {
            return new org.pangrova.spi.connector.metadata.table.SchemaTableName(
                    parts[parts.length - 2], parts[parts.length - 1]);
        }
        if (parts.length == 2) {
            return new org.pangrova.spi.connector.metadata.table.SchemaTableName(parts[0], parts[1]);
        }
        // Unqualified target: no schema information available; fall back to the conventional default.
        return new org.pangrova.spi.connector.metadata.table.SchemaTableName("default", parts[0]);
    }

    /**
     * Validates that each envelope has the required images for its operation type.
     * Fails fast with a clear message for missing images.
     */
    private static void validateEnvelopes(List<CdcEnvelope> envelopes) {
        for (CdcEnvelope envelope : envelopes) {
            switch (envelope.operation()) {
                case INSERT -> {
                    if (envelope.after() == null) {
                        throw new IllegalStateException(
                                "INSERT envelope has no after-image for table " + envelope.tableName());
                    }
                }
                case DELETE -> {
                    if (envelope.before() == null) {
                        throw new IllegalStateException(
                                "DELETE envelope has no before-image for table " + envelope.tableName());
                    }
                }
                case UPDATE -> {
                    if (envelope.before() == null) {
                        throw new IllegalStateException(
                                "UPDATE envelope has no before-image for table " + envelope.tableName());
                    }
                    if (envelope.after() == null) {
                        throw new IllegalStateException(
                                "UPDATE envelope has no after-image for table " + envelope.tableName());
                    }
                }
                default -> throw new IllegalStateException(
                        "Unhandled CDC operation: " + envelope.operation());
            }
        }
    }

    @Override
    public void close() {
        // No persistent resources; PageSink is created per batch.
    }

    /** Converts a list of CdcRow objects to a Page. */
    private static Page cdcRowsToPage(List<CdcRow> cdcRows) {
        if (cdcRows.isEmpty()) {
            return new Page(List.of(), List.of());
        }
        List<String> columns = List.copyOf(cdcRows.get(0).columnNames());
        List<TypedRow> rows = new ArrayList<>(cdcRows.size());
        for (CdcRow cdcRow : cdcRows) {
            TypedRow.Builder builder = TypedRow.builder();
            for (String col : columns) {
                builder.putRaw(col, cdcRow.get(col));
            }
            rows.add(builder.build());
        }
        return new Page(columns, rows);
    }
}
