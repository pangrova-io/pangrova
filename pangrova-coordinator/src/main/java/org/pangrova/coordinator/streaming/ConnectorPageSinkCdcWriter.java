package org.pangrova.coordinator.streaming;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.streaming.record.CdcEnvelope;
import org.pangrova.spi.streaming.record.CdcRow;
import org.pangrova.spi.streaming.sink.CdcWriteResult;
import org.pangrova.spi.streaming.sink.CdcWriteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Writes CDC data through the standard connector PageSink path.
 *
 * <p>The current production bootstrap processes CDC batches locally on the
 * coordinator. Worker-side CDC routing remains open and must be introduced
 * through a real canonical task path, not a nullable dispatch hook.</p>
 *
 * <p>Concurrency strategy: this class holds no mutable state. Each
 * {@link #writeBatch} call creates its own page sink, making
 * concurrent calls safe.</p>
 */
@ThreadSafe
public final class ConnectorPageSinkCdcWriter implements CdcWriteSink {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorPageSinkCdcWriter.class);

    /** Conservative per-row byte estimate for metrics until schema-aware sizing is added. */
    private static final long ESTIMATED_BYTES_PER_ROW = 256L;

    private final PageSinkStreamWriterContext deps;

    /** Creates the current coordinator-local connector writer. */
    public ConnectorPageSinkCdcWriter(PageSinkStreamWriterContext deps) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
    }

    @Override
    public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
        return processLocally(envelopes);
    }

    /**
     * Processes CDC batches locally on the coordinator.
     * Routes each envelope by operation type:
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
            // Once publication was attempted, its outcome may be ambiguous. discard() only
            // removes files from a stage that has never entered the catalog commit path.
            discard(staged);
            throw e;
        }
    }

    // --- Staged write lifecycle ---

    @Override
    public CdcWriteSink.StagedWrite stage(List<CdcEnvelope> envelopes) {
        return stageLocal(envelopes);
    }

    @Override
    public CdcWriteResult publish(CdcWriteSink.StagedWrite staged) {
        if (staged instanceof StagedBatch batch && batch.owner == this) {
            return publishLocal(batch);
        }
        throw new IllegalArgumentException("CDC staged write token was not created by this writer");
    }

    @Override
    public void discard(CdcWriteSink.StagedWrite staged) {
        if (staged instanceof StagedBatch batch && batch.owner == this) {
            if (!batch.state.compareAndSet(PublicationState.PREPARED,
                    PublicationState.DISCARDED)) {
                LOG.warn("event=stream_write_discard_skipped target={} reason=publication_attempted state={}",
                        deps.targetTable(), batch.state.get());
                return;
            }
            for (PhaseStage phase : batch.phases) {
                deps.metadata().abortDistributedWrite(deps.session(), phase.handle(), List.of(phase.fragment()));
            }
            LOG.debug("event=stream_write_staged_discarded target={} phases={}",
                    deps.targetTable(), batch.phases.size());
        } else {
            throw new IllegalArgumentException("CDC staged write token was not created by this writer");
        }
    }

    /**
     * Write each phase's data and finalize its sink to fragments, WITHOUT publishing
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
        try {
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
        } catch (RuntimeException failure) {
            for (PhaseStage phase : phases) {
                try {
                    deps.metadata().abortDistributedWrite(
                            deps.session(), phase.handle(), List.of(phase.fragment()));
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
        return new StagedBatch(this, phases, envelopes.size());
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

    /** Publishes staged phases; separate phases are not one catalog transaction. */
    private CdcWriteResult publishLocal(StagedBatch staged) {
        if (!staged.state.compareAndSet(PublicationState.PREPARED,
                PublicationState.PUBLISHING)) {
            if (staged.state.get() == PublicationState.PUBLISHED) {
                return staged.publishedResult;
            }
            throw new IllegalStateException(
                    "CDC staged write cannot be republished after an ambiguous or discarded commit");
        }
        try {
            for (PhaseStage phase : staged.phases) {
                deps.metadata().finishDistributedWrite(
                        deps.session(), phase.handle(), List.of(phase.fragment()));
            }
            CdcWriteResult result = new CdcWriteResult(staged.rowCount,
                    (long) staged.rowCount * ESTIMATED_BYTES_PER_ROW,
                    staged.phases.size(), deps.targetTable());
            staged.publishedResult = result;
            staged.state.set(PublicationState.PUBLISHED);
            LOG.debug("event=stream_write_batch_committed target={} rows={} phases={}",
                    deps.targetTable(), staged.rowCount, staged.phases.size());
            return result;
        } catch (RuntimeException failure) {
            staged.state.set(PublicationState.UNKNOWN);
            throw failure;
        }
    }

    /** One staged write phase (INSERT or DELETE) — its handle and unpublished fragment. */
    private record PhaseStage(
            org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle handle, byte[] fragment) {}

    private enum PublicationState { PREPARED, PUBLISHING, PUBLISHED, UNKNOWN, DISCARDED }

    /** A staged (written-but-unpublished) batch for the local commit path. */
    private static final class StagedBatch implements CdcWriteSink.StagedWrite {
        private final ConnectorPageSinkCdcWriter owner;
        private final List<PhaseStage> phases;
        private final int rowCount;
        private final AtomicReference<PublicationState> state =
                new AtomicReference<>(PublicationState.PREPARED);
        private volatile CdcWriteResult publishedResult;

        StagedBatch(ConnectorPageSinkCdcWriter owner, List<PhaseStage> phases, int rowCount) {
            this.owner = owner;
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
