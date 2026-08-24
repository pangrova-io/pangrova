package org.pangrova.coordinator.streaming;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
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
public final class ConnectorPageSinkStreamWriter implements CdcWriteSink {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorPageSinkStreamWriter.class);

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
    public ConnectorPageSinkStreamWriter(PageSinkStreamWriterContext deps,
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
    public ConnectorPageSinkStreamWriter(PageSinkStreamWriterContext deps) {
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
     * Acceptable overhead for batches at or below the threshold.
     */
    private CdcWriteResult processLocally(List<CdcEnvelope> envelopes) {
        Page page = convertEnvelopesToPage(envelopes);

        ConnectorOutputTableHandle outputHandle = deps.outputTableHandle();
        ConnectorPageSink pageSink = deps.pageSinkProvider()
                .createPageSink(ConnectorTransactionHandle.EMPTY, deps.session(), outputHandle);
        try {
            pageSink.appendPage(page);
            pageSink.finishWithMetadata();

            LOG.debug("event=stream_write_batch_committed target={} rows={}",
                    deps.targetTable(), envelopes.size());

            return new CdcWriteResult(
                    envelopes.size(),
                    (long) envelopes.size() * ESTIMATED_BYTES_PER_ROW,
                    1,
                    deps.targetTable());
        } catch (Exception e) {
            pageSink.abort();
            throw e;
        } finally {
            pageSink.close();
        }
    }

    @Override
    public void close() {
        // No persistent resources; PageSink is created per batch.
    }

    private Page convertEnvelopesToPage(List<CdcEnvelope> envelopes) {
        if (envelopes.isEmpty()) {
            return new Page(List.of(), List.of());
        }

        CdcRow sampleRow = pickDataRow(envelopes.get(0));
        List<String> columns = sampleRow != null
                ? List.copyOf(sampleRow.columnNames())
                : List.of();

        List<TypedRow> rows = new ArrayList<>(envelopes.size());
        for (CdcEnvelope envelope : envelopes) {
            CdcRow dataRow = pickDataRow(envelope);
            if (dataRow == null) {
                continue;
            }
            TypedRow.Builder builder = TypedRow.builder();
            for (String col : columns) {
                builder.putRaw(col, dataRow.get(col));
            }
            rows.add(builder.build());
        }
        return new Page(columns, rows);
    }

    private static CdcRow pickDataRow(CdcEnvelope envelope) {
        if (envelope.after() != null) {
            return envelope.after();
        }
        return null;
    }
}
