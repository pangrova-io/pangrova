package org.pangrova.coordinator.streaming;

import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcFormat;
import org.pangrova.spi.streaming.CdcWriteResult;

import java.util.List;

/**
 * Compiles a CDC batch transform into a Substrait plan and dispatches
 * it to a native worker for execution.
 *
 * <p>The compiled plan follows the pattern:
 * ReadRel(CDC envelopes) → TransformRel(format-specific parsing) → WriteRel(target table).
 *
 * <p>Used by {@link ConnectorPageSinkCdcWriter} when the batch size exceeds
 * the native transform threshold, offloading envelope parsing and row extraction
 * to Arrow columnar memory on the worker.</p>
 */
@FunctionalInterface
public interface CdcBatchDispatchFunction {

    /**
     * Compiles the CDC batch to a Substrait plan and dispatches to a worker.
     *
     * @param envelopes   CDC change events to transform and write
     * @param targetTable fully qualified target table name
     * @param format      CDC envelope format for the native parser
     * @return write result with metadata
     */
    CdcWriteResult apply(List<CdcEnvelope> envelopes, String targetTable, CdcFormat format);
}
