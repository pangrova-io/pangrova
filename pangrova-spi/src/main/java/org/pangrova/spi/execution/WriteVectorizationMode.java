package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

/**
 * Controls the data transfer mode at the JNI boundary for write operations
 * (INSERT, MERGE sink, COMPACT, etc.).
 *
 * <h3>Problem statement (G-EXEC-06)</h3>
 * <p>The current write path crosses the JNI boundary with a column-to-row
 * materialisation step: the native engine produces columnar Arrow
 * batches, but the Java-side Iceberg/Paimon writer SDK expects row-oriented
 * records. This materialisation negates the vectorization benefit gained on
 * the read/compute side, adding measurable overhead for write-heavy workloads
 * (INSERT-SELECT, MERGE, CTAS).</p>
 *
 * <h3>Future modes</h3>
 * <p>{@link #BATCH_COLUMNAR} and {@link #STREAMING_COLUMNAR} describe two
 * vectorized alternatives that pass Arrow columnar batches through the JNI
 * boundary to columnar file-format writers (Parquet column writers, ORC stripe
 * writers) without row materialisation.</p>
 *
 * <p>Configuration key: {@code execution.write-vectorization}</p>
 *
 * @see org.pangrova.spi.connector.data.ConnectorPageSink
 */
@Experimental(reason = "G-EXEC-06: extension point for eliminating column-to-row conversion on write path")
public enum WriteVectorizationMode {

    /**
     * Column-to-row materialisation at the JNI boundary (current behaviour).
     *
     * <p>Arrow columnar batches are converted to row-oriented records before
     * being handed to the Java connector page-sink. Safe and compatible with
     * all existing connectors but incurs serialisation overhead.</p>
     */
    ROW_WISE,

    /**
     * Accumulate complete Arrow columnar batches and write them in bulk to
     * the underlying columnar file format (e.g., Parquet row groups).
     *
     * <p>Not yet implemented. Requires connectors to implement a columnar
     * page-sink variant that can write Arrow batches directly to file-format
     * column writers without intermediate row materialisation. Batches are
     * buffered until a configurable threshold before flushing, which
     * optimises I/O at the cost of higher memory usage.</p>
     */
    BATCH_COLUMNAR,

    /**
     * Stream individual Arrow columnar batches through to the underlying
     * columnar file format as soon as they arrive, without buffering.
     *
     * <p>Not yet implemented. Like {@link #BATCH_COLUMNAR}, this avoids
     * row materialisation, but writes each batch immediately rather than
     * accumulating. This trades optimal I/O patterns for lower memory
     * usage and reduced latency, suitable for streaming or memory-constrained
     * workloads.</p>
     */
    STREAMING_COLUMNAR
}
