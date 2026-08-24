package org.pangrova.worker.lance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI bridge to Lance C FFI (liblance_c_ffi).
 *
 * <p>Exposes three core vector index operations required by Task 26.14:
 * <ul>
 *   <li>{@link #createVectorIndex} — build an ANN index on a Lance column</li>
 *   <li>{@link #searchVector} — k-NN search against a Lance vector index</li>
 *   <li>{@link #dropIndex} — remove an existing vector index</li>
 * </ul>
 *
 * <p>Additionally exposes reader/writer operations for general Lance dataset
 * access (used by the native execution engine for .lance file scans).
 *
 * <p>All methods use the singleton Tokio runtime managed by {@link LanceFfiBootstrap}.
 * Thread-safe: all native methods are stateless or operate on opaque handles.
 *
 * <p>Error handling: native methods return negative values on failure. Call
 * {@link #getLastError()} to retrieve the error message from the Rust layer.
 */
public final class LanceFfiBridge {

    private static final Logger LOG = LoggerFactory.getLogger(LanceFfiBridge.class);

    private LanceFfiBridge() {}

    // =========================================================================
    // Vector Index Operations (Task 26.14)
    // =========================================================================

    /**
     * Create a vector index on a Lance dataset column.
     *
     * <p>Builds an IVF-PQ or HNSW index depending on the specified parameters.
     * The index is persisted alongside the Lance dataset on storage.
     *
     * @param datasetUri    URI of the Lance dataset (local path or object store)
     * @param columnName    Name of the vector column to index
     * @param indexType     Index algorithm: "IVF_PQ" or "HNSW"
     * @param metric        Distance metric: "L2", "COSINE", or "DOT"
     * @param numPartitions Number of IVF partitions (ignored for HNSW)
     * @param numSubVectors Number of PQ sub-vectors (ignored for HNSW)
     * @return 0 on success, -1 on error (check {@link #getLastError()})
     * @throws IllegalStateException if Lance FFI is not initialized
     */
    public static int createVectorIndex(
            String datasetUri,
            String columnName,
            String indexType,
            String metric,
            int numPartitions,
            int numSubVectors) {
        ensureInitialized();
        return nativeCreateVectorIndex(
                LanceFfiBootstrap.getRuntimeHandle(),
                datasetUri, columnName, indexType, metric,
                numPartitions, numSubVectors);
    }

    /**
     * Perform k-NN vector search against a Lance vector index.
     *
     * <p>Returns row IDs and distances for the top-K nearest neighbors.
     * The result is encoded as a flat array: [rowId0, dist0, rowId1, dist1, ...].
     *
     * @param datasetUri  URI of the Lance dataset
     * @param columnName  Name of the indexed vector column
     * @param queryVector Query vector (float array, dimension must match index)
     * @param topK        Number of nearest neighbors to return
     * @param numProbes   Number of IVF partitions to probe (trade accuracy/speed)
     * @param filter      Optional SQL-like filter expression (null for no filter)
     * @return Array of [rowId, distance] pairs (length = topK * 2),
     *         or null on error (check {@link #getLastError()})
     * @throws IllegalStateException if Lance FFI is not initialized
     */
    public static double[] searchVector(
            String datasetUri,
            String columnName,
            float[] queryVector,
            int topK,
            int numProbes,
            String filter) {
        ensureInitialized();
        return nativeSearchVector(
                LanceFfiBootstrap.getRuntimeHandle(),
                datasetUri, columnName, queryVector, topK, numProbes, filter);
    }

    /**
     * Drop (delete) a vector index from a Lance dataset.
     *
     * @param datasetUri URI of the Lance dataset
     * @param indexName  Name of the index to drop (as returned by index metadata)
     * @return 0 on success, -1 on error (check {@link #getLastError()})
     * @throws IllegalStateException if Lance FFI is not initialized
     */
    public static int dropIndex(String datasetUri, String indexName) {
        ensureInitialized();
        return nativeDropIndex(
                LanceFfiBootstrap.getRuntimeHandle(), datasetUri, indexName);
    }

    // =========================================================================
    // Reader Operations
    // =========================================================================

    /**
     * Open a Lance dataset for sequential reading.
     *
     * @param uri URI of the Lance dataset
     * @return Opaque reader handle (positive), or 0 on failure
     */
    public static long readerOpen(String uri) {
        ensureInitialized();
        return nativeReaderOpen(LanceFfiBootstrap.getRuntimeHandle(), uri);
    }

    /**
     * Set column projection on the reader.
     *
     * @param readerHandle Handle from {@link #readerOpen}
     * @param columns      Comma-separated column names
     * @return 0 on success, -1 on error
     */
    public static int readerSetProjection(long readerHandle, String columns) {
        return nativeReaderSetProjection(readerHandle, columns);
    }

    /**
     * Set a filter expression on the reader.
     *
     * @param readerHandle Handle from {@link #readerOpen}
     * @param filter       SQL-like filter expression
     * @return 0 on success, -1 on error
     */
    public static int readerSetFilter(long readerHandle, String filter) {
        return nativeReaderSetFilter(readerHandle, filter);
    }

    /**
     * Read the next batch as columnar IPC bytes.
     *
     * @param readerHandle Handle from {@link #readerOpen}
     * @param maxRows      Maximum rows per batch (0 for default 8192)
     * @return columnar IPC serialized batch bytes, or null if EOF or error
     */
    public static byte[] readerNext(long readerHandle, int maxRows) {
        return nativeReaderNext(readerHandle, maxRows);
    }

    /**
     * Close and release the reader handle.
     *
     * @param readerHandle Handle from {@link #readerOpen}
     */
    public static void readerClose(long readerHandle) {
        if (readerHandle != 0) {
            nativeReaderClose(readerHandle);
        }
    }

    // =========================================================================
    // Writer Operations
    // =========================================================================

    /**
     * Open a Lance dataset writer.
     *
     * @param uri              Output URI
     * @param arrowSchemaBytes columnar IPC schema bytes
     * @param append           true to append, false to create new
     * @return Opaque writer handle (positive), or 0 on failure
     */
    public static long writerOpen(String uri, byte[] arrowSchemaBytes, boolean append) {
        ensureInitialized();
        return nativeWriterOpen(
                LanceFfiBootstrap.getRuntimeHandle(), uri, arrowSchemaBytes, append);
    }

    /**
     * Write a record batch to the dataset.
     *
     * @param writerHandle Handle from {@link #writerOpen}
     * @param batchBytes   columnar IPC serialized batch bytes
     * @return 0 on success, -1 on error
     */
    public static int writerWriteBatch(long writerHandle, byte[] batchBytes) {
        return nativeWriterWriteBatch(writerHandle, batchBytes);
    }

    /**
     * Flush and finalize the dataset on storage.
     *
     * @param writerHandle Handle from {@link #writerOpen}
     * @return 0 on success, -1 on error
     */
    public static int writerFinish(long writerHandle) {
        return nativeWriterFinish(writerHandle);
    }

    /**
     * Close and release the writer handle.
     *
     * @param writerHandle Handle from {@link #writerOpen}
     */
    public static void writerClose(long writerHandle) {
        if (writerHandle != 0) {
            nativeWriterClose(writerHandle);
        }
    }

    // =========================================================================
    // Error Handling
    // =========================================================================

    /**
     * Retrieve the last error message from the Rust FFI layer.
     *
     * @return Error message string, or null if no error occurred since last call
     */
    public static String getLastError() {
        return nativeGetLastError();
    }

    // =========================================================================
    // Internal
    // =========================================================================

    private static void ensureInitialized() {
        if (!LanceFfiBootstrap.isAvailable()) {
            throw new IllegalStateException(
                    "Lance FFI not available. Ensure LanceFfiBootstrap.initialize() "
                    + "succeeded during worker startup.");
        }
    }

    // -- Native method declarations --

    // Vector index operations
    private static native int nativeCreateVectorIndex(
            long runtimeHandle, String datasetUri, String columnName,
            String indexType, String metric, int numPartitions, int numSubVectors);

    private static native double[] nativeSearchVector(
            long runtimeHandle, String datasetUri, String columnName,
            float[] queryVector, int topK, int numProbes, String filter);

    private static native int nativeDropIndex(
            long runtimeHandle, String datasetUri, String indexName);

    // Reader operations
    private static native long nativeReaderOpen(long runtimeHandle, String uri);
    private static native int nativeReaderSetProjection(long readerHandle, String columns);
    private static native int nativeReaderSetFilter(long readerHandle, String filter);
    private static native byte[] nativeReaderNext(long readerHandle, int maxRows);
    private static native void nativeReaderClose(long readerHandle);

    // Writer operations
    private static native long nativeWriterOpen(
            long runtimeHandle, String uri, byte[] arrowSchemaBytes, boolean append);
    private static native int nativeWriterWriteBatch(long writerHandle, byte[] batchBytes);
    private static native int nativeWriterFinish(long writerHandle);
    private static native void nativeWriterClose(long writerHandle);

    // Error
    private static native String nativeGetLastError();
}
