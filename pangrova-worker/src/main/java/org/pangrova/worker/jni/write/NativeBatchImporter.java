package org.pangrova.worker.jni.write;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.jni.NativeBridgeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports columnar data interface structs into serialized record batch bytes.
 *
 * <p>Converts columnar data interface pointers to serialized IPC bytes for the
 * write path. The serialized data can be wrapped in a {@code NativePage} and
 * passed to {@code ConnectorPageSink.appendNativePage()}.</p>
 *
 * <p>The native side retains ownership of the columnar data interface structs;
 * this method copies the data into a Java byte array before returning.</p>
 */
final class NativeBatchImporter {

    private static final Logger LOG = LoggerFactory.getLogger(NativeBatchImporter.class);

    private NativeBatchImporter() {}

    /**
     * Serializes columnar data interface pointers into record batch bytes.
     *
     * @param arrowArrayPtr  memory address of the ArrowArray struct
     * @param arrowSchemaPtr memory address of the ArrowSchema struct
     * @return serialized record batch bytes
     * @throws NativeBridgeException if serialization fails
     */
    static byte[] importBatch(long arrowArrayPtr, long arrowSchemaPtr) {
        byte[] batchData = NativeRuntime.nativeSerializeBatchFromPointers(
                arrowArrayPtr, arrowSchemaPtr);
        if (batchData == null || batchData.length == 0) {
            throw new NativeBridgeException(
                    PangrovaErrorCode.NATIVE_DATA_INTERFACE_ERROR.name()
                            + ": Native batch serialization returned empty data for"
                            + " arrowArrayPtr=" + arrowArrayPtr
                            + " arrowSchemaPtr=" + arrowSchemaPtr);
        }
        LOG.debug("event=batch_imported arrowArrayPtr={} arrowSchemaPtr={} bytes={}",
                arrowArrayPtr, arrowSchemaPtr, batchData.length);
        return batchData;
    }

    /**
     * Extracts the row count from an ArrowArray struct.
     *
     * @param arrowArrayPtr memory address of the ArrowArray struct
     * @return number of rows in the batch
     */
    static int extractRowCount(long arrowArrayPtr) {
        return NativeRuntime.nativeGetBatchRowCount(arrowArrayPtr);
    }

    /**
     * Extracts the column count from an ArrowSchema struct.
     *
     * @param arrowSchemaPtr memory address of the ArrowSchema struct
     * @return number of columns in the batch
     */
    static int extractColumnCount(long arrowSchemaPtr) {
        return NativeRuntime.nativeGetBatchColumnCount(arrowSchemaPtr);
    }
}
