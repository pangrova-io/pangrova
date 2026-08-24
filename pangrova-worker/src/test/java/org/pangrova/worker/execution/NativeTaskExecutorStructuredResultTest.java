package org.pangrova.worker.execution;

import java.nio.charset.StandardCharsets;

import org.pangrova.protocol.v1.BatchResultStatus;
import org.pangrova.spi.execution.BatchResult;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * P0-FPV-10: Tests the structured byte parsing logic in NativeTaskExecutor.
 * Directly invokes the package-private parseStructuredResult method
 * since it cannot be tested through JNI without the native library.
 */
class NativeTaskExecutorStructuredResultTest {

    private static final byte STATUS_DATA = (byte) BatchResultStatus.BATCH_RESULT_STATUS_DATA_VALUE;
    private static final byte STATUS_EOF = (byte) BatchResultStatus.BATCH_RESULT_STATUS_EOF_VALUE;
    private static final byte STATUS_NOT_READY = (byte) BatchResultStatus.BATCH_RESULT_STATUS_NOT_READY_VALUE;
    private static final byte STATUS_ERROR = (byte) BatchResultStatus.BATCH_RESULT_STATUS_ERROR_VALUE;

    @Test
    void parseDataStatus() throws Exception {
        byte[] raw = {STATUS_DATA, 0x01, 0x02, 0x03};
        BatchResult result = invokeParseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.DATA);
        assertThat(result.copyData()).isEqualTo(new byte[]{0x01, 0x02, 0x03});
    }

    @Test
    void parseEofStatus() throws Exception {
        byte[] raw = {STATUS_EOF};
        BatchResult result = invokeParseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
        assertThat(result.data()).isNull();
    }

    @Test
    void parseNotReadyStatus() throws Exception {
        byte[] raw = {STATUS_NOT_READY};
        BatchResult result = invokeParseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.NOT_READY);
        assertThat(result.data()).isNull();
    }

    @Test
    void parseErrorStatusWithMessage() throws Exception {
        String errorMsg = "Velox OOM";
        byte[] msgBytes = errorMsg.getBytes(StandardCharsets.UTF_8);
        byte[] raw = new byte[1 + msgBytes.length];
        raw[0] = STATUS_ERROR;
        System.arraycopy(msgBytes, 0, raw, 1, msgBytes.length);

        BatchResult result = invokeParseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.ERROR);
        assertThat(result.errorMessage()).isEqualTo("Velox OOM");
    }

    @Test
    void parseErrorStatusWithoutMessage() throws Exception {
        byte[] raw = {STATUS_ERROR};
        BatchResult result = invokeParseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.ERROR);
        assertThat(result.errorMessage()).isEqualTo("Native execution error");
    }

    @Test
    void parseNullReturnsTreatedAsEof() throws Exception {
        BatchResult result = invokeParseStructuredResult(null);

        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
    }

    @Test
    void parseEmptyArrayTreatedAsEof() throws Exception {
        BatchResult result = invokeParseStructuredResult(new byte[0]);

        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
    }

    @Test
    void parseUnknownStatusTreatedAsError() throws Exception {
        byte[] raw = {(byte) 99};
        BatchResult result = invokeParseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.ERROR);
        assertThat(result.errorMessage()).contains("Unknown JNI status byte");
    }

    @Test
    void parseDataStatusEmptyPayload() throws Exception {
        byte[] raw = {STATUS_DATA};
        BatchResult result = invokeParseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.DATA);
        assertThat(result.dataLength()).isZero();
    }

    /**
     * Directly calls the package-private NativeTaskExecutor.parseStructuredResult.
     */
    private static BatchResult invokeParseStructuredResult(byte[] raw) {
        return NativeTaskExecutor.parseStructuredResult(raw);
    }
}
