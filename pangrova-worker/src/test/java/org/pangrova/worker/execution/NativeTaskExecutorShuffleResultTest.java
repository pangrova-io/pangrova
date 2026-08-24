package org.pangrova.worker.execution;


import java.nio.charset.StandardCharsets;

import org.pangrova.protocol.v1.BatchResultStatus;
import org.pangrova.spi.execution.BatchResult;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * P0-FPV-11: Tests that getShuffleBatchResult() correctly parses the
 * structured byte array returned by nativeGetShuffleBatch.
 *
 * <p>The structured format is identical to the result path (A2.1):
 * [status_byte | data...] where status bytes use {@link BatchResultStatus}
 * proto enum values. For DATA status, the remaining bytes carry
 * [4-byte partition_id BE][serialized shuffle data].</p>
 *
 * <p>Uses reflection to invoke the private parseStructuredResult method
 * since it cannot be tested through JNI without the native library.</p>
 */
class NativeTaskExecutorShuffleResultTest {

    private static final byte STATUS_DATA = (byte) BatchResultStatus.BATCH_RESULT_STATUS_DATA_VALUE;
    private static final byte STATUS_EOF = (byte) BatchResultStatus.BATCH_RESULT_STATUS_EOF_VALUE;
    private static final byte STATUS_NOT_READY = (byte) BatchResultStatus.BATCH_RESULT_STATUS_NOT_READY_VALUE;
    private static final byte STATUS_ERROR = (byte) BatchResultStatus.BATCH_RESULT_STATUS_ERROR_VALUE;

    @Test
    void shuffleDataStatusPreservesPartitionHeaderAndPayload()  {
        // Simulate: [DATA] [partition_id=7 BE] [0xAA, 0xBB]
        byte[] raw = {STATUS_DATA, 0x00, 0x00, 0x00, 0x07, (byte) 0xAA, (byte) 0xBB};
        BatchResult result = NativeTaskExecutor.parseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.DATA);
        // Data should include partition header + payload (status byte stripped)
        assertThat(result.copyData()).isEqualTo(new byte[]{0x00, 0x00, 0x00, 0x07, (byte) 0xAA, (byte) 0xBB});
    }

    @Test
    void shuffleEofStatus()  {
        byte[] raw = {STATUS_EOF};
        BatchResult result = NativeTaskExecutor.parseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
        assertThat(result.data()).isNull();
    }

    @Test
    void shuffleNotReadyStatus()  {
        byte[] raw = {STATUS_NOT_READY};
        BatchResult result = NativeTaskExecutor.parseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.NOT_READY);
        assertThat(result.data()).isNull();
    }

    @Test
    void shuffleErrorStatusWithMessage()  {
        String errorMsg = "Shuffle queue overflow";
        byte[] msgBytes = errorMsg.getBytes(StandardCharsets.UTF_8);
        byte[] raw = new byte[1 + msgBytes.length];
        raw[0] = STATUS_ERROR;
        System.arraycopy(msgBytes, 0, raw, 1, msgBytes.length);

        BatchResult result = NativeTaskExecutor.parseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.ERROR);
        assertThat(result.errorMessage()).isEqualTo("Shuffle queue overflow");
    }

    @Test
    void shuffleErrorStatusWithoutMessage()  {
        byte[] raw = {STATUS_ERROR};
        BatchResult result = NativeTaskExecutor.parseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.ERROR);
        assertThat(result.errorMessage()).isEqualTo("Native execution error");
    }

    @Test
    void shuffleNullReturnTreatedAsEof()  {
        BatchResult result = NativeTaskExecutor.parseStructuredResult(null);

        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
    }

    @Test
    void shuffleEmptyArrayTreatedAsEof()  {
        BatchResult result = NativeTaskExecutor.parseStructuredResult(new byte[0]);

        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
    }

    @Test
    void shuffleUnknownStatusTreatedAsError()  {
        byte[] raw = {(byte) 42};
        BatchResult result = NativeTaskExecutor.parseStructuredResult(raw);

        assertThat(result.status()).isEqualTo(BatchResult.Status.ERROR);
        assertThat(result.errorMessage()).contains("Unknown JNI status byte");
    }
}
