package org.pangrova.spi.execution;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BatchResult} factory methods and status semantics.
 */
class BatchResultTest {

    @Test
    void dataResultCarriesPayload() {
        byte[] payload = {0x01, 0x02, 0x03};
        BatchResult result = BatchResult.data(payload);

        assertThat(result.status()).isEqualTo(BatchResult.Status.DATA);
        assertThat(result.data()).isEqualTo(payload);
        assertThat(result.errorMessage()).isNull();
    }

    @Test
    void eofResultHasNoData() {
        BatchResult result = BatchResult.eof();

        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
        assertThat(result.data()).isNull();
        assertThat(result.errorMessage()).isNull();
    }

    @Test
    void notReadyResultHasNoData() {
        BatchResult result = BatchResult.notReady();

        assertThat(result.status()).isEqualTo(BatchResult.Status.NOT_READY);
        assertThat(result.data()).isNull();
        assertThat(result.errorMessage()).isNull();
    }

    @Test
    void errorResultCarriesMessage() {
        BatchResult result = BatchResult.error("OOM in Velox");

        assertThat(result.status()).isEqualTo(BatchResult.Status.ERROR);
        assertThat(result.data()).isNull();
        assertThat(result.errorMessage()).isEqualTo("OOM in Velox");
    }

}
