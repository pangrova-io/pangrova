package org.pangrova.spi.execution;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WriteVectorizationMode} enum.
 */
class WriteVectorizationModeTest {

    @Test
    void allModesAreDefined() {
        assertThat(WriteVectorizationMode.values())
                .containsExactly(
                        WriteVectorizationMode.ROW_WISE,
                        WriteVectorizationMode.BATCH_COLUMNAR,
                        WriteVectorizationMode.STREAMING_COLUMNAR);
    }

    @Test
    void valueOfRoundTrips() {
        for (WriteVectorizationMode mode : WriteVectorizationMode.values()) {
            assertThat(WriteVectorizationMode.valueOf(mode.name())).isEqualTo(mode);
        }
    }

    @Test
    void rowWiseIsDefault() {
        // ROW_WISE should be ordinal 0 — the current (default) behaviour
        assertThat(WriteVectorizationMode.ROW_WISE.ordinal()).isZero();
    }

    @Test
    void configValueParsingFromSnakeCase() {
        // Config files use snake_case; valueOf expects the exact enum name
        String configValue = "row_wise";
        WriteVectorizationMode resolved =
                WriteVectorizationMode.valueOf(configValue.toUpperCase());
        assertThat(resolved).isEqualTo(WriteVectorizationMode.ROW_WISE);
    }

    @Test
    void configValueParsingBatchColumnar() {
        String configValue = "batch_columnar";
        WriteVectorizationMode resolved =
                WriteVectorizationMode.valueOf(configValue.toUpperCase());
        assertThat(resolved).isEqualTo(WriteVectorizationMode.BATCH_COLUMNAR);
    }

    @Test
    void configValueParsingStreamingColumnar() {
        String configValue = "streaming_columnar";
        WriteVectorizationMode resolved =
                WriteVectorizationMode.valueOf(configValue.toUpperCase());
        assertThat(resolved).isEqualTo(WriteVectorizationMode.STREAMING_COLUMNAR);
    }
}
