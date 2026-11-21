package org.pangrova.protocol.rest.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShuffleConfigTest {
    @Test
    void omittedValuesUseOneCanonicalBatchFormat() {
        var config = new ShuffleConfig(null, null, 0, null, 3, List.of(1));
        assertThat(config.serdeFormat()).isEqualTo("ARROW_BATCH");
        assertThat(config.compressionCodec()).isEqualTo("lz4");
        assertThat(config.batchSize()).isEqualTo(10000);
        assertThat(config.persistenceMode()).isEqualTo("MEMORY_ONLY");
        assertThat(config.numPartitions()).isEqualTo(3);
        assertThat(config.partitionKeyIndices()).containsExactly(1);
        assertThat(new ShuffleConfig("zstd", 17).serdeFormat()).isEqualTo(config.serdeFormat());
        assertThat(new ShuffleConfig("none", 9, "MEMORY_AND_REMOTE").serdeFormat()).isEqualTo(config.serdeFormat());
    }

    @Test
    void rejectsRemovedAndNoncanonicalFormatsWithoutNormalization() {
        for (String format : List.of("PRESTO_PAGES", "NATIVE_ROW", "arrow_batch", "", " ARROW_BATCH")) {
            assertThatThrownBy(() -> new ShuffleConfig(format, "none", 1, "MEMORY_ONLY", 1, List.of()))
                    .as("format %s", format).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void validatesExactCodecAndPositiveExecutionRows() {
        for (String codec : List.of("none", "lz4", "zstd")) {
            var config = new ShuffleConfig("ARROW_BATCH", codec, Integer.MAX_VALUE, null, 1, List.of());
            assertThat(config.compressionCodec()).isEqualTo(codec);
            assertThat(config.batchSize()).isEqualTo(Integer.MAX_VALUE);
        }
        for (String codec : List.of("NONE", "LZ4", "ZSTD", "gzip", "")) {
            assertThatThrownBy(() -> new ShuffleConfig("ARROW_BATCH", codec, 1, null, 1, List.of()))
                    .as("codec %s", codec).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new ShuffleConfig("ARROW_BATCH", "none", -1, null, 1, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("batch size");
    }
}
