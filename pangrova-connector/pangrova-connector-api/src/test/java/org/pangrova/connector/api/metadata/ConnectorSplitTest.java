package org.pangrova.connector.api.metadata;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConnectorSplit} default method contracts.
 */
class ConnectorSplitTest {

    @Test
    void splitProperties_defaultReturnsEmptyMap() {
        ConnectorSplit split = new ConnectorSplit() {};

        assertThat(split.splitProperties()).isEmpty();
    }

    @Test
    void typedSplitProperties_defaultBridgesFromSplitProperties() {
        ConnectorSplit split = new ConnectorSplit() {
            @Override
            public Map<String, String> splitProperties() {
                return Map.of(
                        "file_path", "/data/test.parquet",
                        "start_offset", "0",
                        "length", "1024"
                );
            }
        };

        SplitProperties props = split.typedSplitProperties();
        assertThat(props.filePath()).isEqualTo("/data/test.parquet");
        assertThat(props.offset()).isZero();
        assertThat(props.length()).isEqualTo(1024L);
    }
}
