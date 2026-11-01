package org.pangrova.spi.connector.metadata;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConnectorSplit} default method contracts.
 */
class ConnectorSplitTest {

    @Test
    void toSplitInfoProto_defaultReturnsEmptyBytes() {
        ConnectorSplit split = new ConnectorSplit() {};

        byte[] result = split.toSplitInfoProto();

        assertThat(result).isNotNull().isEmpty();
    }

    @Test
    void toSplitInfoProto_defaultIsOverridable() {
        byte[] expected = new byte[]{0x0A, 0x02, 0x08, 0x01};
        ConnectorSplit split = new ConnectorSplit() {
            @Override
            public byte[] toSplitInfoProto() {
                return expected;
            }
        };

        assertThat(split.toSplitInfoProto()).isEqualTo(expected);
    }

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
