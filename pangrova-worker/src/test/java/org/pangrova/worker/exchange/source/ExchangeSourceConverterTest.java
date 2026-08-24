package org.pangrova.worker.exchange.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.junit.jupiter.api.Test;

/**
 * Verifies ExchangeSourceConverter.toSpiEndpoints() correctly maps
 * protocol ExchangeSourceDescriptor to SPI ExchangeEndpoint.
 * No mocks -- pure conversion logic.
 */
class ExchangeSourceConverterTest {

    @Test
    void toSpiEndpoints_preservesAllFields() {
        var src = new ExchangeSourceDescriptor(
                "task-1", "host-a", 9090, 3, "HASH_PARTITION", 42L);

        List<ExchangeEndpoint> result = ExchangeSourceConverter.toSpiEndpoints(List.of(src));

        assertThat(result).hasSize(1);
        ExchangeEndpoint ep = result.get(0);
        assertThat(ep.sourceTaskId()).isEqualTo("task-1");
        assertThat(ep.host()).isEqualTo("host-a");
        assertThat(ep.transportPort()).isEqualTo(9090);
        assertThat(ep.partitionId()).isEqualTo(3);
    }

    @Test
    void toSpiEndpoints_handlesMultipleSources() {
        var src1 = new ExchangeSourceDescriptor(
                "task-1", "host-a", 9090, 0, "BROADCAST", 1L);
        var src2 = new ExchangeSourceDescriptor(
                "task-2", "host-b", 9091, 1, "ROUND_ROBIN", 2L);

        List<ExchangeEndpoint> result =
                ExchangeSourceConverter.toSpiEndpoints(List.of(src1, src2));

        assertThat(result).hasSize(2);
        assertThat(result.get(0).sourceTaskId()).isEqualTo("task-1");
        assertThat(result.get(1).sourceTaskId()).isEqualTo("task-2");
        assertThat(result.get(1).host()).isEqualTo("host-b");
        assertThat(result.get(1).transportPort()).isEqualTo(9091);
    }

    @Test
    void toSpiEndpoints_returnsEmptyForNull() {
        assertThat(ExchangeSourceConverter.toSpiEndpoints(null)).isEmpty();
    }

    @Test
    void toSpiEndpoints_returnsEmptyForEmptyList() {
        assertThat(ExchangeSourceConverter.toSpiEndpoints(List.of())).isEmpty();
    }
}
