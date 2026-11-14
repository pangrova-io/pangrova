package org.pangrova.streaming.format;

import org.junit.jupiter.api.Test;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcFormatDiscoveryTest {
    @Test
    void packagedServiceDescriptorSelectsAndDecodesEachFormat() {
        var registry = CdcRecordConverterRegistry.discover(getClass().getClassLoader());
        assertThat(registry.supportedFormats()).containsExactlyInAnyOrder("canal-json", "debezium-json", "maxwell-json");
        record Example(String format, String payload, List<Integer> ids) {}
        var examples = List.of(
                new Example("canal-json", """
                        {"database":"inventory","table":"items","type":"INSERT","data":[{"id":1},{"id":2}]}
                        """, List.of(1, 2)),
                new Example("debezium-json", """
                        {"op":"c","source":{"db":"inventory","table":"items"},"after":{"id":1}}
                        """, List.of(1)),
                new Example("maxwell-json", """
                        {"database":"inventory","table":"items","type":"insert","data":{"id":1}}
                        """, List.of(1)));
        for (var example : examples) {
            var decoder = registry.get(example.format());
            assertThat(registry.get(example.format())).isSameAs(decoder);
            var events = decoder.convertBatch(List.of(example.payload().getBytes(StandardCharsets.UTF_8)),
                    (payload, error) -> { throw new AssertionError("Rejected valid " + example.format(), error); });
            assertThat(events).allSatisfy(event -> {
                assertThat(event.tableName()).isEqualTo("inventory.items");
                assertThat(event.operation()).isEqualTo(CdcOperation.INSERT);
            });
            assertThat(events).extracting(event -> event.after().get("id")).containsExactlyElementsOf(example.ids());
        }
        assertThatThrownBy(() -> registry.get("debezium-avro"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("debezium-avro");
    }
}
