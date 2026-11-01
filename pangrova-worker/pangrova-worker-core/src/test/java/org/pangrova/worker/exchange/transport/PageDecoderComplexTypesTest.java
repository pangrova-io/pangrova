package org.pangrova.worker.exchange.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures;

class PageDecoderComplexTypesTest {

    @Test
    void decodesNestedArrayStructAndMapWithTypedKeys() throws IOException {
        try (var allocator = new RootAllocator(16L * 1024 * 1024)) {
            Page page = PageDecoder.decode(WriteBatchFixtures.complex(false), allocator);

            assertThat(page.getPositionCount()).isEqualTo(2);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("label", "nested");
            item.put("quantity", null);
            assertThat(page.rows().get(0).getRaw("items"))
                    .isEqualTo(Arrays.asList(item, null));
            Map<Integer, Object> attributes = new LinkedHashMap<>();
            attributes.put(-7, "typed-key");
            attributes.put(9, null);
            assertThat(page.rows().get(0).getRaw("attributes"))
                    .isEqualTo(attributes);
            assertThat(page.rows().get(1).getRaw("attributes")).isNull();
        }
    }

    @Test
    void rejectsDuplicateMapKeysInsteadOfDroppingAnEntry() throws IOException {
        try (var allocator = new RootAllocator(16L * 1024 * 1024)) {
            byte[] stream = WriteBatchFixtures.complex(true);
            assertThatThrownBy(() -> PageDecoder.decode(stream, allocator))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("duplicate key");
        }
    }
}
