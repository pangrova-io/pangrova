package org.pangrova.streaming.coordinator;

import org.junit.jupiter.api.Test;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.source.CdcSourceFactory;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcSourceRegistryTest {
    @Test
    void rejectsUnknownDuplicateAndIncompatibleSourcesBeforeOpeningClients() {
        var one = new TestFactory("feed", CdcSourceFactory.API_VERSION);
        assertThatThrownBy(() -> new CdcSourceRegistry(List.of(one, one)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate");
        assertThatThrownBy(() -> new CdcSourceRegistry(List.of(new TestFactory("feed", 42))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Incompatible");
        var registry = new CdcSourceRegistry(List.of(one));
        assertThatThrownBy(() -> registry.validate("unknown", Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown");
        registry.validate("feed", Map.of("channel", "orders"));
        assertThatThrownBy(() -> registry.validate("feed", Map.of("topic", "orders")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("channel");
    }

    private record TestFactory(String identifier, int apiVersion) implements CdcSourceFactory {
        @Override
        public void validate(Map<String, String> properties) {
            if (!properties.containsKey("channel")) {
                throw new IllegalArgumentException("channel is required");
            }
        }

        @Override
        public CdcSourceReader open(String checkpointId, Map<String, String> properties) {
            throw new AssertionError("No external reader should be opened by validation");
        }
    }
}
