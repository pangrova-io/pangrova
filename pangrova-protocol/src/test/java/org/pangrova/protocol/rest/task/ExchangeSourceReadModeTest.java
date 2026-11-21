package org.pangrova.protocol.rest.task;

import org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.pangrova.protocol.rest.task.ProtocolTestFixtures.exchangeSource;

/**
 * Validates ExchangeSourceDescriptor readMode field serialization and backward compatibility.
 * Covers Task 23: ExchangeSourceDescriptor DTO readMode enhancement.
 */
class ExchangeSourceReadModeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("canonical descriptor sets readMode")
    void fullConstructorSetsReadMode() {
        ExchangeSourceDescriptor src = exchangeSource(
                "task-1", "host-1", 9090, 0, "BLOCKING", 1L);
        assertEquals("BLOCKING", src.readMode());
    }

    @Test
    @DisplayName("null readMode defaults to BLOCKING")
    void nullReadModeDefaultsToBlocking() {
        ExchangeSourceDescriptor src = exchangeSource(
                "task-1", "host-1", 9090, 0, null, 1L);
        assertEquals("BLOCKING", src.readMode());
    }

    @Test
    @DisplayName("STREAMING readMode preserved through serialization")
    void streamingReadModeRoundTrip() throws Exception {
        ExchangeSourceDescriptor original = exchangeSource(
                "task-1", "host-1", 9090, 0, "STREAMING", 1L);
        byte[] bytes = mapper.writeValueAsBytes(original);
        ExchangeSourceDescriptor deserialized = mapper.readValue(bytes, ExchangeSourceDescriptor.class);
        assertEquals("STREAMING", deserialized.readMode());
    }

    @Test
    @DisplayName("BLOCKING readMode preserved through serialization")
    void blockingReadModeRoundTrip() throws Exception {
        ExchangeSourceDescriptor original = exchangeSource(
                "task-1", "host-1", 9090, 0, "BLOCKING", 1L);
        byte[] bytes = mapper.writeValueAsBytes(original);
        ExchangeSourceDescriptor deserialized = mapper.readValue(bytes, ExchangeSourceDescriptor.class);
        assertEquals("BLOCKING", deserialized.readMode());
    }

    @Test
    @DisplayName("missing readMode in otherwise complete JSON defaults to BLOCKING")
    void missingReadModeDeserializedAsBlocking() throws Exception {
        String json = mapper.writeValueAsString(exchangeSource(
                "task-1", "host-1", 9090, 0, "BLOCKING", 1L));
        String withoutReadMode = json.replace(",\"read_mode\":\"BLOCKING\"", "");
        ExchangeSourceDescriptor src = mapper.readValue(
                withoutReadMode, ExchangeSourceDescriptor.class);
        assertEquals("BLOCKING", src.readMode());
    }
}
