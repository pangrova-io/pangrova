package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates ExchangeSourceDescriptor readMode field serialization and backward compatibility.
 * Covers Task 23: ExchangeSourceDescriptor DTO readMode enhancement.
 */
class ExchangeSourceReadModeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("7-arg constructor sets readMode")
    void fullConstructorSetsReadMode() {
        ExchangeSourceDescriptor src = new ExchangeSourceDescriptor(
                "task-1", "host-1", 9090, 0, "SHUFFLE", 1L, "BLOCKING");
        assertEquals("BLOCKING", src.readMode());
    }

    @Test
    @DisplayName("6-arg backward-compatible constructor defaults to BLOCKING")
    void backwardCompatibleConstructorDefaultsToBlocking() {
        ExchangeSourceDescriptor src = new ExchangeSourceDescriptor(
                "task-1", "host-1", 9090, 0, "SHUFFLE", 1L);
        assertEquals("BLOCKING", src.readMode());
    }

    @Test
    @DisplayName("STREAMING readMode preserved through serialization")
    void streamingReadModeRoundTrip() throws Exception {
        ExchangeSourceDescriptor original = new ExchangeSourceDescriptor(
                "task-1", "host-1", 9090, 0, "BROADCAST", 1L, "STREAMING");
        byte[] bytes = mapper.writeValueAsBytes(original);
        ExchangeSourceDescriptor deserialized = mapper.readValue(bytes, ExchangeSourceDescriptor.class);
        assertEquals("STREAMING", deserialized.readMode());
    }

    @Test
    @DisplayName("BLOCKING readMode preserved through serialization")
    void blockingReadModeRoundTrip() throws Exception {
        ExchangeSourceDescriptor original = new ExchangeSourceDescriptor(
                "task-1", "host-1", 9090, 0, "HASH", 1L, "BLOCKING");
        byte[] bytes = mapper.writeValueAsBytes(original);
        ExchangeSourceDescriptor deserialized = mapper.readValue(bytes, ExchangeSourceDescriptor.class);
        assertEquals("BLOCKING", deserialized.readMode());
    }

    @Test
    @DisplayName("Null readMode in JSON deserialized as null")
    void nullReadModeDeserializedAsNull() throws Exception {
        String json = """
                {"source_task_id":"t1","host":"h1","transport_port":9090,
                 "partition_id":0,"exchange_type":"SHUFFLE","sequence_id":1}
                """;
        ExchangeSourceDescriptor src = mapper.readValue(json, ExchangeSourceDescriptor.class);
        assertNull(src.readMode());
    }
}
