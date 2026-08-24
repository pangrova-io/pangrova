package org.pangrova.worker.heartbeat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies HeartbeatPayload JSON wire format uses snake_case field names,
 * ensuring Worker → Coordinator heartbeat round-trip compatibility.
 */
class HeartbeatPayloadWireFormatTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializationUsesSnakeCaseFieldNames() throws Exception {
        HeartbeatPayload payload = new HeartbeatPayload("w-1", "LOW", 0.75, 3, 4096L);
        String json = mapper.writeValueAsString(payload);
        JsonNode node = mapper.readTree(json);

        assertTrue(node.has("worker_id"), "Expected snake_case field 'worker_id'");
        assertTrue(node.has("memory_pressure"), "Expected snake_case field 'memory_pressure'");
        assertTrue(node.has("cpu_utilization"), "Expected snake_case field 'cpu_utilization'");
        assertTrue(node.has("running_task_count"), "Expected snake_case field 'running_task_count'");
        assertTrue(node.has("available_memory_bytes"), "Expected snake_case field 'available_memory_bytes'");
    }

    @Test
    void roundTripPreservesAllFields() throws Exception {
        HeartbeatPayload original = new HeartbeatPayload("w-42", "HIGH", 0.95, 8, 1_048_576L);
        byte[] bytes = mapper.writeValueAsBytes(original);
        HeartbeatPayload restored = mapper.readValue(bytes, HeartbeatPayload.class);

        assertEquals(original.workerId(), restored.workerId());
        assertEquals(original.memoryPressure(), restored.memoryPressure());
        assertEquals(original.cpuUtilization(), restored.cpuUtilization(), 0.001);
        assertEquals(original.runningTaskCount(), restored.runningTaskCount());
        assertEquals(original.availableMemoryBytes(), restored.availableMemoryBytes());
    }

    @Test
    void deserializationFromSnakeCaseJson() throws Exception {
        String json = """
                {
                  "worker_id": "w-7",
                  "memory_pressure": "NONE",
                  "cpu_utilization": 0.1,
                  "running_task_count": 0,
                  "available_memory_bytes": 512
                }
                """;
        HeartbeatPayload payload = mapper.readValue(json, HeartbeatPayload.class);

        assertEquals("w-7", payload.workerId());
        assertEquals("NONE", payload.memoryPressure());
        assertEquals(0.1, payload.cpuUtilization(), 0.001);
        assertEquals(0, payload.runningTaskCount());
        assertEquals(512L, payload.availableMemoryBytes());
    }
}
