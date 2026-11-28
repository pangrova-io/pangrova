package org.pangrova.protocol.rest.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link HeartbeatRequest} DTO.
 */
class HeartbeatRequestTest {
    private static final org.pangrova.protocol.execution.identity.WorkerExecutionIdentity HEARTBEAT_IDENTITY = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");


    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void roundTripPreservesAllFields() throws Exception {
        HeartbeatRequest original = new HeartbeatRequest("w-1", "LOW", 0.75, 3, 4096L,
                List.of("/file"), List.of(new TenantUsageSnapshot("tenant", 512, 1)), HEARTBEAT_IDENTITY);
        byte[] bytes = mapper.writeValueAsBytes(original);
        HeartbeatRequest restored = mapper.readValue(bytes, HeartbeatRequest.class);

        assertThat(restored).isEqualTo(original);
        var fields = new ArrayList<String>();
        mapper.readTree(bytes).fieldNames().forEachRemaining(fields::add);
        assertThat(fields)
                .containsExactlyInAnyOrder("worker_id", "memory_pressure", "cpu_utilization", "in_flight_task_count",
                        "available_memory_bytes", "cached_file_paths", "tenant_usage", "worker_execution_identity");
        assertThat(restored.memoryPressure()).isEqualTo("LOW");
        assertThat(restored.cpuUtilization()).isEqualTo(0.75);
        assertThat(restored.inFlightTaskCount()).isEqualTo(3);
        assertThat(restored.availableMemoryBytes()).isEqualTo(4096L);
    }

    @Test
    void deserializesSnakeCaseFields() throws Exception {
        String json = """
                {
                    "worker_id": "w-7",
                  "worker_execution_identity": {"admission_domain_id": "00000000-0000-0000-0000-0000000000d1", "incarnation_id": "00000000-0000-0000-0000-0000000000e1"},
                    "memory_pressure": "HIGH",
                    "cpu_utilization": 0.9,
                    "in_flight_task_count": 5,
                    "available_memory_bytes": 1024
                }
                """;
        HeartbeatRequest request = mapper.readValue(json, HeartbeatRequest.class);

        assertThat(request.workerId()).isEqualTo("w-7");
        assertThat(request.memoryPressure()).isEqualTo("HIGH");
        assertThat(request.cpuUtilization()).isEqualTo(0.9);
        assertThat(request.inFlightTaskCount()).isEqualTo(5);
        assertThat(request.availableMemoryBytes()).isEqualTo(1024L);
    }

    @Test
    void defaultsMemoryPressureToNoneWhenBlank() {
        HeartbeatRequest request = new HeartbeatRequest("w-1", "", 0.0, 0, 0L, HEARTBEAT_IDENTITY);
        assertThat(request.memoryPressure()).isEqualTo("NONE");
    }

    @Test
    void defaultsMemoryPressureToNoneWhenNull() {
        HeartbeatRequest request = new HeartbeatRequest("w-1", null, 0.0, 0, 0L, HEARTBEAT_IDENTITY);
        assertThat(request.memoryPressure()).isEqualTo("NONE");
    }

    @Test
    void throwsOnNullWorkerId() {
        assertThatThrownBy(() -> new HeartbeatRequest(null, "NONE", 0.0, 0, 0L, HEARTBEAT_IDENTITY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerId");
    }

    @Test
    void throwsOnBlankWorkerId() {
        assertThatThrownBy(() -> new HeartbeatRequest("  ", "NONE", 0.0, 0, 0L, HEARTBEAT_IDENTITY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerId");
    }
    @Test
    void missingExecutionIdentityIsRejectedInsteadOfInferringTheCurrentOwner() {
        assertThatThrownBy(() -> mapper.readValue("{\"worker_id\":\"worker\"}", HeartbeatRequest.class))
                .isInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class)
                .hasMessageContaining("workerExecutionIdentity");
    }

}
