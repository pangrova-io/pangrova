package org.pangrova.coordinator.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies R10.4: WorkerResource rejects workers with incompatible protocol versions.
 */
class WorkerResourceVersionCheckTest {

    private WorkerResource resource;

    @BeforeEach
    void setUp() {
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        resource = new WorkerResource(workerManager);
    }

    @Test
    void register_compatibleVersion1_succeeds() {
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-1", "host-1", 8080, 9090, 0,
                1, List.of(), null, null, null);

        Response response = resource.register(request);

        assertThat(response.getStatus()).isEqualTo(200);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertThat(body.get("status")).isEqualTo("registered");
    }

    @Test
    void register_compatibleVersion2_succeeds() {
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-2", "host-2", 8080, 9090, 0,
                2, List.of(), null, null, null);

        Response response = resource.register(request);

        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void register_incompatibleVersion0_normalizedTo1_succeeds() {
        // protocolVersion=0 is normalized to 1 by the record constructor,
        // so this will actually succeed (version 1 is compatible).
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-3", "host-3", 8080, 9090, 0,
                0, List.of(), null, null, null);

        Response response = resource.register(request);
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void register_incompatibleVersionTooHigh_rejected409() {
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-4", "host-4", 8080, 9090, 0,
                99, List.of(), null, null, null);

        Response response = resource.register(request);

        assertThat(response.getStatus()).isEqualTo(409);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertThat(body.get("status")).isEqualTo("error");
        assertThat((String) body.get("message")).contains("Incompatible protocol version");
        assertThat(body.get("minSupportedVersion")).isEqualTo(WorkerResource.MIN_SUPPORTED_PROTOCOL_VERSION);
        assertThat(body.get("maxSupportedVersion")).isEqualTo(WorkerResource.MAX_SUPPORTED_PROTOCOL_VERSION);
    }

    @Test
    void isProtocolVersionCompatible_withinRange_returnsTrue() {
        assertThat(WorkerResource.isProtocolVersionCompatible(1)).isTrue();
        assertThat(WorkerResource.isProtocolVersionCompatible(2)).isTrue();
    }

    @Test
    void isProtocolVersionCompatible_outsideRange_returnsFalse() {
        assertThat(WorkerResource.isProtocolVersionCompatible(0)).isFalse();
        assertThat(WorkerResource.isProtocolVersionCompatible(-1)).isFalse();
        assertThat(WorkerResource.isProtocolVersionCompatible(3)).isFalse();
        assertThat(WorkerResource.isProtocolVersionCompatible(99)).isFalse();
    }
}
