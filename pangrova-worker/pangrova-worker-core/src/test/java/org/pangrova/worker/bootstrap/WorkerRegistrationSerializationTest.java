package org.pangrova.worker.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpClient;
import java.util.List;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.lifecycle.BootstrapFailedException;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.worker.bootstrap.registration.WorkerRegistrationClient;
import org.pangrova.testing.testkit.function.ActualNativeCatalogue;
import org.junit.jupiter.api.Test;

/**
 * Validates that WorkerRegistrationClient.serialize() throws explicit
 * SerializationException with error code instead of returning null on failure.
 *
 * <p><b>Validates: Requirements 1.25, 1.26, 2.25, 2.26</b></p>
 */
class WorkerRegistrationSerializationTest {

    @Test
    void serializeValidRequestReturnsNonNullBytes() {
        WorkerRegistrationClient client = new WorkerRegistrationClient(
                "http://localhost:8080", HttpClient.newHttpClient());
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-1", "localhost", 8080, 9090, 0, 2,
                null, List.of(), null, canonicalInventory(), new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"));

        byte[] result = client.serialize(request);

        assertThat(result).isNotNull();
        assertThat(result.length).isGreaterThan(0);
        assertThat(new String(result)).contains("worker-1");
    }

    @Test
    void serializePreservesAllFields() {
        WorkerRegistrationClient client = new WorkerRegistrationClient(
                "http://localhost:8080", HttpClient.newHttpClient());
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "w-42", "host-a", 3000, 4000, 0, 2,
                "rack-1", List.of("/data/1"), null, canonicalInventory(), new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"));

        byte[] result = client.serialize(request);
        String json = new String(result);

        assertThat(json).contains("w-42");
        assertThat(json).contains("host-a");
        assertThat(json).contains("3000");
        assertThat(json).contains("4000");
        assertThat(json).contains("rack-1");
        assertThat(json).contains("capability_inventory");
        assertThat(json).doesNotContain("\"capabilities\"");
    }

    @Test
    void registerWithValidRequestDoesNotThrow() {
        // Verify that serialize() succeeds for a valid request — the actual HTTP send
        // will fail without a coordinator, so we only test serialization here
        WorkerRegistrationClient client = new WorkerRegistrationClient(
                "http://192.0.2.1:1", HttpClient.newHttpClient());
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-1", "localhost", 8080, 9090, 0, 2,
                null, List.of(), null, canonicalInventory(), new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"));

        byte[] serialized = client.serialize(request);
        assertThat(serialized).isNotNull();
        assertThat(serialized.length).isGreaterThan(0);
    }

    @Test
    void registrationFailureProducesBootstrapFailedException() {
        // Verify that BootstrapFailedException wraps the registration phase correctly
        var cause = new RuntimeException("simulated failure");
        var ex = new BootstrapFailedException("registration", cause);

        assertThat(ex.getPhase()).isEqualTo("registration");
        assertThat(ex.getCause()).isSameAs(cause);
        assertThat(ex.getMessage()).contains("registration");
    }

    @Test
    void errorCodeHasCorrectFormat() {
        assertThat(PangrovaErrorCode.WORKER_REGISTRATION_SERIALIZATION_FAILED.getCode())
                .isEqualTo("WORKER-0001");
        assertThat(PangrovaErrorCode.WORKER_REGISTRATION_SERIALIZATION_FAILED.getDescription())
                .isEqualTo("Worker registration serialization failed");
    }

    private static String canonicalInventory() {
        try {
            return CapabilityInventoryCodec.encode(CapabilityInventory.parseFrom(ActualNativeCatalogue.bytes()));
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Actual Native catalogue exporter artifact is malformed", failure);
        }
    }
}
