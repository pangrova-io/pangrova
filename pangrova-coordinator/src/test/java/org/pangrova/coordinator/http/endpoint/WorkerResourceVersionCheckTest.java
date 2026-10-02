package org.pangrova.coordinator.http.endpoint;

import com.google.protobuf.ByteString;
import jakarta.ws.rs.core.Response;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.rest.worker.WorkerControlResponse;
import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.CanonicalProtocolDescriptors;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.ProtocolVersion;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void registrationEnvelopeRejectsLegacyVersion1() {
        assertThatThrownBy(() -> new WorkerRegistrationRequest(
                "worker-1", "host-1", 8080, 9090, 0,
                1, null, List.of(), null, canonicalInventory()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical v2");
    }

    @Test
    void register_compatibleVersion2_succeeds() {
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(
                "worker-2", "host-2", 8080, 9090, 0,
                2, null, List.of(), null, canonicalInventory());

        Response response = resource.register(request);

        assertThat(response.getStatus()).isEqualTo(200);
        WorkerControlResponse body = (WorkerControlResponse) response.getEntity();
        assertThat(body.status()).isEqualTo("registered");
        assertThat(body.requireCoordinatorEpoch()).isEqualTo(1L);
    }

    @Test
    void register_missingCanonicalInventory_isRejected() {
        assertThatThrownBy(() -> new WorkerRegistrationRequest(
                "worker-no-inventory", "host-2", 8080, 9090, 0,
                2, null, List.of(), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capabilityInventory");
    }

    @Test
    void register_invalidVersion0_failsBeforeTransport() {
        assertThatThrownBy(() ->
                new WorkerRegistrationRequest(
                        "worker-3", "host-3", 8080, 9090, 0,
                        0, null, List.of(), null, canonicalInventory()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocolVersion");
    }

    @Test
    void registrationEnvelopeRejectsFutureVersion() {
        assertThatThrownBy(() -> new WorkerRegistrationRequest(
                "worker-4", "host-4", 8080, 9090, 0,
                99, null, List.of(), null, canonicalInventory()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical v2");
    }

    @Test
    void canonicalProtocolVersionAcceptsOnlyV2() {
        assertThat(WorkerResource.isCanonicalProtocolVersion(2)).isTrue();
    }

    @Test
    void nonCanonicalProtocolVersionsAreRejected() {
        assertThat(WorkerResource.isCanonicalProtocolVersion(0)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(-1)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(1)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(3)).isFalse();
        assertThat(WorkerResource.isCanonicalProtocolVersion(99)).isFalse();
    }

    private static String canonicalInventory() {
        CapabilityInventory inventory = CapabilityInventory.newBuilder()
                .setProtocol(ProtocolVersion.newBuilder()
                        .setMajor(2)
                        .setMinor(0)
                        .setDescriptorSetSha256(ByteString.copyFrom(
                                CanonicalProtocolDescriptors.sha256()))
                        .setRolloutEpoch("test"))
                .setResultEnvelopeVersion(1)
                .setNativeEngineVersion("native-test")
                .build();
        return CapabilityInventoryCodec.encode(inventory);
    }
}
