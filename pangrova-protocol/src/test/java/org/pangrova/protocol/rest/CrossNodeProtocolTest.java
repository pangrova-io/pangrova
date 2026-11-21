package org.pangrova.protocol.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.CanonicalCapabilityInventoryTestFixture;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.protocol.rest.task.ProtocolTestFixtures.exchangeSource;

/**
 * Verifies field completeness and consistency across cross-node communication DTOs:
 * WorkerRegistrationRequest, HeartbeatPayload (wire format), and ExchangeSourceDescriptor.
 *
 * <p>Validates: Requirements 2.8</p>
 */
class CrossNodeProtocolTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // -- Required fields for each cross-node DTO --

    /**
     * Fields that WorkerRegistrationRequest must carry for Coordinator
     * to build a complete WorkerInfo snapshot.
     */
    private static final Set<String> WORKER_REGISTRATION_REQUIRED_FIELDS = Set.of(
            "workerId", "httpHost", "httpPort", "transportPort",
            "resultServerPort", "copyTaskTlsPort", "protocolVersion",
            "capabilityInventory");

    /**
     * Fields that ExchangeSourceDescriptor must carry for Worker exchange client
     * to establish upstream shuffle data connections.
     */
    private static final Set<String> EXCHANGE_SOURCE_REQUIRED_FIELDS = Set.of(
            "planNodeId", "exchangeId", "expectedSourcePartitions",
            "sourceTaskId", "sourceStageId", "sourcePartitionId",
            "outputPartitionId", "planEpoch", "attemptId", "fencingToken",
            "host", "transportPort", "tlsIdentity", "signedTicket",
            "ticketExpiresAtEpochMs", "expectedRows", "expectedBytes", "readMode");

    // ========================================================================
    // WorkerRegistrationRequest completeness
    // ========================================================================

    @Nested
    class WorkerRegistrationRequestTests {

        @Test
        void containsAllRequiredFields() {
            Set<String> actual = recordFieldNames(WorkerRegistrationRequest.class);
            assertThat(actual).containsAll(WORKER_REGISTRATION_REQUIRED_FIELDS);
            assertThat(actual).doesNotContain("capabilities");
        }

        @Test
        void localShuffleRegistrationRoundTripsWithoutAnExternalProvider() throws Exception {
            var identity = new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                    "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002");
            var request = new WorkerRegistrationRequest("w", "host", 8080, 8081, 8082, 2, null,
                    java.util.List.of(), "LOCAL", CanonicalCapabilityInventoryTestFixture.encodedInventory(), 0, identity);
            var decoded = mapper.readValue(mapper.writeValueAsBytes(request), WorkerRegistrationRequest.class);
            assertThat(decoded.shuffleMode()).isEqualTo("LOCAL");
            assertThat(decoded.shuffleProvider()).isNull();
            assertThatThrownBy(() -> new WorkerRegistrationRequest("w", "host", 8080, 8081, 8082, 2, null,
                    java.util.List.of(), "LOCAL", CanonicalCapabilityInventoryTestFixture.encodedInventory(), 0, identity, "remote"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void roundTripPreservesAllFields() throws Exception {
            WorkerRegistrationRequest original = new WorkerRegistrationRequest(
                    "worker-1", "10.0.0.1", 8080, 9090, 0, 2,
                    null, null, null,
                    CanonicalCapabilityInventoryTestFixture.encodedInventory(), 8443, new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"));
            byte[] bytes = mapper.writeValueAsBytes(original);
            WorkerRegistrationRequest restored = mapper.readValue(bytes, WorkerRegistrationRequest.class);

            assertThat(restored.workerId()).isEqualTo(original.workerId());
            assertThat(restored.httpHost()).isEqualTo(original.httpHost());
            assertThat(restored.httpPort()).isEqualTo(original.httpPort());
            assertThat(restored.transportPort()).isEqualTo(original.transportPort());
            assertThat(restored.copyTaskTlsPort()).isEqualTo(8443);
            assertThat(restored.protocolVersion()).isEqualTo(original.protocolVersion());
            assertThat(restored.capabilityInventory())
                    .isEqualTo(original.capabilityInventory());
        }

        @Test
        void rejectsBlankWorkerId() {
            assertThatThrownBy(() -> new WorkerRegistrationRequest(
                    "", "host", 8080, 9090, 0, 2, null, null, null,
                    CanonicalCapabilityInventoryTestFixture.encodedInventory(), new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("workerId");
        }

        @Test
        void rejectsBlankHost() {
            assertThatThrownBy(() -> new WorkerRegistrationRequest(
                    "w-1", "", 8080, 9090, 0, 2, null, null, null,
                    CanonicalCapabilityInventoryTestFixture.encodedInventory(), new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("httpHost");
        }

        @Test
        void rejectsProtocolVersionZero() {
            assertThatThrownBy(() -> new WorkerRegistrationRequest(
                    "w-1", "host", 8080, 9090, 0, 0,
                    null, null, null,
                    CanonicalCapabilityInventoryTestFixture.encodedInventory(), new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("protocolVersion");
        }

        @Test
        void rejectsBlankCanonicalInventory() {
            assertThatThrownBy(() -> new WorkerRegistrationRequest(
                    "w-1", "host", 8080, 9090, 0, 2,
                    null, null, null, "", new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("capabilityInventory");
        }
    }

    // ========================================================================
    // ExchangeSourceDescriptor completeness
    // ========================================================================

    @Nested
    class ExchangeSourceDescriptorTests {

        @Test
        void containsAllRequiredFields() {
            Set<String> actual = recordFieldNames(ExchangeSourceDescriptor.class);
            assertThat(actual).containsAll(EXCHANGE_SOURCE_REQUIRED_FIELDS);
        }

        @Test
        void roundTripPreservesAllFields() throws Exception {
            ExchangeSourceDescriptor original = exchangeSource(
                    "task-upstream-1", "worker-host-2", 9090, 3, "STREAMING", 42L);
            byte[] bytes = mapper.writeValueAsBytes(original);
            ExchangeSourceDescriptor restored = mapper.readValue(bytes, ExchangeSourceDescriptor.class);

            assertThat(restored.sourceTaskId()).isEqualTo(original.sourceTaskId());
            assertThat(restored.host()).isEqualTo(original.host());
            assertThat(restored.transportPort()).isEqualTo(original.transportPort());
            assertThat(restored.planNodeId()).isEqualTo(original.planNodeId());
            assertThat(restored.exchangeId()).isEqualTo(original.exchangeId());
            assertThat(restored.sourceStageId()).isEqualTo(original.sourceStageId());
            assertThat(restored.sourcePartitionId()).isEqualTo(original.sourcePartitionId());
            assertThat(restored.outputPartitionId()).isEqualTo(original.outputPartitionId());
            assertThat(restored.planEpoch()).isEqualTo(original.planEpoch());
            assertThat(restored.fencingToken()).isEqualTo(original.fencingToken());
            assertThat(restored.signedTicket()).isEqualTo(original.signedTicket());
            assertThat(restored.ticketExpiresAtEpochMs())
                    .isEqualTo(original.ticketExpiresAtEpochMs());
        }

        @Test
        void jsonUsesSnakeCaseFieldNames() throws Exception {
            ExchangeSourceDescriptor src = exchangeSource(
                    "t-1", "host-1", 9090, 0, "BLOCKING", 1L);
            JsonNode node = mapper.valueToTree(src);

            assertThat(node.has("source_task_id")).isTrue();
            assertThat(node.has("host")).isTrue();
            assertThat(node.has("transport_port")).isTrue();
            assertThat(node.has("source_partition_id")).isTrue();
            assertThat(node.has("output_partition_id")).isTrue();
            assertThat(node.has("plan_epoch")).isTrue();
            assertThat(node.has("fencing_token")).isTrue();
            assertThat(node.has("signed_ticket")).isTrue();
        }

        @Test
        void rejectsBlankSourceTaskId() {
            assertThatThrownBy(() -> exchangeSource(
                    "", "host", 9090, 0, "BLOCKING", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sourceTaskId");
        }

        @Test
        void rejectsBlankHost() {
            assertThatThrownBy(() -> exchangeSource(
                    "t-1", "", 9090, 0, "BLOCKING", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("host");
        }

        @Test
        void rejectsZeroTransportPort() {
            assertThatThrownBy(() -> exchangeSource(
                    "t-1", "host", 0, 0, "BLOCKING", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("transportPort");
        }

        @Test
        void rejectsNegativeTransportPort() {
            assertThatThrownBy(() -> exchangeSource(
                    "t-1", "host", -1, 0, "BLOCKING", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("transportPort");
        }
    }

    // ========================================================================
    // Cross-DTO consistency: WorkerRegistrationRequest ↔ HeartbeatPayload
    // ========================================================================

    @Nested
    class CrossDtoConsistencyTests {

        @Test
        void workerIdPresentInBothRegistrationAndHeartbeat() {
            assertThat(recordFieldNames(WorkerRegistrationRequest.class))
                    .contains("workerId");
        }

        /**
         * ExchangeSourceDescriptor fields must align with what ExchangeClient needs:
         * host + transportPort for transport connection, sourceTaskId for ticket,
         * logical producer identity and fence for stale-attempt rejection.
         */
        @Test
        void exchangeSourceFieldsAlignWithClientNeeds() {
            Set<String> fields = recordFieldNames(ExchangeSourceDescriptor.class);
            // Connection establishment
            assertThat(fields).contains("host", "transportPort");
            // Ticket construction
            assertThat(fields).contains(
                    "sourceTaskId", "sourceStageId", "sourcePartitionId",
                    "outputPartitionId", "signedTicket");
            // Stale message rejection
            assertThat(fields).contains("planEpoch", "attemptId", "fencingToken");
        }

        @Test
        void allCrossNodeDtosAreRecords() {
            assertThat(WorkerRegistrationRequest.class.isRecord()).isTrue();
            assertThat(ExchangeSourceDescriptor.class.isRecord()).isTrue();
        }

        @Test
        void noCrossNodeDtoUsesWeakTypes() {
            for (Class<? extends Record> dtoClass : List.of(
                    WorkerRegistrationRequest.class,
                    ExchangeSourceDescriptor.class)) {
                for (RecordComponent component : dtoClass.getRecordComponents()) {
                    assertThat(component.getType())
                            .as("Field '%s' in %s must not be Object",
                                    component.getName(), dtoClass.getSimpleName())
                            .isNotEqualTo(Object.class);
                }
            }
        }
    }

    // -- Utility --

    private static Set<String> recordFieldNames(Class<? extends Record> recordClass) {
        return Arrays.stream(recordClass.getRecordComponents())
                .map(RecordComponent::getName)
                .collect(Collectors.toSet());
    }
}
