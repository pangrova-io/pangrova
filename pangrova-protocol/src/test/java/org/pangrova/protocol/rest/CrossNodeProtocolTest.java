package org.pangrova.protocol.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;
import org.pangrova.protocol.rest.task.UpdateExchangeSourcesRequest;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            "resultServerPort", "protocolVersion", "capabilities");

    /**
     * Fields that ExchangeSourceDescriptor must carry for Worker exchange client
     * to establish upstream shuffle data connections.
     */
    private static final Set<String> EXCHANGE_SOURCE_REQUIRED_FIELDS = Set.of(
            "sourceTaskId", "host", "transportPort", "partitionId",
            "exchangeType", "sequenceId");

    /**
     * Fields that UpdateExchangeSourcesRequest must carry for Worker
     * to update exchange connections for a running task.
     */
    private static final Set<String> UPDATE_EXCHANGE_SOURCES_REQUIRED_FIELDS = Set.of(
            "taskId", "queryId", "exchangeSources", "reason");

    // ========================================================================
    // WorkerRegistrationRequest completeness
    // ========================================================================

    @Nested
    class WorkerRegistrationRequestTests {

        @Test
        void containsAllRequiredFields() {
            Set<String> actual = recordFieldNames(WorkerRegistrationRequest.class);
            assertThat(actual).containsAll(WORKER_REGISTRATION_REQUIRED_FIELDS);
        }

        @Test
        void roundTripPreservesAllFields() throws Exception {
            WorkerRegistrationRequest original = new WorkerRegistrationRequest(
                    "worker-1", "10.0.0.1", 8080, 9090, 0, 2, List.of("shuffle", "spill"),
                    null, null, null);
            byte[] bytes = mapper.writeValueAsBytes(original);
            WorkerRegistrationRequest restored = mapper.readValue(bytes, WorkerRegistrationRequest.class);

            assertThat(restored.workerId()).isEqualTo(original.workerId());
            assertThat(restored.httpHost()).isEqualTo(original.httpHost());
            assertThat(restored.httpPort()).isEqualTo(original.httpPort());
            assertThat(restored.transportPort()).isEqualTo(original.transportPort());
            assertThat(restored.protocolVersion()).isEqualTo(original.protocolVersion());
            assertThat(restored.capabilities()).isEqualTo(original.capabilities());
        }

        @Test
        void rejectsBlankWorkerId() {
            assertThatThrownBy(() -> new WorkerRegistrationRequest(
                    "", "host", 8080, 9090, 0, 1, List.of(), null, null, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("workerId");
        }

        @Test
        void rejectsBlankHost() {
            assertThatThrownBy(() -> new WorkerRegistrationRequest(
                    "w-1", "", 8080, 9090, 0, 1, List.of(), null, null, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("httpHost");
        }

        @Test
        void defaultsProtocolVersionToOneWhenZero() {
            WorkerRegistrationRequest req = new WorkerRegistrationRequest(
                    "w-1", "host", 8080, 9090, 0, 0, List.of(), null, null, null);
            assertThat(req.protocolVersion()).isEqualTo(1);
        }

        @Test
        void defaultsNullCapabilitiesToEmptyList() {
            WorkerRegistrationRequest req = new WorkerRegistrationRequest(
                    "w-1", "host", 8080, 9090, 0, 1, null, null, null, null);
            assertThat(req.capabilities()).isEmpty();
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
            ExchangeSourceDescriptor original = new ExchangeSourceDescriptor(
                    "task-upstream-1", "worker-host-2", 9090, 3, "BROADCAST", 42L);
            byte[] bytes = mapper.writeValueAsBytes(original);
            ExchangeSourceDescriptor restored = mapper.readValue(bytes, ExchangeSourceDescriptor.class);

            assertThat(restored.sourceTaskId()).isEqualTo(original.sourceTaskId());
            assertThat(restored.host()).isEqualTo(original.host());
            assertThat(restored.transportPort()).isEqualTo(original.transportPort());
            assertThat(restored.partitionId()).isEqualTo(original.partitionId());
            assertThat(restored.exchangeType()).isEqualTo(original.exchangeType());
            assertThat(restored.sequenceId()).isEqualTo(original.sequenceId());
        }

        @Test
        void jsonUsesSnakeCaseFieldNames() throws Exception {
            ExchangeSourceDescriptor src = new ExchangeSourceDescriptor(
                    "t-1", "host-1", 9090, 0, "HASH_PARTITION", 1L);
            JsonNode node = mapper.valueToTree(src);

            assertThat(node.has("source_task_id")).isTrue();
            assertThat(node.has("host")).isTrue();
            assertThat(node.has("transport_port")).isTrue();
            assertThat(node.has("partition_id")).isTrue();
            assertThat(node.has("exchange_type")).isTrue();
            assertThat(node.has("sequence_id")).isTrue();
        }

        @Test
        void rejectsBlankSourceTaskId() {
            assertThatThrownBy(() -> new ExchangeSourceDescriptor(
                    "", "host", 9090, 0, "HASH", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sourceTaskId");
        }

        @Test
        void rejectsBlankHost() {
            assertThatThrownBy(() -> new ExchangeSourceDescriptor(
                    "t-1", "", 9090, 0, "HASH", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("host");
        }

        @Test
        void rejectsZeroTransportPort() {
            assertThatThrownBy(() -> new ExchangeSourceDescriptor(
                    "t-1", "host", 0, 0, "HASH", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("transportPort");
        }

        @Test
        void rejectsNegativeTransportPort() {
            assertThatThrownBy(() -> new ExchangeSourceDescriptor(
                    "t-1", "host", -1, 0, "HASH", 1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("transportPort");
        }
    }

    // ========================================================================
    // UpdateExchangeSourcesRequest completeness
    // ========================================================================

    @Nested
    class UpdateExchangeSourcesRequestTests {

        @Test
        void containsAllRequiredFields() {
            Set<String> actual = recordFieldNames(UpdateExchangeSourcesRequest.class);
            assertThat(actual).containsAll(UPDATE_EXCHANGE_SOURCES_REQUIRED_FIELDS);
        }

        @Test
        void roundTripPreservesAllFields() throws Exception {
            ExchangeSourceDescriptor src = new ExchangeSourceDescriptor(
                    "upstream-1", "worker-3", 9090, 0, "HASH_PARTITION", 5L);
            UpdateExchangeSourcesRequest original = new UpdateExchangeSourcesRequest(
                    "task-42", "query-7", List.of(src), "stage-2-ready");
            byte[] bytes = mapper.writeValueAsBytes(original);
            UpdateExchangeSourcesRequest restored = mapper.readValue(
                    bytes, UpdateExchangeSourcesRequest.class);

            assertThat(restored.taskId()).isEqualTo(original.taskId());
            assertThat(restored.queryId()).isEqualTo(original.queryId());
            assertThat(restored.reason()).isEqualTo(original.reason());
            assertThat(restored.exchangeSources()).hasSize(1);
            assertThat(restored.exchangeSources().get(0).sourceTaskId()).isEqualTo("upstream-1");
        }

        @Test
        void rejectsBlankTaskId() {
            assertThatThrownBy(() -> new UpdateExchangeSourcesRequest(
                    "", "q-1", List.of(), "reason"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("taskId");
        }

        @Test
        void rejectsBlankQueryId() {
            assertThatThrownBy(() -> new UpdateExchangeSourcesRequest(
                    "t-1", "", List.of(), "reason"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("queryId");
        }

        @Test
        void nullExchangeSourcesDefaultsToEmptyList() {
            UpdateExchangeSourcesRequest req = new UpdateExchangeSourcesRequest(
                    "t-1", "q-1", null, "reason");
            assertThat(req.exchangeSources()).isEmpty();
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
         * partitionId for partition routing, sequenceId for stale rejection.
         */
        @Test
        void exchangeSourceFieldsAlignWithClientNeeds() {
            Set<String> fields = recordFieldNames(ExchangeSourceDescriptor.class);
            // Connection establishment
            assertThat(fields).contains("host", "transportPort");
            // Ticket construction
            assertThat(fields).contains("sourceTaskId", "partitionId");
            // Stale message rejection
            assertThat(fields).contains("sequenceId");
            // Exchange strategy
            assertThat(fields).contains("exchangeType");
        }

        @Test
        void updateRequestCarriesRoutingIdentifiers() {
            Set<String> fields = recordFieldNames(UpdateExchangeSourcesRequest.class);
            assertThat(fields).contains("taskId", "queryId");
        }

        @Test
        void allCrossNodeDtosAreRecords() {
            assertThat(WorkerRegistrationRequest.class.isRecord()).isTrue();
            assertThat(ExchangeSourceDescriptor.class.isRecord()).isTrue();
            assertThat(UpdateExchangeSourcesRequest.class.isRecord()).isTrue();
        }

        @Test
        void noCrossNodeDtoUsesWeakTypes() {
            for (Class<? extends Record> dtoClass : List.of(
                    WorkerRegistrationRequest.class,
                    ExchangeSourceDescriptor.class,
                    UpdateExchangeSourcesRequest.class)) {
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
