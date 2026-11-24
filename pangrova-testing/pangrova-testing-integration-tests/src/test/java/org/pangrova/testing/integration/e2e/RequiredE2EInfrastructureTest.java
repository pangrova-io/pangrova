package org.pangrova.testing.integration.e2e;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import java.net.ServerSocket;
import java.util.List;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;
import org.pangrova.worker.result.ResultReadTickets;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequiredE2EInfrastructureTest extends SqlE2ETestBase {

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                "iceberg_catalog", "default", icebergWarehouse().toString()));
    }

    @Test
    void e2eDispatcherBindsWorkerAdmissibleControlIdentity() {
        String queryId = "query-" + java.util.UUID.randomUUID();
        byte[] plan = new byte[]{1};
        TaskRequest unbound = TaskRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId(queryId)
                        .setTaskId("task-1")
                        .setPlanEpoch(1)
                        .setFencingToken(1))
                .setTaskKind(TaskKind.TASK_KIND_READ)
                .setSecurity(SecurityContext.newBuilder()
                        .setTenantId("e2e-tenant")
                        .setPrincipal("e2e-user"))
                .setSubstraitPlan(ByteString.copyFrom(plan))
                .setSubstraitPlanChecksum(ProtocolChecksums.sha256(plan))
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId("e2e-control-binding-contract").setCpuSlots(1)
                        .setNativeMemoryBytes(1048576).setShuffleBufferBytes(1048576))
                .setDeadlineEpochMs(4_102_444_800_000L)
                .build();

        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(unbound))
                .hasMessageContaining("execution-owner identity are incomplete");
        var coordination = e2eCoordinationForTest();
        assertThat(coordination.acquireQueryOwnership(queryId, "e2e-coordinator")).isTrue();
        var anchor = coordination.requireExecutionAnchor(queryId);
        try (var dispatcher = SqlE2ETestBase.createE2ETaskDispatcher()) {
            TaskRequest bound = dispatcher.bindControlIdentity(
                    SubmitTaskRequest.from(unbound)).taskRequest();

            assertThat(bound.getAttempt().getControlEpoch())
                    .isEqualTo(coordination.getCurrentEpoch());
            assertThat(bound.getAttempt().getExecutionOwner().getOwnerId())
                    .isEqualTo(anchor.ownerId());
            assertThat(bound.getAttempt().getExecutionOwner().getOwnerIncarnation())
                    .isEqualTo(anchor.ownerIncarnation());
            assertThat(bound.getAttempt().getExecutionOwner().getFencingToken())
                    .isEqualTo(anchor.fencingToken());
            assertThat(bound.getResourceGrant()).isEqualTo(unbound.getResourceGrant());
            assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(
                    bound.toBuilder().clearResourceGrant().build()))
                    .hasMessageContaining("Canonical task resource grant is incomplete");
            assertThatCode(() -> CanonicalTaskRequests.validateForAdmission(bound))
                    .doesNotThrowAnyException();
        } finally {
            coordination.releaseQueryOwnership(queryId, "e2e-coordinator");
        }
    }

    @Test
    void acceptsCompleteInfrastructure() {
        assertThatCode(() -> SqlE2ETestBase.verifyRequiredInfrastructure(
                true, true, 12345, true))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsUnavailableDocker() {
        assertThatThrownBy(() -> SqlE2ETestBase.verifyRequiredInfrastructure(
                false, true, 12345, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Docker is required");
    }

    @Test
    void rejectsMissingOrIncompatibleNativeLibrary() {
        assertThatThrownBy(() -> SqlE2ETestBase.verifyRequiredInfrastructure(
                true, false, 12345, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("native library is required");
    }

    @Test
    void rejectsUnavailableResultServer() {
        assertThatThrownBy(() -> SqlE2ETestBase.verifyRequiredInfrastructure(
                true, true, 0, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("result server is required");
    }

    @Test
    void rejectsUninitializedSqlRunner() {
        assertThatThrownBy(() -> SqlE2ETestBase.verifyRequiredInfrastructure(
                true, true, 12345, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SQL runner was not initialized");
    }

    @Test
    void rejectsMissingResultEndpoint() {
        assertThatThrownBy(() -> SqlE2ETestBase.fetchResultsViaTransport(null, "query-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing result endpoint")
                .hasMessageContaining("query-1");
    }

    @Test
    void rejectsMissingNativeTaskId() {
        assertThatThrownBy(() -> SqlE2ETestBase.fetchResultsViaFlight(
                new ResultEndpoint("localhost", 1, "task-1", "grpc"),
                "query-1", new CoordinatorTaskTracker()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing Native task ID")
                .hasMessageContaining("query-1");
    }

    @Test
    void bindsOpaqueResultAuthorityToIssuedResultTaskAcrossStages() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("query-1", "producer-task");
        tracker.registerTask("query-1", "result-task");
        tracker.registerTaskRequest("producer-task", resultRequest("producer-task", false));
        SubmitTaskRequest result = resultRequest("result-task", true);
        tracker.registerTaskRequest("result-task", result);
        ResultEndpoint endpoint = new ResultReadTickets(new byte[32])
                .endpoint(new ValidatedTaskSubmission(result), "worker-2", 12345, "grpc");

        assertThat(endpoint.ticket()).isNotEqualTo("result-task");
        assertThat(SqlE2ETestBase.requireNativeResultTaskId(
                endpoint, "query-1", tracker)).isEqualTo("result-task");
        assertThatThrownBy(() -> SqlE2ETestBase.requireNativeResultTaskId(
                endpoint, "foreign-query", tracker))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing Native task ID");
    }

    @Test
    void rejectsAmbiguousIssuedResultTasksAndMissingOpaqueAuthority() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("query-1", "result-1");
        tracker.registerTask("query-1", "result-2");
        SubmitTaskRequest first = resultRequest("result-1", true);
        tracker.registerTaskRequest("result-1", first);
        tracker.registerTaskRequest("result-2", resultRequest("result-2", true));
        ResultEndpoint endpoint = new ResultReadTickets(new byte[32])
                .endpoint(new ValidatedTaskSubmission(first), "worker-2", 12345, "grpc");

        assertThatThrownBy(() -> SqlE2ETestBase.requireNativeResultTaskId(endpoint, "query-1", tracker))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Ambiguous Native result task identity");
        assertThatThrownBy(() -> SqlE2ETestBase.requireNativeResultTaskId(
                new ResultEndpoint("worker-2", 12345, "", "grpc"), "query-1", tracker))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing Native task ID");
    }

    private static SubmitTaskRequest resultRequest(String taskId, boolean directResult) {
        return SubmitTaskRequest.from(CanonicalTaskRequestTestFixture.builder(
                taskId, "query-1", 0, 0, TaskKind.TASK_KIND_READ, new byte[]{1})
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId("e2e-result-contract").setCpuSlots(1)
                        .setNativeMemoryBytes(1048576).setShuffleBufferBytes(1048576))
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setDirectResultOutput(directResult))
                .build());
    }

    @Test
    void rejectsUnavailableNativeResultServer() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("query-1", "task-1");
        SubmitTaskRequest result = resultRequest("task-1", true);
        tracker.registerTaskRequest("task-1", result);
        ResultEndpoint endpoint = new ResultReadTickets(new byte[32])
                .endpoint(new ValidatedTaskSubmission(result), "localhost", closedPort, "grpc");
        assertThatThrownBy(() -> SqlE2ETestBase.fetchResultsViaFlight(
                endpoint, "query-1", tracker))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Native Flight result fetch failed")
                .hasCauseInstanceOf(Exception.class);
    }

    @Test
    void rejectsTransportInitializationFailure() {
        int previousPort = SqlE2ETestBase.sharedResultServerPort;
        try {
            assertThatThrownBy(() -> SqlE2ETestBase.createTestTransportRefs(null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to initialize E2E Native transports")
                    .hasCauseInstanceOf(NullPointerException.class);
            assertThat(SqlE2ETestBase.sharedResultServerPort).isZero();
        } finally {
            SqlE2ETestBase.sharedResultServerPort = previousPort;
        }
    }

    @Test
    void rejectsCorruptResultBytes() {
        assertThatThrownBy(() -> SqlE2ETestBase.decodeResultBytes(new byte[] {1, 2, 3}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to decode result bytes");
    }
}
