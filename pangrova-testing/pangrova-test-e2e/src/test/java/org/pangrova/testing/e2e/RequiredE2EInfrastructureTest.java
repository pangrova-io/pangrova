package org.pangrova.testing.e2e;

import java.net.ServerSocket;
import java.util.List;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
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
                .setProtocol(CanonicalTaskRequests.protocolVersion())
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
                .setSubstraitPlanChecksum(CanonicalTaskRequests.sha256(plan))
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
    void bindsNativeResultToEndpointTaskAcrossStages() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("query-1", "producer-task");
        tracker.registerTask("query-1", "result-task");

        assertThat(SqlE2ETestBase.requireNativeResultTaskId(
                new ResultEndpoint("worker-2", 12345, "result-task", "grpc"),
                "query-1", tracker)).isEqualTo("result-task");
        assertThatThrownBy(() -> SqlE2ETestBase.requireNativeResultTaskId(
                new ResultEndpoint("worker-2", 12345, "foreign-task", "grpc"),
                "query-1", tracker))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing Native task ID");
    }

    @Test
    void rejectsUnavailableNativeResultServer() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("query-1", "task-1");
        assertThatThrownBy(() -> SqlE2ETestBase.fetchResultsViaFlight(
                new ResultEndpoint("localhost", closedPort, "task-1", "grpc"),
                "query-1", tracker))
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
