package org.pangrova.worker.http.endpoint;

import com.google.protobuf.ByteString;
import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies R10.2: TaskResource returns binary task acknowledgement on error
 * instead of propagating raw exceptions.
 */
class TaskResourceSubmitErrorTest {

    private TaskResource resource;
    private TaskManager taskManager;

    @BeforeEach
    void setUp() {
        taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport = TestExchangeTransport.INSTANCE;
        ExchangeSourceRegistry sourceRegistry = new ExchangeSourceRegistry();

        resource = new TaskResource(
                taskManager, exchangeTransport, sourceRegistry,
                "test-host", 9090, null);
    }

    @AfterEach
    void tearDown() {
        resource.close();
        taskManager.close();
    }

    @Test
    void submitTask_invalidChecksumReturnsBadRequestBeforeAdmission() throws Exception {
        var canonical =
                org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                        "task-1", "query-1", 0, 1,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .clearSubstraitPlanChecksum()
                        .build();
        SubmitTaskRequest request = SubmitTaskRequest.from(canonical);

        Response response = resource.submitTask(new ByteArrayInputStream(request.encode()), null).toCompletableFuture().join();

        assertThat(response.getStatus()).isEqualTo(400);
        var errorResponse = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
        assertThat(errorResponse.hasRejected()).isTrue();
        assertThat(errorResponse.getRejected().getReason()).contains("checksum is invalid");
        assertThat(errorResponse.hasAdmitted()).isFalse();
        assertThat(taskManager.resolveWorkerTaskId("task-1")).isNull();
    }

    @Test
    void submitTask_missingGrantReturnsBadRequestBeforeAdmission() throws Exception {
        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", 0, 1, org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8)).clearResourceGrant().build();
        var request = SubmitTaskRequest.from(canonical);
        try (Response response = resource.submitTask(new ByteArrayInputStream(request.encode()), null).toCompletableFuture().join()) {
            assertThat(response.getStatus()).isEqualTo(400);
            var error = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
            assertThat(error.hasRejected()).isTrue();
            assertThat(error.getRejected().getReason()).contains("resource grant is incomplete");
            assertThat(error.hasAdmitted()).isFalse();
            assertThat(taskManager.resolveWorkerTaskId("task-1")).isNull();
        }
    }

    @Test
    void submitTask_internalSigningFailureRemainsServerErrorBeforeAdmission() throws Exception {
        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", 0, 1, org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId("task-1:grant").setCpuSlots(1).setNativeMemoryBytes(1_048_576)
                        .setShuffleBufferBytes(1_048_576))
                .setExecutionOptions(org.pangrova.protocol.v2.TaskExecutionOptions.newBuilder()
                        .setDirectResultOutput(true))
                .build();
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(canonical);
        try (Response response = resource.submitTask(new ByteArrayInputStream(SubmitTaskRequest.from(canonical).encode()), null).toCompletableFuture().join()) {
            assertThat(response.getStatus()).isEqualTo(500);
            var error = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
            assertThat(error.hasRejected()).isTrue();
            assertThat(error.getRejected().getReason()).contains("signing authority");
            assertThat(taskManager.resolveWorkerTaskId("task-1")).isNull();
        }
    }

    @Test
    void submitTask_validCdcRequestRequiresInstalledBackend() throws Exception {
        var request = CanonicalTaskRequestTestFixture.builder(
                "cdc-task", "cdc-job", 0, 1, TaskKind.TASK_KIND_CDC_INGEST, new byte[0]);
        request.setAttempt(request.getAttempt().toBuilder()
                .setWorkerExecutionIdentity(taskManager.workerExecutionIdentity().toProto()));
        request.setCdcIngest(CdcIngestBinding.newBuilder()
                .addAllowedOperations(org.pangrova.protocol.v2.CdcWriteOperation.CDC_WRITE_OPERATION_INSERT)
                .setStreamId("orders")
                .setTenantId(request.getSecurity().getTenantId())
                .setSourceOwnershipEpoch("epoch-1")
                .setSourceProvider("kafka")
                .setFormatId("debezium-json")
                .setTargetTableUuid("00000000-0000-0000-0000-000000000001")
                .setCommitIntentId("commit-1")
                .setCommitIdentitySha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(new byte[32])))
                .setWrite(WriteOutputBinding.getDefaultInstance())
                .addRanges(CdcSourceRange.newBuilder().setTopic("orders").setEndOffset(1)));
        try (Response response = resource.submitTask(new ByteArrayInputStream(SubmitTaskRequest.from(request.build()).encode()), null).toCompletableFuture().join()) {
            assertThat(response.getStatus()).isEqualTo(503);
            var error = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
            assertThat(error.hasRejected()).isTrue();
            assertThat(error.getRejected().getReason()).contains("no configured CDC ingestion backend");
            assertThat(taskManager.resolveWorkerTaskId("cdc-task")).isNull();
        }
    }

    @Test
    void submitTask_nullRequest_returnsBadRequest() {
        Response response = resource.submitTask(null, null).toCompletableFuture().join();
        assertThat(response.getStatus()).isEqualTo(400);
    }
}
