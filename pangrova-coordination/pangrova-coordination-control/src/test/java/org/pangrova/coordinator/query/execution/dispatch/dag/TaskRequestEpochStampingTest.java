package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.execution.binding.TaskRequestFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.DirectExchangeInput;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExchangeReadMode;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TaskRequestFactory#buildDmlWriteRequest}, verifying
 * that DML write task requests are constructed correctly.
 */
class TaskRequestEpochStampingTest {

    @Test
    void dmlWriteRequest_hasCorrectTaskType() {
        var session = session("q-2");
        var limit = grant();
        var writeConfig = writeConfig("catalog.schema.table", "APPEND", List.of());
        SubmitTaskRequest req = request(
                session, new byte[]{1, 2, 3}, limit, writeConfig, 0);
        assertThat(req.taskRequest().getTaskKind())
                .isEqualTo(org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE);
    }

    @Test
    void dmlWriteRequest_carriesQueryId() {
        var session = session("q-3");
        var limit = grant();
        var writeConfig = writeConfig("catalog.schema.table", "APPEND", List.of());
        SubmitTaskRequest req = request(session, new byte[]{1}, limit, writeConfig, 0);
        assertThat(req.taskRequest().getAttempt().getQueryOrJobId()).isEqualTo("q-3");
    }

    @Test
    void dmlWriteRequest_carriesWriteConfig() {
        var session = session("q-4");
        var limit = grant();
        var writeConfig = writeConfig("catalog.schema.orders", "APPEND", List.of());
        SubmitTaskRequest req = request(
                session, new byte[]{1, 2}, limit, writeConfig, 0);
        assertThat(req.taskRequest().getExecutionOptions().hasWrite()).isTrue();
        assertThat(req.taskRequest().getExecutionOptions().getWrite().getTargetTable())
                .isEqualTo("catalog.schema.orders");
    }

    @Test
    void dmlWriteRequestCarriesCanonicalStaticPartitionBindings() {
        var writeConfig = writeConfig(
                "catalog.schema.orders", "OVERWRITE",
                List.of("day=2026-09-19", "region=cn"));

        SubmitTaskRequest req = request(
                session("q-partition"), new byte[]{1}, grant(), writeConfig, 0);

        assertThat(req.taskRequest().getExecutionOptions().getWrite().getPartitionSpecList())
                .containsExactly("day=2026-09-19", "region=cn");
    }

    @Test
    void dmlWriteRequest_generatesUniqueTaskId() {
        var session = session("q-5");
        var limit = grant();
        var writeConfig = writeConfig("catalog.schema.table", "APPEND", List.of());
        SubmitTaskRequest req1 = request(session, new byte[]{1}, limit, writeConfig, 0);
        SubmitTaskRequest req2 = request(session, new byte[]{1}, limit, writeConfig, 0);
        assertThat(req1.taskRequest().getAttempt().getTaskId())
                .isNotEqualTo(req2.taskRequest().getAttempt().getTaskId());
    }

    @Test
    void dmlWriteRequest_rejectsClientDeadlineSessionOverride() {
        var session = new QuerySession(
                "q-deadline", Map.of("pangrova.deadline_epoch_ms", "1"),
                identity(), "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        var limit = grant();
        var writeConfig = writeConfig("catalog.schema.table", "APPEND", List.of());
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                request(session, new byte[]{1}, limit, writeConfig, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved task control field");
    }

    @Test
    void dmlWriteRequest_rejectsMissingSecurityIdentity() {
        var session = new QuerySession(
                "q-no-identity", Map.of(), null, "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        var writeConfig = writeConfig(
                "catalog.schema.table", "APPEND", List.of());

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                request(session, new byte[]{1}, grant(), writeConfig, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authenticated tenant and principal");
    }

    @Test
    void dmlWriteRequest_carriesExplicitCoordinatorDeadline() {
        var session = session("q-deadline");
        var limit = grant();
        var writeConfig = writeConfig("catalog.schema.table", "APPEND", List.of());
        long before = System.currentTimeMillis() + 1_000L;

        SubmitTaskRequest req = request(
                session, new byte[]{1}, limit, writeConfig, 5);

        assertThat(req.taskRequest().getDeadlineEpochMs())
                .isBetween(before + 4_000L, before + 6_000L);
        assertThat(req.taskRequest().getExecutionOptions().getSessionPropertiesMap())
                .doesNotContainKey("pangrova.deadline_epoch_ms");
    }

    @Test
    void dmlWriteRequestCarriesExchangeBindingsAndTheirResourceBudget() throws Exception {
        var session = session("q-bound");
        var writeConfig = writeConfig(
                "catalog.schema.table", "APPEND", List.of());
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId("ai-invoke-0")
                .setResources(OperatorResourceRequirement.newBuilder()
                        .setProviderConcurrency(2)
                        .setProviderItems(5)
                        .setProviderAttempts(8)
                        .setProviderRequestBytes(50)
                        .setProviderResponseBytes(20))
                .build();
        ExchangeInputBinding source = exchangeInput();

        SubmitTaskRequest request = TaskRequestFactory.buildDmlWriteRequest(
                new TaskRequestFactory.DmlWriteTask(
                        session, new byte[]{1}, grant(), List.of(source),
                        List.of(binding), List.of(), List.of(), writeConfig, 5,
                        TaskAttemptDescriptor.of(3, 1)));

        assertThat(request.taskRequest().getExchangeInputsList()).hasSize(1);
        assertThat(request.taskRequest().getExchangeInputs(0)
                .getDirect().getSourcesList()).hasSize(1);
        OperatorBinding encoded = request.taskRequest().getOperatorBindings(0);
        assertThat(encoded.getPlanNodeId()).isEqualTo("ai-invoke-0");
        assertThat(encoded.getExecutionPolicy().getDeadlineEpochMs())
                .isEqualTo(request.taskRequest().getDeadlineEpochMs());
        StageResourceGrant resourceGrant = request.taskRequest().getResourceGrant();
        assertThat(resourceGrant.getProviderItems()).isEqualTo(5);
        assertThat(resourceGrant.getProviderConcurrency()).isEqualTo(2);
        assertThat(request.taskRequest().getTraceContext().getTraceId()).isEqualTo(session.traceContext().traceId());
        assertThat(request.taskRequest().getTraceContext().getSpanId()).isEqualTo(session.traceContext().spanId());
    }

    private static SubmitTaskRequest request(
            QuerySession session,
            byte[] plan,
            StageResourceGrant resourceGrant,
            WriteConfig writeConfig,
            long timeoutSeconds) {
        return TaskRequestFactory.buildDmlWriteRequest(
                new TaskRequestFactory.DmlWriteTask(
                        session, plan, resourceGrant, List.of(), List.of(),
                        List.of(), List.of(), writeConfig, timeoutSeconds,
                        TaskAttemptDescriptor.of(1, 0)));
    }

    private static ExchangeInputBinding exchangeInput() {
        return ExchangeInputBinding.newBuilder()
                .setPlanNodeId("exchange-input-0")
                .setExchangeId("exchange-0")
                .setExpectedSourcePartitions(1)
                .setNoMoreSources(true)
                .setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING)
                .setDirect(DirectExchangeInput.newBuilder()
                        .addSources(ExchangePartitionSource.newBuilder()
                                .setSourceTaskId("task-source")
                                .setSourceStageId(1)
                                .setSourcePartitionId(0)
                                .setOutputPartitionId(0)
                                .setPlanEpoch(3)
                                .setAttemptId(0)
                                .setFencingToken(7)
                                .setHost("worker")
                                .setPort(19000)
                                .setSignedTicket(com.google.protobuf.ByteString.copyFromUtf8(
                                        "signed-ticket"))
                                .setTicketExpiresAtEpochMs(
                                        System.currentTimeMillis() + 60_000L)))
                .build();
    }

    private static StageResourceGrant grant() {
        return StageResourceGrant.newBuilder()
                .setGrantId("unbound-test-grant")
                .setNativeMemoryBytes(1024L)
                .setCpuSlots(4)
                .build();
    }

    private static WriteConfig writeConfig(
            String target, String mode, List<String> partitions) {
        var binding = org.pangrova.protocol.v2.WriteOutputBinding.newBuilder()
                .setConnector(org.pangrova.protocol.v2.ConnectorWriteBinding.newBuilder())
                .build();
        return new WriteConfig(target, mode, partitions, null, 0L,
                java.util.Base64.getEncoder().encodeToString(binding.toByteArray()));
    }

    private static QuerySession session(String queryId) {
        return new QuerySession(
                queryId, Map.of(), identity(), "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    private static UserIdentity identity() {
        return new UserIdentity("test-user", Set.of(), Map.of(), Instant.now());
    }
}
