package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.source.SourceRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class CdcStreamExecutionTest {

    @Test
    void dispatchesCollectsPublishesAndThenAcknowledgesOffsets() {
        var tracker = new CoordinatorTaskTracker();
        var completions = new CdcTaskCompletionRegistry(tracker);
        var journal = new InMemoryWriteCommitJournal();
        var source = new CdcSourceIdentity("stream", "epoch-2", List.of(
                new CdcSourceIdentity.OffsetRange("orders", 0, 0, 10)));
        var publisher = new java.util.ArrayList<WriteCommitEntry>();
        var acked = new java.util.ArrayList<Map<TopicPartitionOffset, Long>>();
        var plan = CdcTaskPlanner.plan(stream(), new Reader(), writePlan(),
                Map.of(), 100L, 100L, 1, attempt(), security(), grant(),
                4_102_444_800_000L, "uuid", 42L);
        var dispatcher = (CdcTaskDispatchPort) task -> {
            String queryId = task.getAttempt().getQueryOrJobId();
            String taskId = task.getAttempt().getTaskId();
            tracker.registerTask(queryId, taskId);
            tracker.markScheduled(queryId, taskId);
            var submitted = SubmitTaskRequest.from(task);
            tracker.registerTaskRequest(taskId, submitted);
            var report = report(task);
            assertThat(completions.accept(submitted, report, canonical(task))).isTrue();
        };
        var execution = new CdcStreamExecution(dispatcher, completions, journal,
                publisher::add, (s, offsets, fingerprint) -> acked.add(offsets),
                Duration.ofSeconds(2));

        WriteCommitEntry intent = execution.execute(plan, writePlan(), stream(),
                new Session(), source, 42L, 1_000L, "coordinator", "process", 7L);

        assertThat(publisher).containsExactly(intent);
        assertThat(journal.isCommitted(intent.queryId())).isTrue();
        assertThat(acked).containsExactly(Map.of(new TopicPartitionOffset("orders", 0), 10L));
        assertThat(intent.cdcSource()).isEqualTo(source);
    }

    private static TaskStatusReport report(org.pangrova.protocol.v2.TaskRequest task) {
        var attempt = task.getAttempt();
        var write = new WriteResultDescriptor(List.of(), 1, 10, new byte[0],
                List.of(new byte[]{3}, new byte[]{4}));
        return new TaskStatusReport(attempt.getTaskId(), attempt.getQueryOrJobId(),
                attempt.getStageId(), attempt.getPartitionId(), "CDC_INGEST", "worker-1",
                "FINISHED", null, null, write, 1, 1, null, null,
                TaskAttemptDescriptor.from(attempt), canonical(task).toByteArray());
    }

    private static TaskReport canonical(org.pangrova.protocol.v2.TaskRequest task) {
        return TaskReport.newBuilder().setAttempt(task.getAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED)
                .setExecutionDrainState(org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED).build();
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query").setTaskId("task")
                .setPlanEpoch(1).setFencingToken(7).setControlEpoch(1)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("coordinator").setOwnerIncarnation("process")
                        .setFencingToken(7))
                .build();
    }

    private static SecurityContext security() {
        return SecurityContext.newBuilder().setTenantId("tenant-a")
                .setPrincipal("creator").build();
    }

    private static StageResourceGrant grant() {
        return StageResourceGrant.newBuilder().setGrantId("grant")
                .setCpuSlots(1).setNativeMemoryBytes(1_048_576).build();
    }

    private static StreamState stream() {
        return new StreamState("stream", "tenant-a", "checkpoint-1",
                StreamLifecycleState.RUNNING, "catalog.schema.target", "kafka",
                Map.of("topic", "orders"), "debezium-json",
                Map.of(CdcWriteAuthority.PRIVILEGES_PROPERTY, "INSERT,UPDATE,DELETE"),
                "creator", "coordinator-a", 2L, 1, null,
                0L, 0L, 0L, null, Instant.EPOCH, Instant.EPOCH);
    }

    private static CdcWriteAuthority.CdcWritePlan writePlan() {
        var receipt = new WriteAuthorizationReceipt(new byte[]{1},
                new WriteDeclarationSnapshot(new QualifiedTableName("catalog", "schema", "target"),
                        "0".repeat(64), "0".repeat(64), false));
        return new CdcWriteAuthority.CdcWritePlan(new Handle(),
                WriteOutputBinding.newBuilder()
                        .setConnector(ConnectorWriteBinding.newBuilder()
                                .setOutputHandle(org.pangrova.protocol.v2.SerializedConnectorOutputHandle
                                        .newBuilder().setTypeUrl("fixture").setCodecVersion(1)
                                        .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{2}))
                                        .setPayloadChecksum(org.pangrova.protocol.checksum.ProtocolChecksums
                                                .sha256(new byte[]{2}))
                                        .addWriteColumnNames("id")))
                        .build(),
                receipt, new org.pangrova.coordinator.query.task.write.WriteAllocationIntent(
                        "MERGE", Map.of()));
    }

    private static final class Reader implements CdcSourceReader {
        @Override public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            throw new AssertionError();
        }
        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            throw new AssertionError();
        }
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
            throw new AssertionError();
        }
        @Override public List<TopicPartitionOffset> partitions() {
            return List.of(new TopicPartitionOffset("orders", 0));
        }
        @Override public Map<TopicPartitionOffset, Long> endOffsets(
                Collection<TopicPartitionOffset> partitions) {
            return Map.of(new TopicPartitionOffset("orders", 0), 10L);
        }
        @Override public OptionalLong currentLag() { return OptionalLong.empty(); }
        @Override public void close() { }
    }

    private static final class Handle implements ConnectorOutputTableHandle { }

    private static final class Session implements ConnectorSession {
        @Override public String getUser() { return "creator"; }
        @Override public Optional<String> getCatalog() { return Optional.of("catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("schema"); }
        @Override public Map<String, String> getProperties() {
            return Map.of("tenant_id", "tenant-a");
        }
        @Override public Optional<ConnectorQueryAuthority> getQueryAuthority() {
            return Optional.empty();
        }
    }
}
