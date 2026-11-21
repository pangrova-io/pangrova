package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
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
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class CdcTaskPlannerTest {

    @Test
    void plansTasksFromCommittedOffsetsAndCurrentLogEnds() {
        CdcTaskPlanner.CdcTaskPlan plan = CdcTaskPlanner.plan(
                stream(), new Reader(), plan(), Map.of(new TopicPartitionOffset("orders", 0), 4L),
                100L, 2, attempt(), security(), grant(), 4_102_444_800_000L,
                "00000000-0000-0000-0000-000000000001", 42L);

        assertThat(plan.tasks()).hasSize(1);
        assertThat(plan.tasks().get(0).getAttempt().getTaskId()).isEqualTo("task-0");
        assertThat(plan.tasks().get(0).getCdcIngest().getRangesList())
                .extracting(range -> range.getTopic() + "@" + range.getPartition()
                        + ":" + range.getStartOffset() + "-" + range.getEndOffset())
                .containsExactlyInAnyOrder("orders@0:4-10", "orders@1:0-12");
        assertThat(plan.endOffsets()).containsEntry(
                new TopicPartitionOffset("orders", 0), 10L)
                .containsEntry(new TopicPartitionOffset("orders", 1), 12L);
    }

    private static final class Reader implements CdcSourceReader {
        @Override public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            throw new AssertionError("Planner must not poll");
        }
        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            throw new AssertionError("Planner must not commit offsets");
        }
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
            throw new AssertionError("Planner must not seek");
        }
        @Override public List<TopicPartitionOffset> partitions() {
            return List.of(new TopicPartitionOffset("orders", 0),
                    new TopicPartitionOffset("orders", 1));
        }
        @Override public Map<TopicPartitionOffset, Long> endOffsets(
                Collection<TopicPartitionOffset> partitions) {
            return Map.of(new TopicPartitionOffset("orders", 0), 10L,
                    new TopicPartitionOffset("orders", 1), 12L);
        }
        @Override public OptionalLong currentLag() {
            return OptionalLong.empty();
        }
        @Override public void close() {
        }
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

    private static CdcWriteAuthority.CdcWritePlan plan() {
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

    private static final class Handle implements ConnectorOutputTableHandle { }
}
