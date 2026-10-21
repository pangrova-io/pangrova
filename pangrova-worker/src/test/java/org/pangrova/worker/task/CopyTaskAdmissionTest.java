package org.pangrova.worker.task;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.testing.nativefixture.CanonicalTaskRequestTestFixture;

import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Guards the real admission boundary when the verified COPY runtime is absent. */
class CopyTaskAdmissionTest {
    @Test
    void unboundCopySplitCannotReserveAWorkerTaskOrReachNativeCreation() {
        var split = org.pangrova.protocol.v2.DataSplit.newBuilder().setSplitId("copy-input")
                .setConnectorId("copy").setSourceId("copy-source").setSourceSnapshot("version-one")
                .setDataObjectId("s3://incoming/data.parquet").setOpaqueLocation("s3://incoming/data.parquet")
                .setLength(1024).setConnectorDetail(com.google.protobuf.Any.pack(
                        org.pangrova.protocol.v2.FileSplitDetail.newBuilder().setFileFormat(
                                org.pangrova.protocol.v2.FileFormat.FILE_FORMAT_PARQUET).build()));
        var canonical = CanonicalTaskRequestTestFixture.builder("copy-task", "copy-query", 0, 0,
                TaskKind.TASK_KIND_READ, new byte[] {1}).addSplits(split).build();
        var request = SubmitTaskRequest.from(canonical);
        var descriptor = org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter.toTaskDescriptor(request);
        try (TaskManager manager = TaskManager.forHttpWiringOnly()) {
            assertThatThrownBy(() -> manager.submitTask(descriptor, request))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("COPY source split");
            assertThat(manager.resolveWorkerTaskId(canonical.getAttempt().getTaskId())).isNull();
            assertThat(manager.getRunningTaskCount()).isZero();
        }
    }

    @Test
    void copyTaskCannotReserveOrCreateNativeOwnerBeforeStagingIsWired() {
        var scope = new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1,
                "s3://incoming/file.parquet", "s3://managed/copy", "a".repeat(64),
                "query-7", "task-42",
                "coordinator", "test", 1, 1, 4_102_444_800_000L);
        var canonical = CanonicalTaskRequestTestFixture.builder("task-42", "query-7", 0, 0,
                        TaskKind.TASK_KIND_WRITE, new byte[] {1})
                .setCopySourceStagingCommandSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(scope.commandSha256()))))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId("task-42:grant").setCpuSlots(1)
                        .setNativeMemoryBytes(64L * 1024 * 1024))
                .build();
        var request = SubmitTaskRequest.from(canonical,
                new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope));
        var descriptor = org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter
                .toTaskDescriptor(request);
        try (TaskManager manager = TaskManager.forHttpWiringOnly()) {
            assertThatThrownBy(() -> manager.submitTask(descriptor, request))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("configured Native input runtime");
            assertThat(manager.resolveWorkerTaskId(canonical.getAttempt().getTaskId())).isNull();
            assertThat(manager.getRunningTaskCount()).isZero();
        }
    }
}
