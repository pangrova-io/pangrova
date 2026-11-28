package org.pangrova.testing.integration.e2e.jni;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;
import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.worker.jni.NativeRuntime;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual packaged JNI task grants must cover metadata reads, not only data-file reads. */
class NativeMetadataReadBudgetE2ETest extends SqlE2ETestBase {
    @TempDir
    Path directory;
    private static final String SNAPSHOT = """
            {"version":3,"id":7,"schemaId":0,"commitUser":"writer","commitIdentifier":1,
             "commitKind":"APPEND","timeMillis":1700000000000,"baseManifestList":"base",
             "deltaManifestList":"delta","totalRecordCount":0,"deltaRecordCount":0}
            """;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1})
    void admittedMetadataReadFailsBeforeExceedingTheActualBackendGrant(long grant) throws Exception {
        var report = run(grant);
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
        assertThat(report.getError().getMessage()).contains("requested-read grant exhausted");
        assertThat(report.getUsage().getObjectStoreReadBytes()).isLessThanOrEqualTo(grant);
    }

    @Test
    void successfulMetadataReadPublishesItsRealRequestedBytes() throws Exception {
        var report = run(64 * 1024);
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(report.getUsage().getObjectStoreReadBytes())
                .isEqualTo(SNAPSHOT.getBytes(StandardCharsets.UTF_8).length);
    }

    private TaskReport run(long grant) throws Exception {
        Path file = directory.resolve("snapshot-7");
        Files.writeString(file, SNAPSHOT);
        var request = request(file, grant);
        long handle = NativeRuntime.nativeCreateTaskExecutor(request.toByteArray());
        try {
            NativeRuntime.nativeStartTask(handle);
            AtomicReference<TaskReport> terminal = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                var report = TaskReport.parseFrom(NativeRuntime.nativeGetTaskStatus(handle));
                assertThat(report.getState()).isIn(TaskState.TASK_STATE_FAILED, TaskState.TASK_STATE_FINISHED);
                terminal.set(report);
            });
            return terminal.get();
        } finally {
            NativeRuntime.nativeReleaseExecutor(handle);
        }
    }

    private TaskRequest request(Path file, long grant) throws Exception {
        Plan.Builder plan = Plan.newBuilder();
        org.pangrova.protocol.substrait.SubstraitProtocolVersion.stamp(plan);
        var root = plan.addRelationsBuilder().getRootBuilder().addNames("snapshot_id");
        var read = root.getInputBuilder().getReadBuilder();
        read.getBaseSchemaBuilder().addNames("snapshot_id").getStructBuilder().addTypesBuilder()
                .getI64Builder().setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        read.getNamedTableBuilder().addNames("orders$snapshots");
        byte[] bytes = plan.build().toByteArray();
        var detail = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_METADATA_JSON)
                .setSystemTable(SystemTableSplitDetail.newBuilder()
                        .setKind(SystemTableSplitDetail.Kind.PAIMON_SNAPSHOTS).setSnapshotId(7)
                        .setObjectSha256(ByteString.copyFrom(HexFormat.of().parseHex(Sha256Digests.sha256Hex(SNAPSHOT)))))
                .build();
        var task = TaskRequest.parseFrom(CanonicalNativeTask.valuesRequest()).toBuilder()
                .setSubstraitPlan(ByteString.copyFrom(bytes)).setSubstraitPlanChecksum(ProtocolChecksums.sha256(bytes))
                .addSplits(DataSplit.newBuilder().setSplitId("snapshot-7").setConnectorId("paimon")
                        .setSourceId("orders$snapshots").setSourceSnapshot("snapshot-7")
                        .setDataObjectId(file.toUri().toString()).setOpaqueLocation(file.toUri().toString())
                        .setLength(Files.size(file)).setFileDetail(detail));
        task.getResourceGrantBuilder().setObjectStoreReadBytes(grant);
        return task.build();
    }
}
