package org.pangrova.testing.integration.e2e.jni;

import com.google.protobuf.ByteString;
import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.protocol.checksum.ProtocolChecksums;
import org.pangrova.protocol.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Cross-language policy check through the packaged JNI executor, without Java prevalidation. */
class NativeSubstraitVersionE2ETest extends SqlE2ETestBase {
    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of();
    }

    @ParameterizedTest
    @CsvSource({"missing,missing required Version", "producer,producer is empty", "major,major-version mismatch"})
    void incompatiblePlanFailsWithNativeDiagnostic(String invalid, String diagnostic) throws Exception {
        var request = TaskRequest.parseFrom(CanonicalNativeTask.valuesRequest());
        var plan = Plan.parseFrom(request.getSubstraitPlan()).toBuilder();
        switch (invalid) {
            case "missing" -> plan.clearVersion();
            case "producer" -> plan.getVersionBuilder().clearProducer();
            case "major" -> plan.getVersionBuilder().setMajorNumber(SubstraitProtocolVersion.SPEC_MAJOR + 1);
            default -> throw new IllegalArgumentException(invalid);
        }
        var report = execute(request, plan.build());
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
        assertThat(report.getError().getMessage()).contains(diagnostic);
        assertThat(report.getUsage().getObjectStoreReadBytes()).isZero();
    }

    @Test
    void javaStampedPlanAndCompatibleMinorPatchExecuteInNative() throws Exception {
        var request = TaskRequest.parseFrom(CanonicalNativeTask.valuesRequest());
        var plan = Plan.parseFrom(request.getSubstraitPlan()).toBuilder();
        assertThat(execute(request, plan.build()).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        plan.getVersionBuilder().setMinorNumber(999).setPatchNumber(7).setProducer("compatible-producer/test");
        SubstraitProtocolVersion.validate(plan.build());
        var compatibleRequest = TaskRequest.parseFrom(CanonicalNativeTask.valuesRequest());
        assertThat(execute(compatibleRequest, plan.build()).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
    }

    private static TaskReport execute(TaskRequest request, Plan plan) throws Exception {
        byte[] bytes = plan.toByteArray();
        var admitted = request.toBuilder().setSubstraitPlan(ByteString.copyFrom(bytes))
                .setSubstraitPlanChecksum(ProtocolChecksums.sha256(bytes)).build();
        long handle = NativeRuntime.nativeCreateTaskExecutor(admitted.toByteArray());
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
}
