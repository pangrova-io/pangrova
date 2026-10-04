package org.pangrova.worker.execution;

import com.google.protobuf.Any;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryRequest;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryResult;
import org.pangrova.protocol.v2.ExecutorCreationState;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.jni.NativeRuntime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual packaged JNI creation/Registry/release boundaries; not a simulated OOME or SQL E2E. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class NativeCreationTransactionIntegrationTest {
    @Test
    void actualNativeConfigRejectionRollsBackBeforeWorkerCanClaimNoOwner() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            // Java admission accepts this exact request. The actual Native
            // decoder rejects result-column metadata on a non-result task.
            var request = fixture.valuesRequest(false).toBuilder()
                    .addResultColumnTypes(ResultColumnLogicalType.newBuilder().setColumnIndex(0)).build();
            CanonicalTaskRequests.validateForAdmission(request);
            try {
                fixture.executor().createTask(request.toByteArray());
                throw new AssertionError("Actual Native configuration must reject non-result metadata");
            } catch (NativeTaskCreationFailure failure) {
                assertThat(failure.withoutNativeOwner()).isTrue();
                assertThat(failure.actualHandle()).isZero();
                assertThat(failure.getCause()).hasMessageContaining("Result column logical types require direct result output");
            }
            var evidence = nativeRecovery(request);
            assertThat(evidence.getState()).isEqualTo(ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER);
            assertThat(evidence.getExecutorHandle()).isZero();
            assertThat(fixture.nodePool().getUsedBytes()).isZero();
            assertThat(fixture.arbitrator().getUsedMemory()).isZero();
        }
    }

    @Test
    void actualRegisteredNativeOwnerCanBeRecoveredWithoutTheCreateReturnValue() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            var request = fixture.valuesRequest(false);
            long actual = NativeRuntime.nativeCreateTaskExecutor(request.toByteArray());
            try {
                var recovered = nativeRecovery(request);
                assertThat(recovered.getState()).isEqualTo(ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT);
                assertThat(recovered.getExecutorHandle()).isEqualTo(actual);
                var released = TaskReport.parseFrom(NativeRuntime.nativeReleaseExecutor(recovered.getExecutorHandle()));
                assertThat(released.getAttempt()).isEqualTo(request.getAttempt());
                assertThat(released.getExecutionDrainState()).isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
                assertThat(released.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
                // A real Pending Native owner released without a Velox task
                // still has exit evidence; statistics absence is not its proof.
                assertThat(released.hasNativeTaskStats()).isFalse();
            } catch (Exception | Error failure) {
                try { NativeRuntime.nativeReleaseExecutor(actual); }
                catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
    }

    @Test
    void actualNativePreParseSealRejectsALaterExactCreate() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            var request = fixture.valuesRequest(false);
            assertThat(nativeRecovery(request).getState()).isEqualTo(ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER);
            assertThatThrownBy(() -> NativeRuntime.nativeCreateTaskExecutor(request.toByteArray()))
                    .hasMessageContaining("already owned or sealed");
            assertThat(nativeRecovery(request).getState()).isEqualTo(ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER);
            assertThat(fixture.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void changedTenantCannotRecoverOrReplaceAnActualNativeOwner() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            var request = fixture.valuesRequest(false);
            long handle = fixture.createPending(request);
            try {
                var changed = request.toBuilder().setSecurity(request.getSecurity().toBuilder().setTenantId("different-tenant")).build();
                assertThatThrownBy(() -> nativeRecovery(changed)).isInstanceOf(IllegalStateException.class);
                assertThat(nativeRecovery(request).getExecutorHandle()).isEqualTo(handle);
                assertThat(fixture.executor().releaseTask(handle)).isPresent();
            } finally {
                fixture.executor().releaseTask(handle);
            }
        }
    }

    private static ExecutorCreationRecoveryResult nativeRecovery(TaskRequest request) throws Exception {
        var identity = ExecutorCreationRecoveryRequest.newBuilder()
                .setAdmissionSeal(CanonicalTaskAdmissionSeals.create(request)).setSecurity(request.getSecurity()).build();
        var command = RuntimeCommand.newBuilder().setProtocol(request.getProtocol()).setAttempt(request.getAttempt())
                .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_RECOVER_EXECUTOR_CREATION)
                .setPayload(Any.pack(identity)).build();
        var response = RuntimeCommandResponse.parseFrom(NativeRuntime.nativeCommand(0, command.toByteArray()));
        if (!response.getSuccess() || response.hasError() || !response.getPayload().is(ExecutorCreationRecoveryResult.class)) {
            throw new IllegalStateException("Actual Native recovery failed: " + response.getError().getMessage());
        }
        var evidence = response.getPayload().unpack(ExecutorCreationRecoveryResult.class);
        assertThat(evidence.getIdentity()).isEqualTo(identity);
        return evidence;
    }
}
