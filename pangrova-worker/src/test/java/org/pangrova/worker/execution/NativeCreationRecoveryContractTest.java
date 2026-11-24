package org.pangrova.worker.execution;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.execution.AiTaskLifecycle;
import org.pangrova.protocol.v2.ExecutionCallbackContext;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.worker.function.WorkerCallbackProviders;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryRequest;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryResult;
import org.pangrova.protocol.v2.ExecutorCreationState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Generated-protocol decoder boundaries only; these do not prove Native creation or resource release. */
class NativeCreationRecoveryContractTest {
    @Test
    void exactPresentIdentityPublishesOnlyTheReturnedPositiveHandle() {
        var owner = owner();
        var identity = identity(owner);
        NativeCreationRecovery.accept(owner, identity, result(identity, ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT)
                .setExecutorHandle(17).build());
        assertThat(owner.handle).isEqualTo(17);
        assertThat(owner.nativeClosedWithoutOwner).isFalse();
    }

    @Test
    void closedNativeEvidenceDoesNotReleaseWorkerCallbackObligations() {
        var owner = owner();
        owner.aiRegistered = true;
        owner.externalRegistered = true;
        var identity = identity(owner);
        NativeCreationRecovery.accept(owner, identity,
                result(identity, ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER).build());
        assertThat(owner.nativeClosedWithoutOwner).isTrue();
        assertThat(owner.aiRegistered).isTrue();
        assertThat(owner.externalRegistered).isTrue();
        assertThat(owner.releaseReceipt).isNull();
    }

    @Test
    void constructingUnspecifiedAndUnknownStateCannotInventCompletedCreation() {
        var owner = owner();
        var identity = identity(owner);
        for (int state : new int[]{0, 1, 99}) {
            var evidence = ExecutorCreationRecoveryResult.newBuilder().setIdentity(identity).setStateValue(state).build();
            assertThatThrownBy(() -> NativeCreationRecovery.accept(owner, identity, evidence))
                    .as("state %s", state).isInstanceOf(IllegalStateException.class);
        }
        assertThat(owner.handle).isZero();
        assertThat(owner.nativeClosedWithoutOwner).isFalse();
    }

    @Test
    void mismatchedIdentityAndUnknownFieldsNeverPublishHandleOrNoOwner() {
        var owner = owner();
        var identity = identity(owner);
        var changed = identity.toBuilder().setSecurity(identity.getSecurity().toBuilder().setTenantId("other")).build();
        assertThatThrownBy(() -> NativeCreationRecovery.accept(owner, identity,
                result(changed, ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER).build()))
                .isInstanceOf(IllegalStateException.class);
        var unknown = UnknownFieldSet.newBuilder().addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        assertThatThrownBy(() -> NativeCreationRecovery.accept(owner, identity,
                result(identity, ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT)
                        .setExecutorHandle(17).setUnknownFields(unknown).build())).isInstanceOf(IllegalStateException.class);
        assertThat(owner.handle).isZero();
        assertThat(owner.nativeClosedWithoutOwner).isFalse();
    }

    @Test
    void zeroOverflowAndNoOwnerWithHandleAreRejected() {
        var owner = owner();
        var identity = identity(owner);
        for (long handle : new long[]{0, -1}) {
            assertThatThrownBy(() -> NativeCreationRecovery.accept(owner, identity,
                    result(identity, ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT).setExecutorHandle(handle).build()))
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThatThrownBy(() -> NativeCreationRecovery.accept(owner, identity,
                result(identity, ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER).setExecutorHandle(1).build()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void recoveryCannotReplaceAnAlreadyPublishedActualHandle() {
        var owner = owner();
        var identity = identity(owner);
        NativeCreationRecovery.accept(owner, identity,
                result(identity, ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT).setExecutorHandle(17).build());
        assertThatThrownBy(() -> NativeCreationRecovery.accept(owner, identity,
                result(identity, ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT).setExecutorHandle(18).build()))
                .hasMessageContaining("changed its actual registered handle");
        assertThat(owner.handle).isEqualTo(17);
        assertThat(owner.nativeClosedWithoutOwner).isFalse();
    }

    @Test
    void recoveryReleasesOnlyTheCallbackOwnerAdmittedByThisExecutor(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        var descriptor = directory.resolve("META-INF/services/" + AiTaskLifecycle.class.getName());
        java.nio.file.Files.createDirectories(descriptor.getParent());
        java.nio.file.Files.writeString(descriptor, TrackingLifecycle.class.getName());
        try (var loader = new java.net.URLClassLoader(new java.net.URL[]{directory.toUri().toURL()}, getClass().getClassLoader()) {
            @Override
            public java.util.Enumeration<java.net.URL> getResources(String name) throws java.io.IOException {
                return name.equals("META-INF/services/" + AiTaskLifecycle.class.getName())
                        ? findResources(name) : super.getResources(name);
            }
        }) {
            var installed = (TrackingLifecycle) WorkerCallbackProviders.taskLifecycle(loader);
            var independent = (TrackingLifecycle) WorkerCallbackProviders.taskLifecycle(loader);
            var request = ActualNativeTaskFixture.valuesTaskRequest(false);
            var owner = new NativeCreationOwners().prepare(request, installed);
            installed.registerTask(org.pangrova.protocol.task.CanonicalTaskRequests.callbackFor(request),
                    request.getOperatorBindingsList());
            owner.aiRegistered = true;
            NativeCreationOwners.cleanupCallbacks(owner, null);
            NativeCreationOwners.cleanupCallbacks(owner, null);
            assertThat(installed.releases).containsExactly(request.getAttempt());
            assertThat(independent.releases).isEmpty();

            java.nio.file.Files.writeString(descriptor, TrackingLifecycle.class.getName() + "\n" + ConflictingLifecycle.class.getName());
            assertThatThrownBy(() -> WorkerCallbackProviders.taskLifecycle(loader))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining(TrackingLifecycle.class.getName()).hasMessageContaining(ConflictingLifecycle.class.getName());
        }
    }

    public static class TrackingLifecycle implements AiTaskLifecycle {
        private final java.util.List<TaskAttemptId> releases = new java.util.ArrayList<>();
        public void registerTask(ExecutionCallbackContext callback, List<OperatorBinding> bindings) {}
        public ResourceUsage releaseTask(String tenant, TaskAttemptId attempt) {
            releases.add(attempt);
            return ResourceUsage.getDefaultInstance();
        }
        public void cancelTask(String tenant, TaskAttemptId attempt) {}
        public ResourceUsage usage(String tenant, TaskAttemptId attempt) { return ResourceUsage.getDefaultInstance(); }
    }

    public static final class ConflictingLifecycle extends TrackingLifecycle {}

    private static NativeCreationOwners.Owner owner() {
        return new NativeCreationOwners.Owner(ActualNativeTaskFixture.valuesTaskRequest(false),
                AiTaskLifecycle.NONE);
    }
    private static ExecutorCreationRecoveryRequest identity(NativeCreationOwners.Owner owner) {
        return ExecutorCreationRecoveryRequest.newBuilder().setAdmissionSeal(CanonicalTaskAdmissionSeals.create(owner.request))
                .setSecurity(owner.request.getSecurity()).build();
    }
    private static ExecutorCreationRecoveryResult.Builder result(ExecutorCreationRecoveryRequest identity, ExecutorCreationState state) {
        return ExecutorCreationRecoveryResult.newBuilder().setIdentity(identity).setState(state);
    }
}
