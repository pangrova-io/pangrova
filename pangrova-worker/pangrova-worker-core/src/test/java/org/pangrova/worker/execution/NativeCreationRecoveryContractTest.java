package org.pangrova.worker.execution;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryRequest;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryResult;
import org.pangrova.protocol.v2.ExecutorCreationState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;

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

    private static NativeCreationOwners.Owner owner() {
        return new NativeCreationOwners.Owner(ActualNativeTaskFixture.valuesTaskRequest(false));
    }
    private static ExecutorCreationRecoveryRequest identity(NativeCreationOwners.Owner owner) {
        return ExecutorCreationRecoveryRequest.newBuilder().setAdmissionSeal(CanonicalTaskAdmissionSeals.create(owner.request))
                .setSecurity(owner.request.getSecurity()).build();
    }
    private static ExecutorCreationRecoveryResult.Builder result(ExecutorCreationRecoveryRequest identity, ExecutorCreationState state) {
        return ExecutorCreationRecoveryResult.newBuilder().setIdentity(identity).setState(state);
    }
}
