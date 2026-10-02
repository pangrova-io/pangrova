package org.pangrova.protocol;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionCallbackContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TraceContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Canonical authority validation, not a JNI or provider execution test. */
class CanonicalCallbackAuthorityTest {
    @Test
    void callbackProjectionPreservesTheCompleteAdmittedAuthority() {
        var task = CanonicalTaskRequestTestFixture.builder("task", "query", TaskKind.TASK_KIND_READ,
                new byte[]{1}).setTraceContext(TraceContext.newBuilder().setTraceId("trace"))
                .setResourceGrant(StageResourceGrant.newBuilder().setGrantId("grant").setCpuSlots(2)
                        .setNativeMemoryBytes(1024).setProviderItems(8).setProviderAttempts(11)
                        .setProviderRequestBytes(1024).setProviderResponseBytes(2048)).build();
        var callback = CanonicalTaskRequests.callbackFor(task);
        assertThat(callback.getProtocol()).isEqualTo(task.getProtocol());
        assertThat(callback.getSecurity()).isEqualTo(task.getSecurity());
        assertThat(callback.getAttempt()).isEqualTo(task.getAttempt());
        assertThat(callback.getResourceGrant()).isEqualTo(task.getResourceGrant());
        assertThat(callback.getDeadlineEpochMs()).isEqualTo(task.getDeadlineEpochMs());
        assertThat(callback.getTraceContext()).isEqualTo(task.getTraceContext());
        assertThatThrownBy(() -> CanonicalTaskRequests.callbackFor(task.toBuilder()
                .clearSubstraitPlanChecksum().build())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checksum");
    }

    @Test
    void nonProviderCallbackHasFullTaskAuthorityWithoutAnArtificialProviderGrant() throws Exception {
        var authority = ExecutionCallbackContext.parseFrom(context().toByteArray());
        assertThatCode(() -> CanonicalTaskRequests.validateCallback(authority)).doesNotThrowAnyException();
        assertThat(authority.getResourceGrant().getProviderAttempts()).isZero();
        assertThat(authority.getAttempt().getExecutionOwner().getOwnerIncarnation()).isEqualTo("test");
        assertThat(authority.getAttempt().getFencingToken()).isEqualTo(1L << 32 | 1L);
    }

    @Test
    void completeAttemptIsNotCollapsedToTheSameTaskIdAcrossRetryOrEpoch() throws Exception {
        var first = context();
        var second = first.toBuilder().setAttempt(first.getAttempt().toBuilder()
                .setPlanEpoch(2).setAttemptId(1).setFencingToken(1L << 32 | 2L)).build();
        CanonicalTaskRequests.validateCallback(second);
        assertThat(second.getAttempt().getTaskId()).isEqualTo(first.getAttempt().getTaskId());
        assertThat(ExecutionCallbackContext.parseFrom(second.toByteArray()).getAttempt())
                .isEqualTo(second.getAttempt()).isNotEqualTo(first.getAttempt());
    }

    @Test
    void absentWrongVersionOrUnsignedOverflowAuthorityFailsClosed() {
        var good = context();
        for (var malformed : java.util.List.of(good.toBuilder().clearSecurity().build(),
                good.toBuilder().clearAttempt().build(), good.toBuilder().clearResourceGrant().build(),
                good.toBuilder().setProtocol(good.getProtocol().toBuilder().setRolloutEpoch("old")).build(),
                good.toBuilder().setAttempt(good.getAttempt().toBuilder().setPlanEpoch(-1)).build(),
                good.toBuilder().setDeadlineEpochMs(-1).build(),
                good.toBuilder().setResourceGrant(good.getResourceGrant().toBuilder()
                        .setProviderRequestBytes(-1)).build())) {
            assertThatThrownBy(() -> CanonicalTaskRequests.validateCallback(malformed))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void unknownFieldsAreNotAcceptedAsACompatibleCallbackSchema() {
        var unknown = UnknownFieldSet.newBuilder().addField(999,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var context = context().toBuilder().setUnknownFields(unknown).build();
        assertThatThrownBy(() -> CanonicalTaskRequests.validateCallback(context))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown fields");
        var good = context();
        assertThatThrownBy(() -> CanonicalTaskRequests.validateCallback(good.toBuilder()
                .setProtocol(good.getProtocol().toBuilder().setUnknownFields(unknown)).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("protocol identity");
        assertThatThrownBy(() -> CanonicalTaskRequests.validateCallback(good.toBuilder()
                .setTraceContext(good.getTraceContext().toBuilder().setUnknownFields(unknown)).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("trace context");
    }

    private static ExecutionCallbackContext context() {
        var task = CanonicalTaskRequestTestFixture.builder("task", "query", TaskKind.TASK_KIND_READ,
                new byte[]{1}).build();
        return ExecutionCallbackContext.newBuilder().setProtocol(task.getProtocol()).setSecurity(task.getSecurity())
                .setAttempt(task.getAttempt()).setDeadlineEpochMs(task.getDeadlineEpochMs())
                .setTraceContext(TraceContext.newBuilder().setTraceId("trace"))
                .setResourceGrant(StageResourceGrant.newBuilder().setGrantId("grant").setCpuSlots(2)
                        .setNativeMemoryBytes(1024)).build();
    }
}
