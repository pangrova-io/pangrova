package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ResourceDrainEvidence;
import org.pangrova.protocol.v2.ResourceDrainEvidenceRequest;
import org.pangrova.protocol.v2.ResourceDrainScope;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;
import org.pangrova.worker.jni.NativeCommandBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Metadata contract validation only; actual RPC/buffer drain requires JNI lifecycle tests. */
class TaskResourceDrainProbeTest {
    private TaskRequest request() {
        var request = CanonicalTaskRequestTestFixture.builder("resource-task", "resource-query", 2, 3,
                TaskKind.TASK_KIND_READ, new byte[] {1}).build();
        return request.toBuilder().setSecurity(request.getSecurity().toBuilder()
                .addActiveRoles("result-reader").setPolicyContextHash("admitted-result-policy")).build();
    }

    private ResourceDrainEvidence.Builder evidence(TaskRequest request) {
        return ResourceDrainEvidence.newBuilder().setAttempt(request.getAttempt())
                .setSecurity(request.getSecurity())
                .setResource(ResourceDrainScope.RESOURCE_DRAIN_SCOPE_RESULT_STREAM)
                .setProducerStopped(true).setAuthorityRevoked(true).setQueueCleared(true)
                .setDrained(true);
    }

    @Test
    void canonicalCommandRequiresAndPreservesExactFencedAttemptAndSecurity() throws Exception {
        var request = request();
        var payload = ResourceDrainEvidenceRequest.newBuilder().setSecurity(request.getSecurity())
                .setResource(ResourceDrainScope.RESOURCE_DRAIN_SCOPE_RESULT_STREAM).build();
        var type = RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_RESOURCE_DRAIN_EVIDENCE;
        assertThatThrownBy(() -> NativeCommandBuilder.buildCommand(type, payload))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("attempt");
        var command = RuntimeCommand.parseFrom(NativeCommandBuilder.buildCommand(type, request.getAttempt(), payload));
        assertThat(command.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(command.getPayload().unpack(ResourceDrainEvidenceRequest.class)).isEqualTo(payload);
    }

    @Test
    void requiresProducerRevocationQueueAndEveryRealOwnerToBeDrained() {
        var request = request();
        assertThat(TaskResourceDrainProbe.validatedResultDrain(request, evidence(request).build())).isTrue();
        for (int held = 0; held < 5; held++) {
            var outstanding = evidence(request).setDrained(false);
            switch (held) {
                case 0 -> outstanding.setProducerStopped(false);
                case 1 -> outstanding.setAuthorityRevoked(false);
                case 2 -> outstanding.setQueueCleared(false);
                case 3 -> outstanding.setActiveReaders(1);
                case 4 -> outstanding.setRetainedBatchOwners(1);
                default -> throw new IllegalStateException();
            }
            assertThat(TaskResourceDrainProbe.validatedResultDrain(request, outstanding.build())).isFalse();
            outstanding.setDrained(true);
            assertThatThrownBy(() -> TaskResourceDrainProbe.validatedResultDrain(request, outstanding.build()))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectsCrossTenantAttemptOwnerPolicyUnknownFieldsAndResourceScope() {
        var request = request();
        for (int changed = 0; changed < 7; changed++) {
            var forged = evidence(request);
            switch (changed) {
                case 0 -> forged.setSecurity(request.getSecurity().toBuilder().setTenantId("other"));
                case 1 -> forged.setSecurity(request.getSecurity().toBuilder().setPolicyContextHash("other"));
                case 2 -> forged.setSecurity(request.getSecurity().toBuilder().clearActiveRoles());
                case 3 -> forged.setAttempt(request.getAttempt().toBuilder().setAttemptId(9));
                case 4 -> forged.setAttempt(request.getAttempt().toBuilder().setExecutionOwner(
                        request.getAttempt().getExecutionOwner().toBuilder().setFencingToken(9)));
                case 5 -> forged.setResource(ResourceDrainScope.RESOURCE_DRAIN_SCOPE_UNSPECIFIED);
                case 6 -> forged.setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(100, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build());
                default -> throw new IllegalStateException();
            }
            assertThat(forged.build()).as("Changed authority dimension %d must actually differ", changed)
                    .isNotEqualTo(evidence(request).build());
            assertThatThrownBy(() -> TaskResourceDrainProbe.validatedResultDrain(request, forged.build()))
                    .as("Changed authority dimension %d must be rejected", changed)
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
