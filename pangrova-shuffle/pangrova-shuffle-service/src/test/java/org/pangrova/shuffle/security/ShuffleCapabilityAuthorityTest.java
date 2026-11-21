package org.pangrova.shuffle.security;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.security.enums.CapabilityOperation;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShuffleCapabilityAuthorityTest {

    private static final ExecutionOwnerIdentity OWNER = ExecutionOwnerIdentity.newBuilder()
            .setOwnerId("coordinator").setOwnerIncarnation("boot-1").setFencingToken(7).build();

    @Test
    void rejectsOperationConfusionAndExpiry() {
        ShuffleCapabilityAuthority authority = ShuffleCapabilityAuthority.testing(new byte[32]);
        long expiry = System.currentTimeMillis() + 60_000;
        var capability = authority.issue(
                CapabilityScope.forShuffle(CapabilityOperation.WRITE, "shuffle-1", OWNER), expiry);

        assertThatThrownBy(() -> authority.verify(capability,
                CapabilityScope.forShuffle(CapabilityOperation.READ, "shuffle-1", OWNER),
                System.currentTimeMillis())).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> authority.verify(capability,
                CapabilityScope.forShuffle(CapabilityOperation.WRITE, "shuffle-1", OWNER), expiry))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void rejectsTamperedToken() {
        ShuffleCapabilityAuthority authority = ShuffleCapabilityAuthority.testing(new byte[32]);
        long expiry = System.currentTimeMillis() + 60_000;
        var capability = authority.issue(
                CapabilityScope.forQuery("query-1", OWNER), expiry);
        byte[] token = capability.getToken().toByteArray();
        token[0] ^= 1;

        assertThatThrownBy(() -> authority.verify(capability.toBuilder()
                        .setToken(ByteString.copyFrom(token)).build(),
                CapabilityScope.forQuery("query-1", OWNER), System.currentTimeMillis()))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void bindsEveryTaskAttemptFieldAndRejectsStageBearerForData() {
        ShuffleCapabilityAuthority authority = ShuffleCapabilityAuthority.testing(new byte[32]);
        long now = System.currentTimeMillis();
        TaskAttemptId attempt = TaskAttemptId.newBuilder()
                .setQueryOrJobId("query-1").setStageId(2).setPartitionId(3)
                .setPlanEpoch(4).setAttemptId(5).setFencingToken(6)
                .setTaskId("task-1").setControlEpoch(8).setExecutionOwner(OWNER)
                .setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId("00000000-0000-0000-0000-000000000001")
                        .setIncarnationId("00000000-0000-0000-0000-000000000002")).build();
        CapabilityScope exact = CapabilityScope.forTask(
                CapabilityOperation.WRITE, "shuffle-1", OWNER, attempt);
        var grant = authority.issue(exact, now + 60_000);
        authority.verify(grant, exact, now);

        assertThatThrownBy(() -> authority.verify(grant,
                CapabilityScope.forTask(CapabilityOperation.WRITE, "shuffle-1", OWNER,
                        attempt.toBuilder().setTaskId("task-2").build()), now))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> authority.verify(grant,
                CapabilityScope.forTask(CapabilityOperation.WRITE, "shuffle-1", OWNER,
                        attempt.toBuilder().setAttemptId(6).build()), now))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> authority.verify(grant,
                CapabilityScope.forTask(CapabilityOperation.WRITE, "shuffle-1", OWNER,
                        attempt.toBuilder().setControlEpoch(9).build()), now))
                .isInstanceOf(SecurityException.class);
        var stage = authority.issue(CapabilityScope.forShuffle(
                CapabilityOperation.WRITE, "shuffle-1", OWNER), now + 60_000);
        assertThatThrownBy(() -> authority.verify(stage, exact, now))
                .isInstanceOf(SecurityException.class);
    }
}
