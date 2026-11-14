package org.pangrova.coordinator.task;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskJournalEventContextTest {
    @Test
    void retryRetainsIdentityProducerTimeAndIssuingOwner() {
        var first = TaskJournalEventContext.create(attempt(), 9, 173L);
        var replay = TaskJournalEventContext.create(attempt().toBuilder().build(), 9, 173L);

        assertThat(replay).isEqualTo(first);
        assertThat(first.timestampMs()).isEqualTo(173L);
        assertThat(first.coordinatorId()).isEqualTo("issuing-coordinator");
    }

    @Test
    void payloadTimeChangeDoesNotHideBehindANewIdentity() {
        var original = TaskJournalEventContext.create(attempt(), 9, 173L);
        var changed = TaskJournalEventContext.create(attempt(), 9, 174L);

        assertThat(changed.eventId()).isEqualTo(original.eventId());
        assertThat(changed).isNotEqualTo(original);
    }

    @Test
    void completeAttemptAndSequenceArePartOfTheIdentity() {
        var exact = attempt();
        var expected = TaskJournalEventContext.create(exact, 9, 173L).eventId();
        var changes = List.of(exact.toBuilder().setQueryOrJobId("another-query").build(),
                exact.toBuilder().setTaskId("another-task").build(),
                exact.toBuilder().setStageId(3).build(), exact.toBuilder().setPartitionId(4).build(),
                exact.toBuilder().setPlanEpoch(8).build(), exact.toBuilder().setAttemptId(2).build(),
                exact.toBuilder().setFencingToken(12).build(), exact.toBuilder().setControlEpoch(13).build(),
                exact.toBuilder().setExecutionOwner(exact.getExecutionOwner().toBuilder()
                        .setOwnerIncarnation("another-process")).build());

        for (var changed : changes) {
            assertThat(TaskJournalEventContext.create(changed, 9, 173L).eventId()).isNotEqualTo(expected);
        }
        assertThat(TaskJournalEventContext.create(exact, 10, 173L).eventId()).isNotEqualTo(expected);
        assertThat(TaskJournalEventContext.create(exact, 0, 173L).eventId()).isNotEqualTo(expected);
    }

    @Test
    void rejectsMissingOriginalOwnerAndInvalidSequenceOrTime() {
        assertThatThrownBy(() -> TaskJournalEventContext.create(attempt().toBuilder()
                .clearExecutionOwner().build(), 9, 173L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TaskJournalEventContext.create(attempt(), -1, 173L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TaskJournalEventContext.create(attempt(), 9, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query").setTaskId("task")
                .setStageId(1).setPartitionId(2).setPlanEpoch(7).setAttemptId(1)
                .setFencingToken(11).setControlEpoch(12).setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("issuing-coordinator").setOwnerIncarnation("issuing-process")
                        .setFencingToken(11)).build();
    }
}
