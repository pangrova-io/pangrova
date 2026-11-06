package org.pangrova.persistence.query.postgresql.state;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.persistence.query.api.commit.JournalWriteException;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskStateEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises real resident-event admission only; no SQL completion or durable receipt is simulated. */
class JournalPendingWritesTest {
    @Test
    void queryAdmissionRejectsForeignOwnerBeforeTransferAndRetainsExactRetryAttempt() {
        var owner = new JournalPendingWrites(1);
        var foreign = new JournalPendingWrites(1);
        var event = new QueryStateEvent(JournalEventContext.create(1, "coordinator"),
                "query", null, null, "RUNNING");
        var write = new JournalPendingWrites.Write(owner, event);

        assertThatThrownBy(() -> foreign.admit(write, true))
                .isInstanceOfSatisfying(JournalWriteException.class, failure ->
                        assertThat(failure.admissionOutcome())
                                .isEqualTo(JournalWriteException.AdmissionOutcome.REJECTED_BEFORE_TRANSFER))
                .hasMessageContaining("different pending owner");
        assertThat(owner.drained()).isTrue();
        assertThat(foreign.drained()).isTrue();

        var attempt = owner.admit(write, false);
        assertThat(owner.admit(new JournalPendingWrites.Write(owner, event), true)).isSameAs(attempt);
        assertThat(attempt.isDone()).isFalse();
        assertThat(owner.drained()).isFalse();
        assertThat(foreign.drained()).isTrue();
    }

    @Test
    void taskAdmissionRejectsForeignOwnerWithoutTakingItsCapacity() {
        var owner = new JournalPendingWrites(1);
        var foreign = new JournalPendingWrites(1);
        var event = new TaskStateEvent(JournalEventContext.create(1, "coordinator"),
                "query", "task", "worker", "RUNNING", 1, 1);

        assertThatThrownBy(() -> foreign.admit(new JournalPendingWrites.Write(owner, event), false))
                .isInstanceOfSatisfying(JournalWriteException.class, failure ->
                        assertThat(failure.admissionOutcome())
                                .isEqualTo(JournalWriteException.AdmissionOutcome.REJECTED_BEFORE_TRANSFER));
        var attempt = foreign.admit(new JournalPendingWrites.Write(foreign, event), false);
        assertThat(foreign.admit(new JournalPendingWrites.Write(foreign, event), true)).isSameAs(attempt);
        assertThat(attempt.isDone()).isFalse();
        assertThat(owner.drained()).isTrue();
        assertThat(foreign.drained()).isFalse();
    }
}
