package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual rollback action/exception ordering, independent of Native execution. */
class TaskAdmissionRollbackTest {
    @Test
    void successfulRollbackReturnsAllResourcesInOrder() {
        var returned = new ArrayList<Integer>();
        TaskAdmissionRollback.release(() -> returned.add(1), () -> returned.add(2), () -> returned.add(3));
        assertThat(returned).containsExactly(1, 2, 3);
    }

    @Test
    void failureDoesNotPreventRemainingCleanupAndKeepsOriginalCause() {
        var returned = new ArrayList<Integer>();
        var first = new IllegalStateException("first resource return failed");
        var second = new AssertionError("second resource return failed");
        assertThatThrownBy(() -> TaskAdmissionRollback.release(
                () -> { returned.add(1); throw first; },
                () -> { returned.add(2); throw second; },
                () -> returned.add(3))).isSameAs(first);
        assertThat(returned).containsExactly(1, 2, 3);
        assertThat(first.getSuppressed()).containsExactly(second);
    }

    @Test
    void repeatedSameFailureCannotAbortTheRemainingRollbackThroughSelfSuppression() {
        var returned = new ArrayList<Integer>();
        var failure = new IllegalStateException("same backend outage");
        assertThatThrownBy(() -> TaskAdmissionRollback.release(
                () -> { returned.add(1); throw failure; },
                () -> { returned.add(2); throw failure; },
                () -> returned.add(3))).isSameAs(failure);
        assertThat(returned).containsExactly(1, 2, 3);
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @Test
    void fatalErrorIsRethrownAfterEveryReturnWasAttempted() {
        var failure = new AssertionError("fatal resource return failure");
        var returned = new ArrayList<Integer>();
        assertThatThrownBy(() -> TaskAdmissionRollback.release(
                () -> { returned.add(1); throw failure; },
                () -> returned.add(2))).isSameAs(failure);
        assertThat(returned).containsExactly(1, 2);
    }
}
