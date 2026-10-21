package org.pangrova.worker.copy;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopyTaskReadBudgetTest {
    @Test
    void concurrentBackendAdmissionsCannotOverspendTheOriginalGrant() throws Exception {
        var budget = new CopyTaskReadBudget(7);
        var executor = Executors.newFixedThreadPool(8);
        try {
            var attempts = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 32; i++) {
                attempts.add(() -> {
                    try {
                        budget.reserve(1);
                        return true;
                    } catch (IllegalStateException exhausted) {
                        return false;
                    }
                });
            }
            int accepted = 0;
            for (var outcome : executor.invokeAll(attempts)) {
                if (outcome.get()) accepted++;
            }
            assertThat(accepted).isEqualTo(7);
            assertThat(budget.requestedBytes()).isEqualTo(7);
            assertThat(budget.remainingBytes()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectedOverflowOrNegativeRequestsNeverRefundOrWrapEnteredReads() {
        var budget = new CopyTaskReadBudget(Long.MAX_VALUE);
        budget.reserve(Long.MAX_VALUE - 1);
        assertThatThrownBy(() -> budget.reserve(2)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> budget.reserve(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(budget.remainingBytes()).isEqualTo(1);
        budget.reserve(1);
        assertThat(budget.requestedBytes()).isEqualTo(Long.MAX_VALUE);
        assertThatThrownBy(() -> new CopyTaskReadBudget(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
