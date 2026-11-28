package org.pangrova.coordinator.task;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.query.api.lifecycle.QueryOperation;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the public operation contract against actual query issuance owners. */
class QueryOperationOwnershipTest {

    @Test
    void concurrentClosesReleaseTheOriginalOperationExactlyOnce() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        tracker.openQueryIssuance("concurrent-close");
        var exits = new AtomicInteger();
        tracker.addQueryDrainProgressListener(query -> exits.incrementAndGet());
        QueryOperation operation = tracker.enterQueryOperation("concurrent-close");
        tracker.sealQueryIssuance("concurrent-close");
        assertThatThrownBy(() -> tracker.retireQueryIssuance("concurrent-close"))
                .hasMessageContaining("operations have not exited");

        int callers = 8;
        var ready = new CountDownLatch(callers);
        var release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(callers);
        try {
            var completions = new ArrayList<Future<?>>();
            for (int i = 0; i < callers; i++) {
                completions.add(executor.submit(() -> {
                    ready.countDown();
                    if (!release.await(3, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Concurrent closers were not released");
                    }
                    operation.close();
                    return null;
                }));
            }
            assertThat(ready.await(3, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            for (var completion : completions) completion.get(3, TimeUnit.SECONDS);
            tracker.requireQueryExecutionDrained("concurrent-close");
            tracker.retireQueryIssuance("concurrent-close");
            operation.close();
            assertThat(exits).hasValue(1);
        } finally {
            release.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 3000);
        }
    }

    @Test
    void frontendAndMetadataOperationsRetainIndependentOwnership() {
        var tracker = new CoordinatorTaskTracker();
        tracker.openQueryIssuance("two-operations");
        var exits = new AtomicInteger();
        tracker.addQueryDrainProgressListener(query -> exits.incrementAndGet());
        QueryOperation frontend = tracker.enterQueryOperation("two-operations");
        QueryOperation metadata = tracker.enterQueryMetadataOperation("two-operations");
        tracker.sealQueryIssuance("two-operations");

        frontend.close();
        frontend.close();
        assertThat(exits).hasValue(0);
        assertThatThrownBy(() -> tracker.requireQueryExecutionDrained("two-operations"))
                .hasMessageContaining("operations have not exited");

        metadata.close();
        tracker.requireQueryExecutionDrained("two-operations");
        tracker.retireQueryIssuance("two-operations");
        assertThat(exits).hasValue(1);
    }

    @Test
    void notificationFailureDoesNotReopenOrReleaseAnOperationTwice() {
        var tracker = new CoordinatorTaskTracker();
        tracker.openQueryIssuance("notification-failure");
        var notifications = new AtomicInteger();
        var failure = new IllegalStateException("Exit listener failed after release");
        tracker.addQueryDrainProgressListener(query -> {
            notifications.incrementAndGet();
            throw failure;
        });
        QueryOperation operation = tracker.enterQueryMetadataOperation("notification-failure");
        tracker.sealQueryIssuance("notification-failure");

        assertThatThrownBy(operation::close).isSameAs(failure);
        tracker.requireQueryExecutionDrained("notification-failure");
        tracker.retireQueryIssuance("notification-failure");
        assertThatCode(operation::close).doesNotThrowAnyException();
        assertThat(notifications).hasValue(1);
    }
}
