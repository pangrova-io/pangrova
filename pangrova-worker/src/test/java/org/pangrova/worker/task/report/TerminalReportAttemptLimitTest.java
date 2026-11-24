package org.pangrova.worker.task.report;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TerminalReportAttemptLimitTest {
    @Test
    void saturatingTheTransportQueuesWorkAndDropsAnExpiredRoundBeforeSendingIt() throws Exception {
        var capacity = new RemoteTaskStatusReporter.TerminalAttempts();
        var active = new AtomicInteger();
        int limit = RemoteTaskStatusReporter.TerminalAttempts.MAX_ACTIVE;
        for (int i = 0; i < limit; i++) {
            capacity.startOrQueue(active::incrementAndGet, new CompletableFuture<>());
        }
        var expired = new CompletableFuture<Void>();
        var sentExpired = new AtomicBoolean();
        var firstWaiting = new CompletableFuture<Integer>();
        var secondWaiting = new CompletableFuture<Integer>();
        capacity.startOrQueue(() -> sentExpired.set(true), expired);
        capacity.startOrQueue(() -> firstWaiting.complete(active.incrementAndGet()), new CompletableFuture<>());
        capacity.startOrQueue(() -> secondWaiting.complete(active.incrementAndGet()), new CompletableFuture<>());
        assertThat(active.get()).isEqualTo(limit);
        assertThat(firstWaiting).isNotDone();
        assertThat(secondWaiting).isNotDone();

        expired.completeExceptionally(new java.util.concurrent.TimeoutException("Terminal round expired"));
        active.decrementAndGet();
        capacity.release();
        assertThat(firstWaiting.get(2, TimeUnit.SECONDS)).isEqualTo(limit);
        assertThat(secondWaiting).isNotDone();
        active.decrementAndGet();
        capacity.release();
        assertThat(secondWaiting.get(2, TimeUnit.SECONDS)).isEqualTo(limit);
        assertThat(sentExpired).isFalse();
    }
}
