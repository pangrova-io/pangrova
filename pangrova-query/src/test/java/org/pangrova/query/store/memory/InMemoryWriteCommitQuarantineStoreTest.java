package org.pangrova.query.store.memory;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.query.transaction.QuarantinedWriteCommit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryWriteCommitQuarantineStoreTest {
    private static final Instant AT = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void retainsEveryExactIntentWhenQueryIdIsReused() {
        var store = new InMemoryWriteCommitQuarantineStore();
        var first = decision("first", 0);
        var latest = decision("latest", 2);
        store.record(first);
        store.record(latest);
        store.record(decision("older", 1));

        assertThat(store.exists("query", "first")).isTrue();
        assertThat(store.exists("query", "latest")).isTrue();
        assertThat(store.exists("query", "older")).isTrue();
        assertThat(store.exists("query", "unknown")).isFalse();
        assertThat(store.exists("other-query", "latest")).isFalse();
        assertThat(store.find("query")).contains(latest);
        assertThat(store.find("other-query")).isEmpty();
    }

    @Test
    void repeatingAnExactIntentPreservesItsOriginalDecision() {
        var store = new InMemoryWriteCommitQuarantineStore();
        var first = decision("same", 0);
        store.record(first);
        store.record(decision("same", 10));
        assertThat(store.find("query")).contains(first);
    }

    @Test
    void simultaneousDecisionsForDistinctIntentsAreNotDiscarded() throws Exception {
        var store = new InMemoryWriteCommitQuarantineStore();
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(4);
        try {
            var writes = new ArrayList<Future<?>>();
            for (int index = 0; index < 16; index++) {
                var entry = decision("intent-" + index, index);
                writes.add(executor.submit(() -> {
                    start.await();
                    store.record(entry);
                    return null;
                }));
            }
            start.countDown();
            for (var write : writes) write.get(5, TimeUnit.SECONDS);
            for (int index = 0; index < 16; index++) {
                assertThat(store.exists("query", "intent-" + index)).isTrue();
            }
            assertThat(store.find("query")).contains(decision("intent-15", 15));
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static QuarantinedWriteCommit decision(String fingerprint, int seconds) {
        return new QuarantinedWriteCommit("query", fingerprint, "catalog", "schema.table",
                "unproven publication", AT.plusSeconds(seconds));
    }
}
