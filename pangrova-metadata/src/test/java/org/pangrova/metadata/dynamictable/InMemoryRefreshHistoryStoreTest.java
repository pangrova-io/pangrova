package org.pangrova.metadata.dynamictable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InMemoryRefreshHistoryStore}.
 *
 * <p><b>Validates: Requirements 23</b></p>
 */
class InMemoryRefreshHistoryStoreTest {

    private InMemoryRefreshHistoryStore store;
    private static final Instant BASE = Instant.parse("2026-01-01T00:00:00Z");

    @BeforeEach
    void setUp() {
        store = new InMemoryRefreshHistoryStore();
    }

    @Test
    void recordAndQuerySingleEntry() {
        RefreshHistoryEntry entry = createEntry("r-1", "dt_sales", BASE);
        store.record(entry);

        List<RefreshHistoryEntry> result = store.query(
                "dt_sales", BASE.minus(1, ChronoUnit.HOURS), BASE.plus(1, ChronoUnit.HOURS), 10);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).refreshId()).isEqualTo("r-1");
    }

    @Test
    void queryReturnsEmptyForUnknownDtName() {
        store.record(createEntry("r-1", "dt_sales", BASE));

        List<RefreshHistoryEntry> result = store.query(
                "dt_unknown", BASE.minus(1, ChronoUnit.HOURS), BASE.plus(1, ChronoUnit.HOURS), 10);

        assertThat(result).isEmpty();
    }

    @Test
    void queryFiltersOutEntriesOutsideTimeRange() {
        store.record(createEntry("r-1", "dt_sales", BASE));
        store.record(createEntry("r-2", "dt_sales", BASE.plus(2, ChronoUnit.HOURS)));
        store.record(createEntry("r-3", "dt_sales", BASE.plus(5, ChronoUnit.HOURS)));

        List<RefreshHistoryEntry> result = store.query(
                "dt_sales", BASE.plus(1, ChronoUnit.HOURS), BASE.plus(3, ChronoUnit.HOURS), 10);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).refreshId()).isEqualTo("r-2");
    }

    @Test
    void queryIncludesBoundaryTimestamps() {
        store.record(createEntry("r-1", "dt_sales", BASE));
        store.record(createEntry("r-2", "dt_sales", BASE.plus(1, ChronoUnit.HOURS)));

        List<RefreshHistoryEntry> result = store.query(
                "dt_sales", BASE, BASE.plus(1, ChronoUnit.HOURS), 10);

        assertThat(result).hasSize(2);
    }

    @Test
    void queryReturnsResultsInDescendingStartTimeOrder() {
        store.record(createEntry("r-1", "dt_sales", BASE));
        store.record(createEntry("r-2", "dt_sales", BASE.plus(1, ChronoUnit.HOURS)));
        store.record(createEntry("r-3", "dt_sales", BASE.plus(2, ChronoUnit.HOURS)));

        List<RefreshHistoryEntry> result = store.query(
                "dt_sales", BASE, BASE.plus(3, ChronoUnit.HOURS), 10);

        assertThat(result).extracting(RefreshHistoryEntry::refreshId)
                .containsExactly("r-3", "r-2", "r-1");
    }

    @Test
    void queryTruncatesResultToLimit() {
        for (int i = 0; i < 10; i++) {
            store.record(createEntry("r-" + i, "dt_sales", BASE.plus(i, ChronoUnit.MINUTES)));
        }

        List<RefreshHistoryEntry> result = store.query(
                "dt_sales", BASE, BASE.plus(1, ChronoUnit.HOURS), 3);

        assertThat(result).hasSize(3);
        // Most recent 3 entries
        assertThat(result).extracting(RefreshHistoryEntry::refreshId)
                .containsExactly("r-9", "r-8", "r-7");
    }

    @Test
    void purgeRemovesEntriesOlderThanCutoff() {
        store.record(createEntry("r-old", "dt_sales", BASE));
        store.record(createEntry("r-new", "dt_sales", BASE.plus(2, ChronoUnit.DAYS)));

        store.purge(BASE.plus(1, ChronoUnit.DAYS), 1000);

        List<RefreshHistoryEntry> result = store.query(
                "dt_sales", BASE.minus(1, ChronoUnit.DAYS), BASE.plus(3, ChronoUnit.DAYS), 10);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).refreshId()).isEqualTo("r-new");
    }

    @Test
    void purgeTrimsToMaxEntriesKeepingMostRecent() {
        for (int i = 0; i < 5; i++) {
            store.record(createEntry("r-" + i, "dt_sales", BASE.plus(i, ChronoUnit.HOURS)));
        }

        // Keep at most 2 entries, no time-based purge (far future cutoff)
        store.purge(BASE.minus(1, ChronoUnit.DAYS), 2);

        List<RefreshHistoryEntry> result = store.query(
                "dt_sales", BASE, BASE.plus(10, ChronoUnit.HOURS), 10);

        assertThat(result).hasSize(2);
        assertThat(result).extracting(RefreshHistoryEntry::refreshId)
                .containsExactly("r-4", "r-3");
    }

    @Test
    void purgeHandlesMultipleDtNamesIndependently() {
        store.record(createEntry("r-a1", "dt_a", BASE));
        store.record(createEntry("r-a2", "dt_a", BASE.plus(1, ChronoUnit.HOURS)));
        store.record(createEntry("r-b1", "dt_b", BASE));

        store.purge(BASE.plus(30, ChronoUnit.MINUTES), 1000);

        assertThat(store.query("dt_a", BASE, BASE.plus(2, ChronoUnit.HOURS), 10)).hasSize(1);
        assertThat(store.query("dt_b", BASE, BASE.plus(2, ChronoUnit.HOURS), 10)).isEmpty();
    }

    @Test
    void concurrentRecordAndQueryIsSafe() throws InterruptedException {
        int threadCount = 4;
        int opsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threadCount; t++) {
            int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        Instant time = BASE.plus(threadId * opsPerThread + i, ChronoUnit.MINUTES);
                        store.record(createEntry("r-" + threadId + "-" + i, "dt_concurrent", time));
                        store.query("dt_concurrent", BASE, BASE.plus(1, ChronoUnit.DAYS), 100);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
        assertThat(errors.get()).isZero();

        List<RefreshHistoryEntry> all = store.query(
                "dt_concurrent", BASE, BASE.plus(365, ChronoUnit.DAYS), 1000);
        assertThat(all).hasSize(threadCount * opsPerThread);
    }

    private static RefreshHistoryEntry createEntry(String refreshId, String dtName, Instant startTime) {
        return new RefreshHistoryEntry(
                refreshId, dtName, "t1",
                TriggerType.SCHEDULED,
                startTime, startTime.plus(10, ChronoUnit.SECONDS),
                RefreshStatus.SUCCESS,
                100L, 4096L,
                "snap-0", "snap-1",
                null);
    }
}
