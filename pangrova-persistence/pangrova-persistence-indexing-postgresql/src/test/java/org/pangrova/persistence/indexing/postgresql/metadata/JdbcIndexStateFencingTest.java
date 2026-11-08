package org.pangrova.persistence.indexing.postgresql.metadata;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcIndexStateFencingTest {
    private DataSource dataSource;
    private JdbcIndexMetadataStore store;

    @BeforeAll
    void initializeEmptySchema() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("index_state_fence");
        store = new JdbcIndexMetadataStore(dataSource, org.pangrova.metadata.service.TableControlWriteAuthorities.postgres()::requireCurrent);
    }

    @Test
    void eachFenceMismatchLeavesStateTimestampAndReasonUnchanged() {
        var original = register();
        Instant next = original.updatedAt().plusSeconds(1);
        assertThat(store.updateIndexState(original.indexId(), "STALE", next, "lost-time",
                original.updatedAt().minusSeconds(1), "BUILDING", 0)).isFalse();
        assertThat(store.updateIndexState(original.indexId(), "STALE", next, "lost-state",
                original.updatedAt(), "READY", 0)).isFalse();
        assertThat(store.updateIndexState(original.indexId(), "STALE", next, "lost-generation",
                original.updatedAt(), "BUILDING", 1)).isFalse();
        assertThat(store.findIndex(original.indexId())).contains(original);
    }

    @Test
    void reasonPersistsAtomicallyAndClearsOnNonStaleTransition() {
        var original = register();
        Instant next = original.updatedAt().plusSeconds(1);
        assertThat(store.updateIndexState(original.indexId(), "STALE", next, "source changed",
                original.updatedAt(), "BUILDING", 0)).isTrue();
        var stale = store.findIndex(original.indexId()).orElseThrow();
        assertThat(stale.staleReason()).isEqualTo("source changed");
        assertThat(stale.updatedAt()).isEqualTo(next);
        assertThat(stale.state()).isEqualTo(IndexState.STALE);
        assertThat(store.updateIndexState(original.indexId(), "BUILDING", next.plusSeconds(1),
                "must not persist", next, "STALE", 0)).isTrue();
        assertThat(store.findIndex(original.indexId()).orElseThrow().staleReason()).isNull();
    }

    @Test
    void simultaneousStateGenerationAttemptsHaveOneWinner() throws Exception {
        race(true);
    }

    @Test
    void simultaneousTimestampAttemptsHaveOneWinner() throws Exception {
        race(false);
    }

    private void race(boolean fullFence) throws Exception {
        var original = register();
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> attempt(original, "STALE", "first", 1, fullFence, ready, start));
            var second = pool.submit(() -> attempt(original, "READY", "second", 2, fullFence, ready, start));
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            boolean firstWon = first.get(15, TimeUnit.SECONDS);
            boolean secondWon = second.get(15, TimeUnit.SECONDS);
            assertThat(firstWon ^ secondWon).isTrue();
            var committed = store.findIndex(original.indexId()).orElseThrow();
            assertThat(committed.state()).isEqualTo(firstWon ? IndexState.STALE : IndexState.READY);
            assertThat(committed.updatedAt()).isEqualTo(original.updatedAt().plusSeconds(firstWon ? 1 : 2));
            assertThat(committed.staleReason()).isEqualTo(fullFence && firstWon ? "first" : null);
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        }
    }

    private boolean attempt(IndexDescriptor original, String state, String reason, int seconds,
                            boolean fullFence, CountDownLatch ready, CountDownLatch start) throws Exception {
        var competitor = new JdbcIndexMetadataStore(dataSource, org.pangrova.metadata.service.TableControlWriteAuthorities.postgres()::requireCurrent);
        ready.countDown();
        if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Race did not start");
        return fullFence
                ? competitor.updateIndexState(original.indexId(), state, original.updatedAt().plusSeconds(seconds),
                        reason, original.updatedAt(), "BUILDING", 0)
                : competitor.updateIndexState(original.indexId(), state, original.updatedAt().plusSeconds(seconds),
                        original.updatedAt());
    }

    private IndexDescriptor register() {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.parse("2026-09-05T00:00:00Z");
        var descriptor = new IndexDescriptor(id, id, "catalog", "schema", "table", List.of("embedding"),
                IndexType.VECTOR, Map.of(), IndexState.BUILDING, null, 0, "snapshot-1", null, now, now, "tenant");
        store.registerIndex(descriptor);
        return descriptor;
    }
}
