package org.pangrova.streaming.cdc.commit;

import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.streaming.TopicPartitionOffset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TwoPhaseCommitCoordinator} lifecycle:
 * prepare → commit, prepare → rollback, idempotent dedup, and recovery.
 *
 * <p>Uses PostgreSQL TestContainer for unit testing.</p>
 */
class TwoPhaseCommitCoordinatorTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() { PG.start(); }

    @AfterAll
    static void stopContainer() { PG.stop(); }

    private javax.sql.DataSource dataSource;
    private JdbcCdcCommitStateStore stateStore;
    private TwoPhaseCommitCoordinator coordinator;

    @BeforeEach
    void setUp() throws Exception {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        dataSource = ds;

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS cdc_commit_state");
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS cdc_commit_state (
                    id              VARCHAR(255)  NOT NULL PRIMARY KEY,
                    source_id       VARCHAR(255)  NOT NULL,
                    partition_num   INT           NOT NULL,
                    start_offset    BIGINT        NOT NULL,
                    end_offset      BIGINT        NOT NULL,
                    state           VARCHAR(32)   NOT NULL,
                    deadline        TIMESTAMP     NOT NULL,
                    created_at      TIMESTAMP     NOT NULL,
                    batch_key       VARCHAR(255)
                )""");
            stmt.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_cdc_commit_state_dedup
                    ON cdc_commit_state (source_id, batch_key)
                    WHERE state <> 'ROLLED_BACK'""");
        }

        stateStore = new JdbcCdcCommitStateStore(dataSource);
        coordinator = new TwoPhaseCommitCoordinator(stateStore, Duration.ofMinutes(5));
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM cdc_commit_state");
        }
    }

    @Test
    void prepareCreatesTokenWithCorrectFields() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 100, 199);

        PrepareToken token = coordinator.prepare(source, range);

        assertNotNull(token.id());
        assertEquals(source, token.source());
        assertEquals(range, token.range());
        assertTrue(token.deadline().isAfter(Instant.now()));
    }

    @Test
    void commitTransitionsToCommittedState() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 0, 49);

        PrepareToken token = coordinator.prepare(source, range);
        claimAndCommit(token);

        // Verify state is COMMITTED
        CdcCommitState state = stateStore.findStateById(token.id()).orElseThrow();
        assertEquals(CdcCommitState.COMMITTED, state);
    }

    @Test
    void commitCannotAcknowledgeUnpublishedPrepare() {
        PrepareToken token = coordinator.prepare(
                new SourceId("kafka-orders"), new OffsetRange(0, 0, 49));

        assertThrows(IllegalStateException.class, () -> coordinator.commit(token));
        assertEquals(CdcCommitState.PREPARED,
                stateStore.findStateById(token.id()).orElseThrow());
    }

    @Test
    void rollbackTransitionsToRolledBackState() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(1, 200, 299);

        PrepareToken token = coordinator.prepare(source, range);
        coordinator.rollback(token);

        CdcCommitState state = stateStore.findStateById(token.id()).orElseThrow();
        assertEquals(CdcCommitState.ROLLED_BACK, state);
    }

    @Test
    void commitIsIdempotent() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 50, 99);

        PrepareToken token = coordinator.prepare(source, range);
        claimAndCommit(token);
        // Second commit should be a no-op
        assertDoesNotThrow(() -> coordinator.commit(token));

        CdcCommitState state = stateStore.findStateById(token.id()).orElseThrow();
        assertEquals(CdcCommitState.COMMITTED, state);
    }

    @Test
    void rollbackIsIdempotent() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 300, 399);

        PrepareToken token = coordinator.prepare(source, range);
        coordinator.rollback(token);
        // Second rollback should be a no-op
        assertDoesNotThrow(() -> coordinator.rollback(token));

        CdcCommitState state = stateStore.findStateById(token.id()).orElseThrow();
        assertEquals(CdcCommitState.ROLLED_BACK, state);
    }

    @Test
    void rolledBackPrepareCanRetryTheSameSourceBatchKey() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 300, 399);
        PrepareToken first = coordinator.prepare(source, range, "stable-source-batch");
        coordinator.rollback(first);

        PrepareToken retry = coordinator.prepare(source, range, "stable-source-batch");

        assertNotEquals(first.id(), retry.id());
        assertEquals(CdcCommitState.ROLLED_BACK,
                stateStore.findStateById(first.id()).orElseThrow());
        assertEquals(CdcCommitState.PREPARED,
                stateStore.findStateById(retry.id()).orElseThrow());
    }

    @Test
    void prepareDeduplicatesSameOffsetRange() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 100, 199);

        PrepareToken first = coordinator.prepare(source, range);
        PrepareToken second = coordinator.prepare(source, range);

        assertEquals(first.id(), second.id());
    }

    @Test
    void prepareDifferentPartitionsAreIndependent() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range0 = new OffsetRange(0, 100, 199);
        OffsetRange range1 = new OffsetRange(1, 100, 199);

        PrepareToken token0 = coordinator.prepare(source, range0);
        PrepareToken token1 = coordinator.prepare(source, range1);

        assertNotEquals(token0.id(), token1.id());
    }

    @Test
    void commitAfterRollbackThrows() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 400, 499);

        PrepareToken token = coordinator.prepare(source, range);
        coordinator.rollback(token);

        assertThrows(PangrovaException.class, () -> coordinator.commit(token));
    }

    @Test
    void rollbackAfterCommitThrows() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 500, 599);

        PrepareToken token = coordinator.prepare(source, range);
        claimAndCommit(token);

        assertThrows(PangrovaException.class, () -> coordinator.rollback(token));
        AtomicBoolean aborted = new AtomicBoolean();
        CommitParticipant published = new CommitParticipant() {
            @Override public String name() { return "published"; }
            @Override public void prepare() { }
            @Override public void commit() { }
            @Override public void abort() { aborted.set(true); }
        };
        assertThrows(IllegalStateException.class,
                () -> coordinator.rollback(token, List.of(published)));
        assertFalse(aborted.get(), "published files must not be discarded");
    }

    @Test
    void participantRollbackClaimsStateBeforeAbortingStagedFiles() {
        PrepareToken token = coordinator.prepare(
                new SourceId("kafka-orders"), new OffsetRange(0, 600, 699));
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean competingPublicationClaimed = new AtomicBoolean();
        CommitParticipant staged = new CommitParticipant() {
            @Override public String name() { return "staged"; }
            @Override public void prepare() { }
            @Override public void commit() { }
            @Override public void abort() {
                aborted.set(true);
                // Interleave a second coordinator at the exact point where a
                // catalog file would be deleted. It must not be able to publish.
                competingPublicationClaimed.set(new JdbcCdcCommitStateStore(dataSource)
                        .compareAndSetState(token.id(), CdcCommitState.PREPARED,
                                CdcCommitState.COMMITTING));
            }
        };

        coordinator.rollback(token, List.of(staged));

        assertTrue(aborted.get());
        assertFalse(competingPublicationClaimed.get(),
                "publication cannot start while staged files are being aborted");
        assertEquals(CdcCommitState.ROLLED_BACK,
                stateStore.findStateById(token.id()).orElseThrow());
    }

    @Test
    void recoverRollsBackExpiredPreparedTransactions() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 600, 699);

        // Create a token with an already-expired deadline
        PrepareToken expiredToken = new PrepareToken(
                UUID.randomUUID(), source, range, Instant.now().minusSeconds(60));
        stateStore.insert(expiredToken, CdcCommitState.PREPARED);

        List<PrepareToken> recovered = coordinator.recover(source);

        assertEquals(1, recovered.size());
        CdcCommitState state = stateStore.findStateById(expiredToken.id()).orElseThrow();
        assertEquals(CdcCommitState.ROLLED_BACK, state);
    }

    @Test
    void recoverQuarantinesAmbiguousCommittingTransactions() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 700, 799);

        // Simulate a transaction stuck in COMMITTING state
        PrepareToken token = new PrepareToken(
                UUID.randomUUID(), source, range, Instant.now().plusSeconds(300));
        stateStore.insert(token, CdcCommitState.PREPARED);
        stateStore.updateState(token.id(), CdcCommitState.COMMITTING);

        assertThrows(IllegalStateException.class, () -> coordinator.recover(source));
        CdcCommitState state = stateStore.findStateById(token.id()).orElseThrow();
        assertEquals(CdcCommitState.COMMITTING, state);
    }

    @Test
    void recoverReturnsEmptyWhenNoInFlightTransactions() {
        SourceId source = new SourceId("kafka-clean-source");

        List<PrepareToken> recovered = coordinator.recover(source);

        assertTrue(recovered.isEmpty());
    }

    @Test
    void commitUnknownTokenThrows() {
        PrepareToken unknownToken = new PrepareToken(
                UUID.randomUUID(),
                new SourceId("unknown"),
                new OffsetRange(0, 0, 0),
                Instant.now().plusSeconds(300));

        assertThrows(PangrovaException.class, () -> coordinator.commit(unknownToken));
    }

    // --- executeAtomicCommit tests ---

    @Test
    void executeAtomicCommitSucceedsWithTwoParticipants() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 0, 99);

        AtomicBoolean writePrepared = new AtomicBoolean(false);
        AtomicBoolean writeCommitted = new AtomicBoolean(false);
        AtomicBoolean offsetCommitted = new AtomicBoolean(false);

        List<CommitParticipant> participants = List.of(
            new CommitParticipant() {
                @Override public String name() { return "table-write"; }
                @Override public void prepare() { writePrepared.set(true); }
                @Override public void commit() { writeCommitted.set(true); }
                @Override public void abort() {}
            },
            new CommitParticipant() {
                @Override public String name() { return "offset-commit"; }
                @Override public void prepare() {}
                @Override public void commit() { offsetCommitted.set(true); }
                @Override public void abort() {}
            }
        );

        boolean result = coordinator.executeAtomicCommit(source, range, participants);

        assertTrue(result, "should return true for new commit");
        assertTrue(writePrepared.get(), "write participant should be prepared");
        assertTrue(writeCommitted.get(), "write participant should be committed");
        assertTrue(offsetCommitted.get(), "offset participant should be committed");

        // JDBC state should be COMMITTED
        CdcCommitState state = stateStore.findStateById(
                stateStore.findDuplicate(source, range).orElseThrow().id()
        ).orElseThrow();
        assertEquals(CdcCommitState.COMMITTED, state);
    }

    @Test
    void executeAtomicCommitSkipsDuplicateRange() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 100, 199);

        AtomicInteger writeCount = new AtomicInteger(0);

        List<CommitParticipant> participants = List.of(
            new CommitParticipant() {
                @Override public String name() { return "table-write"; }
                @Override public void prepare() { writeCount.incrementAndGet(); }
                @Override public void commit() {}
                @Override public void abort() {}
            }
        );

        // First commit
        boolean first = coordinator.executeAtomicCommit(source, range, participants);
        assertTrue(first, "first commit should execute");
        assertEquals(1, writeCount.get(), "first commit should prepare participant");

        // Second commit with same range (dedup)
        boolean second = coordinator.executeAtomicCommit(source, range, participants);
        assertFalse(second, "duplicate commit should be skipped");
        assertEquals(1, writeCount.get(), "duplicate should not prepare participant again");
    }

    @Test
    void failoverReplayOnFreshCoordinatorIsDedupedExactlyOnce() {
        // A-EOS-01/A-EOS-02 (cross-coordinator exactly-once): after coordinator A commits a batch,
        // a FAILOVER to a brand-new coordinator B that replays the SAME batch (same dedupKey) must
        // be deduped via the shared persistent commit record in Postgres — the write participant
        // runs EXACTLY ONCE across the two coordinators (not once per coordinator).
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 500, 599);
        String dedupKey = "failover-batch-fingerprint";
        AtomicInteger writeCount = new AtomicInteger(0);

        TwoPhaseCommitCoordinator coordA =
                new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource), Duration.ofMinutes(5));
        boolean aCommitted = coordA.executeAtomicCommit(source, range, dedupKey, List.of(
                new CommitParticipant() {
                    @Override public String name() { return "table-write"; }
                    @Override public void prepare() {}
                    @Override public void commit() { writeCount.incrementAndGet(); }
                    @Override public void abort() {}
                }));
        assertTrue(aCommitted, "coordinator A commits the batch");
        assertEquals(1, writeCount.get(), "write ran once on A");

        // Failover: a fresh coordinator sharing the SAME Postgres store replays the same batch.
        TwoPhaseCommitCoordinator coordB =
                new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource), Duration.ofMinutes(5));
        boolean bCommitted = coordB.executeAtomicCommit(source, range, dedupKey, List.of(
                new CommitParticipant() {
                    @Override public String name() { return "table-write"; }
                    @Override public void prepare() {}
                    @Override public void commit() { writeCount.incrementAndGet(); }
                    @Override public void abort() {}
                }));
        assertFalse(bCommitted, "failover replay must be deduped via the shared commit record");
        assertEquals(1, writeCount.get(), "write participant runs EXACTLY ONCE across coordinators");
    }

    @Test
    void failoverCoordinatorRollsBackAbandonedPrepareFromSharedStore() throws Exception {
        // A-EOS-02 (cross-coordinator recovery): coordinator A prepares (stages) then "crashes"
        // without committing. A failover coordinator B sharing the same Postgres store recovers:
        // the abandoned (expired) prepare is rolled back so the source safely replays with no
        // partial-commit leak.
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(2, 0, 42);
        String dedupKey = "abandoned-prepare";

        // Coordinator A prepares with a 1ms deadline, then "crashes" (never commits).
        TwoPhaseCommitCoordinator coordA =
                new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource), Duration.ofMillis(1));
        PrepareToken abandoned = coordA.prepare(source, range, dedupKey);
        assertEquals(CdcCommitState.PREPARED,
                stateStore.findStateById(abandoned.id()).orElseThrow());
        Thread.sleep(50); // let the prepare deadline expire

        // Failover: coordinator B recovers from the shared store.
        TwoPhaseCommitCoordinator coordB =
                new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource), Duration.ofMinutes(5));
        List<PrepareToken> recovered = coordB.recover(source);

        assertFalse(recovered.isEmpty(), "B recovers the abandoned prepare");
        assertEquals(CdcCommitState.ROLLED_BACK,
                stateStore.findStateById(abandoned.id()).orElseThrow(),
                "abandoned prepare rolled back on failover — no partial-commit leak");
    }

    @Test
    void executeAtomicCommitDistinctBatchKeysSameRangeBothCommit() {
        // A-EOS-01: two DISTINCT batches that share the same representative (primary-partition)
        // range must NOT be mis-deduped. Previously the range WAS the dedup key, so the second
        // batch was silently skipped and its other-partition data lost. With a full-batch
        // fingerprint dedup key, both commit.
        SourceId source = new SourceId("kafka-orders");
        OffsetRange sharedRange = new OffsetRange(0, 0, 99);

        AtomicInteger writeCount = new AtomicInteger(0);
        List<CommitParticipant> participants = List.of(
            new CommitParticipant() {
                @Override public String name() { return "table-write"; }
                @Override public void prepare() { writeCount.incrementAndGet(); }
                @Override public void commit() {}
                @Override public void abort() {}
            }
        );

        boolean first = coordinator.executeAtomicCommit(
                source, sharedRange, "fingerprint-batch-A", participants);
        boolean second = coordinator.executeAtomicCommit(
                source, sharedRange, "fingerprint-batch-B", participants);

        assertTrue(first, "batch A should commit");
        assertTrue(second, "batch B (distinct fingerprint, same range) must NOT be mis-deduped");
        assertEquals(2, writeCount.get(), "both distinct batches must be written");
    }

    @Test
    void executeAtomicCommitSameBatchKeyIsDeduped() {
        // A-EOS-01: a crash-recovery replay of the SAME batch (same fingerprint) is deduped exactly
        // once, regardless of the representative range recorded for offset restore.
        SourceId source = new SourceId("kafka-orders");
        AtomicInteger writeCount = new AtomicInteger(0);
        List<CommitParticipant> participants = List.of(
            new CommitParticipant() {
                @Override public String name() { return "table-write"; }
                @Override public void prepare() { writeCount.incrementAndGet(); }
                @Override public void commit() {}
                @Override public void abort() {}
            }
        );

        boolean first = coordinator.executeAtomicCommit(
                source, new OffsetRange(0, 0, 99), "fingerprint-batch-A", participants);
        boolean replay = coordinator.executeAtomicCommit(
                source, new OffsetRange(0, 0, 49), "fingerprint-batch-A", participants);

        assertTrue(first, "first commit should execute");
        assertFalse(replay, "same fingerprint (replay) must be deduped even with a different range");
        assertEquals(1, writeCount.get(), "replay must not write again");
    }

    @Test
    void executeAtomicCommitAbortsAllOnPrepareFailure() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 200, 299);

        AtomicBoolean firstAborted = new AtomicBoolean(false);
        AtomicBoolean secondAborted = new AtomicBoolean(false);

        List<CommitParticipant> participants = List.of(
            new CommitParticipant() {
                @Override public String name() { return "first"; }
                @Override public void prepare() {}
                @Override public void commit() {}
                @Override public void abort() { firstAborted.set(true); }
            },
            new CommitParticipant() {
                @Override public String name() { return "second-fails"; }
                @Override public void prepare() throws Exception {
                    throw new RuntimeException("Simulated prepare failure");
                }
                @Override public void commit() {}
                @Override public void abort() { secondAborted.set(true); }
            }
        );

        assertThrows(RuntimeException.class,
            () -> coordinator.executeAtomicCommit(source, range, participants));

        // First participant should be aborted (it was successfully prepared)
        assertTrue(firstAborted.get(), "first participant should be aborted");
        // Second participant was never prepared, so not in the prepared list
        assertFalse(secondAborted.get(), "second participant was never prepared");

        // JDBC state should be ROLLED_BACK
        PrepareToken token = stateStore.findDuplicate(source, range).orElse(null);
        // After rollback, the state is ROLLED_BACK — findDuplicate won't find it
        // because it only searches PREPARED/COMMITTING/COMMITTED states.
        assertNull(token, "rolled back transaction should not appear as duplicate");
    }

    @Test
    void executeAtomicCommitLeavesAmbiguousPublicationForReconciliation() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 300, 399);

        AtomicBoolean firstAborted = new AtomicBoolean(false);
        AtomicBoolean secondAborted = new AtomicBoolean(false);

        List<CommitParticipant> participants = List.of(
            new CommitParticipant() {
                @Override public String name() { return "first"; }
                @Override public void prepare() {}
                @Override public void commit() throws Exception {
                    throw new RuntimeException("Simulated commit failure");
                }
                @Override public void abort() { firstAborted.set(true); }
            },
            new CommitParticipant() {
                @Override public String name() { return "second"; }
                @Override public void prepare() {}
                @Override public void commit() {}
                @Override public void abort() { secondAborted.set(true); }
            }
        );

        assertThrows(RuntimeException.class,
            () -> coordinator.executeAtomicCommit(source, range, participants));

        assertFalse(firstAborted.get(), "published files must not be discarded after commit starts");
        assertFalse(secondAborted.get(), "participant outcome is ambiguous after commit starts");
        PrepareToken token = stateStore.findDuplicate(source, range).orElseThrow();
        assertEquals(CdcCommitState.COMMITTING,
                stateStore.findStateById(token.id()).orElseThrow());
        assertThrows(IllegalStateException.class,
                () -> coordinator.executeAtomicCommit(source, range, participants));
        assertThrows(IllegalStateException.class, () -> coordinator.rollback(token));
        assertThrows(IllegalStateException.class,
                () -> coordinator.rollback(token, participants));
        assertFalse(firstAborted.get());
        assertFalse(secondAborted.get());
    }

    @Test
    void compareAndSetClaimAllowsOnlyOneCoordinatorToStartPublication() throws Exception {
        SourceId source = new SourceId("kafka-orders");
        PrepareToken token = coordinator.prepare(source, new OffsetRange(0, 800, 899));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var workers = Executors.newFixedThreadPool(2);
        try {
            var first = workers.submit(() -> claimAfterStart(token, ready, start));
            var second = workers.submit(() -> claimAfterStart(token, ready, start));
            assertTrue(ready.await(30, TimeUnit.SECONDS));
            start.countDown();
            boolean firstWon = first.get(30, TimeUnit.SECONDS);
            boolean secondWon = second.get(30, TimeUnit.SECONDS);
            assertNotEquals(firstWon, secondWon);
        } finally {
            start.countDown();
            workers.shutdownNow();
        }
        assertEquals(CdcCommitState.COMMITTING,
                stateStore.findStateById(token.id()).orElseThrow());
    }

    private boolean claimAfterStart(
            PrepareToken token, CountDownLatch ready, CountDownLatch start)
            throws InterruptedException {
        ready.countDown();
        start.await();
        return new JdbcCdcCommitStateStore(dataSource).compareAndSetState(
                token.id(), CdcCommitState.PREPARED, CdcCommitState.COMMITTING);
    }

    @Test
    void executeAtomicCommitRejectsEmptyParticipants() {
        SourceId source = new SourceId("kafka-orders");
        OffsetRange range = new OffsetRange(0, 400, 499);

        assertThrows(IllegalArgumentException.class,
            () -> coordinator.executeAtomicCommit(source, range, List.of()));
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.executeAtomicCommit(source, range, null));
    }

    // --- findLastCommittedOffsets tests ---

    @Test
    void findLastCommittedOffsetsReturnsEndOffsetPlusOne() {
        SourceId source = new SourceId("kafka-orders");

        // Commit two ranges on partition 0
        PrepareToken token1 = coordinator.prepare(source, new OffsetRange(0, 0, 49));
        claimAndCommit(token1);
        PrepareToken token2 = coordinator.prepare(source, new OffsetRange(0, 50, 99));
        claimAndCommit(token2);

        // Commit one range on partition 1
        PrepareToken token3 = coordinator.prepare(source, new OffsetRange(1, 0, 24));
        claimAndCommit(token3);

        Map<TopicPartitionOffset, Long> offsets = coordinator.findLastCommittedOffsets(source);

        assertEquals(2, offsets.size(), "should have 2 partitions");
        assertEquals(100L, offsets.get(new TopicPartitionOffset("kafka-orders", 0)),
            "partition 0 should have endOffset+1=100");
        assertEquals(25L, offsets.get(new TopicPartitionOffset("kafka-orders", 1)),
            "partition 1 should have endOffset+1=25");
    }

    @Test
    void findLastCommittedOffsetsReturnsEmptyWhenNoneCommitted() {
        SourceId source = new SourceId("kafka-empty");

        Map<TopicPartitionOffset, Long> offsets = coordinator.findLastCommittedOffsets(source);

        assertTrue(offsets.isEmpty(), "should return empty map when no committed offsets");
    }

    @Test
    void findLastCommittedOffsetsIgnoresNonCommittedStates() {
        SourceId source = new SourceId("kafka-orders");

        // Prepare but don't commit
        coordinator.prepare(source, new OffsetRange(0, 0, 49));

        // Prepare and rollback
        PrepareToken rolled = coordinator.prepare(source, new OffsetRange(1, 0, 24));
        coordinator.rollback(rolled);

        Map<TopicPartitionOffset, Long> offsets = coordinator.findLastCommittedOffsets(source);

        assertTrue(offsets.isEmpty(),
            "should not include PREPARED or ROLLED_BACK offsets");
    }

    private void claimAndCommit(PrepareToken token) {
        assertTrue(stateStore.compareAndSetState(
                token.id(), CdcCommitState.PREPARED, CdcCommitState.COMMITTING));
        coordinator.commit(token);
    }
}
