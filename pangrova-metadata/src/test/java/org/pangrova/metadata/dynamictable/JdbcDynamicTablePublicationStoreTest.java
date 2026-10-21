package org.pangrova.metadata.dynamictable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.coordination.JdbcWriteCommitJournal;
import org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL publication contracts. Journal fixtures are metadata oracles, not Native/SQL acceptance. */
class JdbcDynamicTablePublicationStoreTest extends ContainerTestBase {
    private static final String TENANT = "tenant-dynamic-table";
    private static final String NAME = "lake.analytics.daily_events";
    private static final String DEFINITION = "SELECT event_id FROM lake.raw.events";
    private DataSource dataSource;
    private JdbcDynamicTablePublicationStore store;
    private JdbcExecutionAnchorStore anchors;
    private long controlEpoch;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("dynamic_table_publication");
        store = new JdbcDynamicTablePublicationStore(dataSource);
        anchors = new JdbcExecutionAnchorStore(dataSource);
        controlEpoch = new JdbcDistributedLease(dataSource)
                .tryAcquire("coordinator_leader", "coordinator", Duration.ofMinutes(5)).orElseThrow().epoch();
    }

    @Test
    void definitionAndPublishedGenerationSurviveStoreReopen() {
        var reserved = create(TENANT, NAME, "initial");
        assertThat(reserved.first()).isTrue();
        assertThat(reserved.publication().state()).isEqualTo(DynamicTablePublication.State.CREATING);
        assertThat(reserved.publication().publishedTable()).isNull();
        commit(reserved.attempt());
        var receipt = store.publish(reserved.attempt(), reserved.attempt().issuer());
        var published = new JdbcDynamicTablePublicationStore(dataSource).load(TENANT, NAME).orElseThrow();
        assertThat(published.definitionSql()).isEqualTo(DEFINITION);
        assertThat(published.bindingCatalog()).isEqualTo("lake");
        assertThat(published.bindingSchema()).isEqualTo("raw");
        assertThat(published.publishedTable()).isEqualTo(reserved.attempt().candidateTable());
        assertThat(published.revision()).isEqualTo(1);
        assertThat(published.state()).isEqualTo(DynamicTablePublication.State.ACTIVE);
        assertThat(receipt.state()).isEqualTo(DynamicTableRefreshAttempt.State.PUBLISHED);
        assertThat(new JdbcDynamicTablePublicationStore(dataSource).findRefresh(TENANT, "initial")).contains(receipt);
    }

    @Test
    void staleRefreshCannotReplacePublishedGeneration() {
        var initial = create(TENANT, NAME, "initial");
        publish(initial.attempt());
        var head = store.load(TENANT, NAME).orElseThrow();
        var winner = store.reserveRefresh(head, issuer("winner"));
        var loser = store.reserveRefresh(head, issuer("loser"));
        commit(winner.attempt());
        commit(loser.attempt());
        store.publish(winner.attempt(), winner.attempt().issuer());
        assertThatThrownBy(() -> store.publish(loser.attempt(), loser.attempt().issuer()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("changed during refresh");
        assertThat(store.load(TENANT, NAME).orElseThrow().publishedTable()).isEqualTo(winner.attempt().candidateTable());
        assertThat(store.findRefresh(TENANT, "loser").orElseThrow().state())
                .isEqualTo(DynamicTableRefreshAttempt.State.PREPARING);
    }

    @Test
    void droppedNameCannotBeReusedByOldRefreshOrCleanup() {
        var initial = create(TENANT, NAME, "initial");
        publish(initial.attempt());
        var first = store.load(TENANT, NAME).orElseThrow();
        var stale = store.reserveRefresh(first, issuer("stale"));
        commit(stale.attempt());
        store.delete(first);
        var next = create(TENANT, NAME, "replacement");
        assertThat(next.publication().incarnationId()).isNotEqualTo(first.incarnationId());
        assertThatThrownBy(() -> store.publish(stale.attempt(), stale.attempt().issuer()))
                .isInstanceOf(IllegalStateException.class);
        store.abandonCreation(initial.publication());
        assertThat(store.load(TENANT, NAME)).contains(next.publication());
        assertThat(store.findRefresh(TENANT, "initial")).isPresent();
    }

    @Test
    void tenantIdentityIsolatesSameLogicalName() {
        var first = create(TENANT, NAME, "first");
        create("other-tenant", NAME, "second");
        publish(first.attempt());
        assertThat(store.load("other-tenant", NAME).orElseThrow().publishedTable()).isNull();
        assertThat(store.list(TENANT)).extracting(DynamicTablePublication::tenantId).containsExactly(TENANT);
        assertThat(store.findRefresh("other-tenant", "first")).isEmpty();
    }

    @Test
    void sameSimpleNameInDifferentSchemasHasIndependentHead() {
        var first = create(TENANT, NAME, "first");
        String otherSchema = "lake.reporting.daily_events";
        create(TENANT, otherSchema, "second");
        publish(first.attempt());
        assertThat(store.load(TENANT, otherSchema).orElseThrow().publishedTable()).isNull();
        assertThat(store.list(TENANT)).extracting(DynamicTablePublication::logicalName)
                .containsExactlyInAnyOrder(NAME, otherSchema);
    }

    @Test
    void databaseRejectsPublicationOfAnUnregisteredCandidate() {
        create(TENANT, NAME, "initial");
        assertThatThrownBy(() -> {
            try (var connection = dataSource.getConnection(); var update = connection.prepareStatement(
                    "UPDATE dynamic_table_publications SET published_table=?, revision=1, lifecycle_state='ACTIVE' "
                            + "WHERE tenant_id=? AND logical_name=?")) {
                update.setString(1, "lake.analytics.some_unowned_table");
                update.setString(2, TENANT);
                update.setString(3, NAME);
                update.executeUpdate();
            }
        }).isInstanceOf(java.sql.SQLException.class).hasMessageContaining("exact preparing attempt");
    }

    @Test
    void databaseRejectsRebindingAnExistingDefinition() {
        create(TENANT, NAME, "initial");
        assertThatThrownBy(() -> {
            try (var connection = dataSource.getConnection(); var update = connection.prepareStatement(
                    "UPDATE dynamic_table_publications SET binding_schema='another_source_schema' "
                            + "WHERE tenant_id=? AND logical_name=?")) {
                update.setString(1, TENANT);
                update.setString(2, NAME);
                update.executeUpdate();
            }
        }).isInstanceOf(java.sql.SQLException.class).hasMessageContaining("definition identity is immutable");
    }

    @Test
    void repeatedCreationAndReservationNeverIssueAnotherCandidate() {
        var initial = create(TENANT, NAME, "initial");
        var duplicate = create(TENANT, NAME, "initial");
        assertThat(duplicate.first()).isFalse();
        assertThat(duplicate.attempt()).isEqualTo(initial.attempt());
        var retry = store.reserveRefresh(initial.publication(), initial.attempt().issuer());
        assertThat(retry.first()).isFalse();
        assertThat(retry.attempt()).isEqualTo(initial.attempt());
    }

    @Test
    void candidateCannotPublishBeforeItsNativeCommitAndExactPublicationEvidence() {
        var initial = create(TENANT, NAME, "initial");
        assertThatThrownBy(() -> store.publish(initial.attempt(), initial.attempt().issuer()))
                .isInstanceOf(IllegalStateException.class).satisfies(failure ->
                        assertThat(failure.getCause()).isInstanceOf(java.sql.SQLException.class)
                                .hasMessageContaining("no exact committed Native publication"));
    }

    @Test
    void candidateReferenceCannotReplaceItsPersistedPhysicalIdentity() {
        var initial = create(TENANT, NAME, "initial");
        var original = initial.attempt();
        var forged = new DynamicTableRefreshAttempt(original.tenantId(), original.logicalName(),
                original.incarnationId(), original.baseRevision(), original.issuer(),
                "lake.analytics.__pg_dt_another_candidate", original.state(), null, null);
        assertThatThrownBy(() -> store.publish(forged, original.issuer()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("durable attempt identity");
    }

    @Test
    void publishedReceiptIsIdempotentAfterANewerHeadAndJournalCleanup() throws Exception {
        var initial = create(TENANT, NAME, "initial");
        var receipt = publish(initial.attempt());
        var next = store.reserveRefresh(store.load(TENANT, NAME).orElseThrow(), issuer("next"));
        publish(next.attempt());
        try (var connection = dataSource.getConnection(); var delete = connection.prepareStatement(
                "DELETE FROM write_commit_journal WHERE query_id='initial'")) {
            delete.executeUpdate();
        }
        assertThat(store.publish(initial.attempt(), initial.attempt().issuer())).isEqualTo(receipt);
        assertThat(store.load(TENANT, NAME).orElseThrow().publishedTable()).isEqualTo(next.attempt().candidateTable());
        assertThat(store.load(TENANT, NAME).orElseThrow().revision()).isEqualTo(2);
    }

    @Test
    void oldIssuerCannotPublishButNewOwnerCanReconcileTheExactCommittedCandidate() {
        var initial = create(TENANT, NAME, "initial");
        commit(initial.attempt());
        anchors.yield(initial.attempt().issuer().owner());
        var acquired = anchors.tryAcquire("initial", "replacement", "new-process", Duration.ofMinutes(5)).orElseThrow();
        var replacement = new DynamicTableRefreshIssuer(acquired, controlEpoch, "test-user");
        assertThatThrownBy(() -> store.publish(initial.attempt(), initial.attempt().issuer()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("owner");
        assertThat(store.publish(initial.attempt(), replacement).state()).isEqualTo(DynamicTableRefreshAttempt.State.PUBLISHED);
        assertThat(store.load(TENANT, NAME).orElseThrow().publishedTable()).isEqualTo(initial.attempt().candidateTable());
    }

    @ParameterizedTest
    @ValueSource(strings = {"target", "tenant", "principal", "owner", "missing-proof"})
    void committedJournalMustMatchTheExactCandidateAndIssuer(String mismatch) {
        var initial = create(TENANT, NAME, "initial");
        commit(initial.attempt(), mismatch);
        var journal = new JdbcWriteCommitJournal(dataSource);
        var intent = journal.getEntry("initial").orElseThrow();
        if (mismatch.equals("missing-proof")) {
            assertThat(journal.requiresPublication(intent)).isTrue();
        } else {
            assertThatThrownBy(() -> journal.requiresPublication(intent))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("durable publication consumer");
        }
        assertThatThrownBy(() -> store.publish(initial.attempt(), initial.attempt().issuer()))
                .isInstanceOf(IllegalStateException.class).satisfies(failure ->
                        assertThat(failure.getCause()).isInstanceOf(java.sql.SQLException.class)
                                .hasMessageContaining("no exact committed Native publication"));
        assertThat(store.load(TENANT, NAME).orElseThrow().revision()).isZero();
        assertThat(store.findRefresh(TENANT, "initial").orElseThrow().state())
                .isEqualTo(DynamicTableRefreshAttempt.State.PREPARING);
    }

    @Test
    void ordinaryNativeJournalRetirementCannotLoseAnUnpublishedCandidateCommit() {
        var initial = create(TENANT, NAME, "initial");
        commit(initial.attempt());
        var journal = new JdbcWriteCommitJournal(dataSource);
        var committed = journal.getEntry("initial").orElseThrow();
        journal.acknowledgeNoDeferredWork(committed.commitFingerprint());
        journal.purge("initial");
        assertThat(journal.getEntry("initial")).isEmpty();
        var published = store.publish(initial.attempt(), initial.attempt().issuer());
        assertThat(published.state()).isEqualTo(DynamicTableRefreshAttempt.State.PUBLISHED);
        assertThat(published.commitFingerprint()).isEqualTo(committed.commitFingerprint());
        assertThat(published.publication()).isEqualTo(committed.publication());
    }

    @Test
    void retainedNativeProofCannotBeForgedChangedOrDeleted() throws Exception {
        var initial = create(TENANT, NAME, "initial");
        assertThatThrownBy(() -> {
            try (var connection = dataSource.getConnection(); var insert = connection.prepareStatement(
                    "INSERT INTO dynamic_table_refresh_native_commits VALUES (?, 'initial',repeat('0',64),101,"
                            + "decode(repeat('00',32),'hex'))")) {
                insert.setString(1, TENANT);
                insert.executeUpdate();
            }
        }).isInstanceOf(java.sql.SQLException.class).hasMessageContaining("exact committed intent");
        commit(initial.attempt());
        for (String mutation : List.of("UPDATE dynamic_table_refresh_native_commits SET published_version=102",
                "DELETE FROM dynamic_table_refresh_native_commits")) {
            assertThatThrownBy(() -> {
                try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                    statement.executeUpdate(mutation);
                }
            }).isInstanceOf(java.sql.SQLException.class).hasMessageContaining("evidence is immutable");
        }
        assertThat(store.publish(initial.attempt(), initial.attempt().issuer()).publication().publishedVersion())
                .isEqualTo(101);
    }

    @Test
    void nativeCommitCaptureRollsBackWithItsJournalTransition() throws Exception {
        var initial = create(TENANT, NAME, "initial");
        commit(initial.attempt(), "pending");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            assertThat(statement.executeUpdate("UPDATE write_commit_journal SET state='COMMITTED' WHERE query_id='initial'"))
                    .isEqualTo(1);
            try (var rows = statement.executeQuery("SELECT count(*) FROM dynamic_table_refresh_native_commits")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(1);
            }
            connection.rollback();
        }
        var journal = new JdbcWriteCommitJournal(dataSource);
        assertThat(journal.getEntry("initial").orElseThrow().state()).isEqualTo(WriteCommitEntry.State.PENDING);
        assertThatThrownBy(() -> store.publish(initial.attempt(), initial.attempt().issuer()))
                .isInstanceOf(IllegalStateException.class).satisfies(failure -> assertThat(failure.getCause())
                        .hasMessageContaining("no exact committed Native publication"));
        journal.markCommitted("initial");
        assertThat(store.publish(initial.attempt(), initial.attempt().issuer()).state())
                .isEqualTo(DynamicTableRefreshAttempt.State.PUBLISHED);
    }

    @Test
    void directAttemptIdentityAndOutcomeChangesAreRejected() throws Exception {
        create(TENANT, NAME, "initial");
        for (String replacement : List.of("candidate_table='lake.analytics.__pg_dt_forged'",
                "principal='another-user'", "base_revision=42", "lifecycle_state='PUBLISHED', "
                        + "commit_fingerprint=repeat('0',64),published_version=101,manifest_sha256=decode(repeat('00',32),'hex')")) {
            assertThatThrownBy(() -> {
                try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                    statement.executeUpdate("UPDATE dynamic_table_refresh_attempts SET " + replacement
                            + " WHERE execution_id='initial'");
                }
            }).isInstanceOf(java.sql.SQLException.class).hasMessageContaining("identity or outcome cannot be replaced");
        }
    }

    @Test
    void directAttemptInsertionCannotInventAnExecutionOwner() throws Exception {
        create(TENANT, NAME, "initial");
        assertThatThrownBy(() -> {
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO dynamic_table_refresh_attempts (tenant_id,execution_id,logical_name,"
                        + "incarnation_id,base_revision,principal,owner_id,owner_incarnation,owner_token,owner_acquired_at,"
                        + "owner_expires_at,control_epoch,candidate_table,lifecycle_state) SELECT tenant_id,'invented',"
                        + "logical_name,incarnation_id,base_revision,principal,owner_id,owner_incarnation,owner_token,"
                        + "owner_acquired_at,owner_expires_at,control_epoch,"
                        + "'lake.analytics.__pg_dt_00000000000000000000000000000000','PREPARING' "
                        + "FROM dynamic_table_refresh_attempts WHERE execution_id='initial'");
            }
        }).isInstanceOf(java.sql.SQLException.class).hasMessageContaining("actual execution owner");
        assertThat(store.findRefresh(TENANT, "invented")).isEmpty();
    }

    @Test
    void replacedControlTermRejectsOldPublisherAndAllowsExactReconciliation() {
        var initial = create(TENANT, NAME, "initial");
        commit(initial.attempt());
        var leases = new JdbcDistributedLease(dataSource);
        assertThat(leases.release("coordinator_leader", "coordinator", controlEpoch)).isTrue();
        long nextEpoch = leases.tryAcquire("coordinator_leader", "replacement", Duration.ofMinutes(5))
                .orElseThrow().epoch();
        assertThat(nextEpoch).isGreaterThan(controlEpoch);
        assertThatThrownBy(() -> store.publish(initial.attempt(), initial.attempt().issuer()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("control term");
        var publisher = new DynamicTableRefreshIssuer(initial.attempt().issuer().owner(), nextEpoch, "test-user");
        assertThat(store.publish(initial.attempt(), publisher).state()).isEqualTo(DynamicTableRefreshAttempt.State.PUBLISHED);
    }

    @Test
    void concurrentDuplicateReservationsReturnOneDurableCandidate() throws Exception {
        var owner = issuer("initial");
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        try {
            var pending = new java.util.ArrayList<java.util.concurrent.Future<JdbcDynamicTablePublicationStore.Reservation>>();
            for (int i = 0; i < 2; i++) {
                pending.add(executor.submit(() -> {
                    if (!start.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("start missing");
                    return store.create(new DynamicTableDefinition(TENANT, NAME, DEFINITION, "lake", "raw"), owner);
                }));
            }
            start.countDown();
            var first = pending.get(0).get(10, java.util.concurrent.TimeUnit.SECONDS);
            var second = pending.get(1).get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(List.of(first.first(), second.first())).containsExactlyInAnyOrder(true, false);
            assertThat(first.attempt()).isEqualTo(second.attempt());
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void simultaneousCandidatesPublishExactlyOneHeadAndOneReceipt() throws Exception {
        var initial = create(TENANT, NAME, "initial");
        publish(initial.attempt());
        var head = store.load(TENANT, NAME).orElseThrow();
        var left = store.reserveRefresh(head, issuer("left")).attempt();
        var right = store.reserveRefresh(head, issuer("right")).attempt();
        commit(left);
        commit(right);
        var start = new java.util.concurrent.CountDownLatch(1);
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var pending = new java.util.ArrayList<java.util.concurrent.Future<Boolean>>();
            for (var candidate : List.of(left, right)) {
                pending.add(executor.submit(() -> {
                    if (!start.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("start missing");
                    try {
                        store.publish(candidate, candidate.issuer());
                        return true;
                    } catch (IllegalStateException failure) {
                        assertThat(failure).hasMessageContaining("changed during refresh");
                        return false;
                    }
                }));
            }
            start.countDown();
            assertThat(List.of(pending.get(0).get(10, java.util.concurrent.TimeUnit.SECONDS),
                    pending.get(1).get(10, java.util.concurrent.TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(store.load(TENANT, NAME).orElseThrow().revision()).isEqualTo(2);
            assertThat(List.of(store.findRefresh(TENANT, "left").orElseThrow().state(),
                    store.findRefresh(TENANT, "right").orElseThrow().state()))
                    .containsExactlyInAnyOrder(DynamicTableRefreshAttempt.State.PREPARING,
                            DynamicTableRefreshAttempt.State.PUBLISHED);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void candidateLockWaitCannotPublishAfterExecutionLeaseExpires() throws Exception {
        var initial = create(TENANT, NAME, "initial");
        commit(initial.attempt());
        assertThat(anchors.renewOwned("coordinator", "process", Duration.ofSeconds(3))).hasSize(1);
        var publisher = issuer("initial");
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            int pid;
            try (var statement = blocker.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                assertThat(rows.next()).isTrue();
                pid = rows.getInt(1);
            }
            try (var statement = blocker.createStatement(); var rows = statement.executeQuery(
                    "SELECT execution_id FROM dynamic_table_refresh_attempts WHERE execution_id='initial' FOR UPDATE")) {
                assertThat(rows.next()).isTrue();
            }
            var pending = executor.submit(() -> store.publish(initial.attempt(), publisher));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                try (var observer = dataSource.getConnection(); var query = observer.prepareStatement(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid)))")) {
                    query.setInt(1, pid);
                    try (var rows = query.executeQuery()) { return rows.next() && rows.getBoolean(1); }
                }
            });
            try (var wait = blocker.prepareStatement(
                    "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM (?::timestamptz-clock_timestamp())))+0.05)")) {
                wait.setObject(1, publisher.owner().expiresAt().atOffset(java.time.ZoneOffset.UTC));
                wait.executeQuery().close();
            }
            assertThat(pending.isDone()).isFalse();
            blocker.commit();
            assertThatThrownBy(() -> pending.get(10, java.util.concurrent.TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .satisfies(failure -> assertThat(failure.getCause().getCause())
                            .isInstanceOf(java.sql.SQLException.class)
                            .hasMessageContaining("publisher owner is absent, replaced or expired"));
            assertThat(store.load(TENANT, NAME).orElseThrow().revision()).isZero();
            assertThat(store.findRefresh(TENANT, "initial").orElseThrow().state())
                    .isEqualTo(DynamicTableRefreshAttempt.State.PREPARING);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private JdbcDynamicTablePublicationStore.Reservation create(String tenant, String name, String executionId) {
        return store.create(new DynamicTableDefinition(tenant, name, DEFINITION, "lake", "raw"), issuer(executionId));
    }

    private DynamicTableRefreshIssuer issuer(String executionId) {
        var owner = anchors.current(executionId).orElseGet(() -> anchors.tryAcquire(executionId,
                "coordinator", "process", Duration.ofMinutes(5)).orElseThrow());
        return new DynamicTableRefreshIssuer(owner, controlEpoch, "test-user");
    }

    private DynamicTableRefreshAttempt publish(DynamicTableRefreshAttempt attempt) {
        commit(attempt);
        return store.publish(attempt, attempt.issuer());
    }

    private void commit(DynamicTableRefreshAttempt attempt) {
        commit(attempt, "none");
    }

    private void commit(DynamicTableRefreshAttempt attempt, String mismatch) {
        String[] target = attempt.candidateTable().split("\\.");
        if (mismatch.equals("target")) target[2] = "another_candidate";
        String tenant = mismatch.equals("tenant") ? "another-tenant" : attempt.tenantId();
        String principal = mismatch.equals("principal") ? "another-user" : attempt.issuer().principal();
        var receipt = MetadataWriteReceiptFixtures.receipt();
        List<byte[]> fragments = List.of(new byte[] {1});
        byte[] binding = {2};
        var fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(attempt.executionId(),
                tenant, target[0], target[1] + "." + target[2], "INSERT", -1, binding,
                principal, receipt, null), fragments);
        var owner = attempt.issuer().owner();
        var journal = new JdbcWriteCommitJournal(dataSource);
        journal.recordIntent(new WriteCommitEntry(attempt.executionId(), target[0], target[1] + "." + target[2],
                WriteCommitEntry.State.PENDING, System.currentTimeMillis(), fragments, "INSERT", fingerprint,
                tenant, principal, receipt, -1, binding, fingerprint,
                mismatch.equals("owner") ? "another-owner" : owner.ownerId(),
                owner.ownerIncarnation(), owner.fencingToken(), null, null));
        if (!mismatch.equals("missing-proof")) journal.recordPublication(attempt.executionId(), fingerprint,
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 101, new byte[32]));
        if (!mismatch.equals("pending")) journal.markCommitted(attempt.executionId());
    }
}
