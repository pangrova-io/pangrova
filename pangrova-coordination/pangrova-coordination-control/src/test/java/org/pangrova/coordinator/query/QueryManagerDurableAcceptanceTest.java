package org.pangrova.coordinator.query;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.api.exception.IdempotencyConflictException;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.testkit.constants.TestObservability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryManagerDurableAcceptanceTest {
    private static final String KEY_DIGEST = "a".repeat(64);
    private static final String FINGERPRINT = "b".repeat(64);
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final CountDownLatch releaseFirst = new CountDownLatch(1);
    private final CountDownLatch releaseSecond = new CountDownLatch(1);
    private final CountingDispatcher firstDispatcher = new CountingDispatcher(releaseFirst);
    private final CountingDispatcher secondDispatcher = new CountingDispatcher(releaseSecond);

    @AfterEach
    void tearDown() throws InterruptedException {
        releaseFirst.countDown();
        releaseSecond.countDown();
        executor.shutdownNow();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void sameKeyReplaysOriginalReceiptWithoutSecondDispatchAndPrincipalsAreIsolated() throws Exception {
        var journal = new InMemoryQueryStateJournal();
        var first = manager(firstDispatcher, journal);
        var second = manager(secondDispatcher, journal);
        var request = acceptance("alice", FINGERPRINT);

        QuerySubmissionResult original = first.submitQueryAsync("q-original", "SELECT 1",
                session("q-original", "alice"), request).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(firstDispatcher.started.await(5, TimeUnit.SECONDS)).isTrue();
        QuerySubmissionResult replay = second.submitQueryAsync("q-replay", "SELECT 1",
                session("q-replay", "alice"), request).toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(original.queryId()).isEqualTo("q-original");
        assertThat(original.state().name()).isEqualTo("QUEUED");
        assertThat(original.resultDisposition().name()).isEqualTo("PENDING");
        assertThat(replay).isEqualTo(original);
        assertThat(secondDispatcher.calls).hasValue(0);
        assertThat(second.getStateMachine("q-replay")).isNull();
        assertThat(journal.findAcceptance("alice", KEY_DIGEST).orElseThrow().receipt()).isEqualTo(original);

        second.submitQueryAsync("q-bob", "SELECT 1", session("q-bob", "bob"),
                acceptance("bob", FINGERPRINT)).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(secondDispatcher.started.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(secondDispatcher.calls).hasValue(1);
    }

    @Test
    void replayReleasesCandidateAdmissionAndThrottle() throws Exception {
        var journal = new InMemoryQueryStateJournal();
        var first = manager(firstDispatcher, journal);
        var admission = new CountingAdmission();
        var second = manager(secondDispatcher, journal, admission);
        var request = acceptance("alice", FINGERPRINT);
        first.submitQueryAsync("q-original", "SELECT 1", session("q-original", "alice"), request)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(firstDispatcher.started.await(5, TimeUnit.SECONDS)).isTrue();

        second.submitQueryAsync("q-replay", "SELECT 1", session("q-replay", "alice"), request)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(admission.throttleReleases).hasValue(1);
        assertThat(admission.clusterReleases).hasValue(1);
        assertThat(secondDispatcher.calls).hasValue(0);
    }

    @Test
    void sameKeyWithDifferentFingerprintConflicts() {
        var journal = new InMemoryQueryStateJournal();
        var first = manager(firstDispatcher, journal);
        first.submitQueryAsync("q-original", "SELECT 1", session("q-original", "alice"),
                acceptance("alice", FINGERPRINT));

        var admission = new CountingAdmission();
        var second = manager(secondDispatcher, journal, admission);
        assertThatThrownBy(() -> second.submitQueryAsync(
                "q-conflict", "SELECT 2", session("q-conflict", "alice"),
                acceptance("alice", "c".repeat(64))))
                .isInstanceOf(IdempotencyConflictException.class);
        assertThat(admission.throttleReleases).hasValue(1);
        assertThat(admission.clusterReleases).hasValue(1);
        assertThat(secondDispatcher.calls).hasValue(0);
    }

    @Test
    void acceptancePrincipalMustMatchAuthenticatedSession() {
        var journal = new InMemoryQueryStateJournal();
        var manager = manager(firstDispatcher, journal);
        assertThatThrownBy(() -> manager.submitQueryAsync("q-mismatch", "SELECT 1",
                session("q-mismatch", "alice"), acceptance("bob", FINGERPRINT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("principal");
        assertThat(journal.findAcceptance("bob", KEY_DIGEST)).isEmpty();
        assertThat(firstDispatcher.calls).hasValue(0);
    }

    @Test
    void uncertainCommitReconcilesTheSameQueryAndKeepsExecutionOwnership() throws Exception {
        var journal = new UncertainCommitJournal(new InMemoryQueryStateJournal());
        var manager = manager(firstDispatcher, journal);
        var receipt = manager.submitQueryAsync("q-uncertain", "SELECT 1",
                session("q-uncertain", "alice"), acceptance("alice", FINGERPRINT))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(receipt.queryId()).isEqualTo("q-uncertain");
        assertThat(firstDispatcher.started.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(journal.delegate.findAcceptance("alice", KEY_DIGEST)).isPresent();
    }

    private QueryManager manager(CountingDispatcher dispatcher, QueryStateJournal journal) {
        return manager(dispatcher, journal,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null));
    }

    private QueryManager manager(CountingDispatcher dispatcher, QueryStateJournal journal,
            org.pangrova.query.lifecycle.QueryAdmissionPort admission) {
        return new QueryManager(QueryManagerTestContexts.minimalAsync(dispatcher, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), executor, journal, admission));
    }

    private static QueryAcceptanceRequest acceptance(String principal, String fingerprint) {
        return new QueryAcceptanceRequest(principal, KEY_DIGEST, fingerprint);
    }

    private static QuerySession session(String queryId, String principal) {
        return new QuerySession(queryId, Map.of(),
                new UserIdentity(principal, Set.of(), Map.of(), Instant.EPOCH), null,
                Optional.empty(), new OtelExecutionTraceContextFactory().capture());
    }

    private static final class CountingAdmission implements org.pangrova.query.lifecycle.QueryAdmissionPort {
        private final AtomicInteger throttleReleases = new AtomicInteger();
        private final AtomicInteger clusterReleases = new AtomicInteger();

        @Override
        public boolean acquireHealthProbe() {
            return true;
        }

        @Override
        public void recordOutcome(org.pangrova.query.api.enums.QueryState state,
                org.pangrova.query.api.error.QueryFailure failure) {
        }

        @Override
        public void checkCluster(String queryId, org.pangrova.security.api.tenant.ResolvedTenantContext tenant,
                QueryStateMachine state) {
            state.retainAdmissionRelease(clusterReleases::incrementAndGet);
        }

        @Override
        public void checkThrottle(String queryId, QuerySession session) {
        }

        @Override
        public void releaseThrottle(String queryId) {
            throttleReleases.incrementAndGet();
        }

        @Override
        public void setClusterController(
                org.pangrova.scheduler.api.admission.ClusterAdmissionController controller) {
        }
    }

    private static final class CountingDispatcher extends QueryManagerAsyncSubmitTest.SlowQueryDispatcher {
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch started;

        private CountingDispatcher(CountDownLatch release) {
            super(new CountDownLatch(1), release);
            this.started = new CountDownLatch(1);
        }

        @Override
        public CompletionStage<DispatchResult> executeAsync(QueryStateMachine state, String sql,
                QuerySession session) {
            calls.incrementAndGet();
            started.countDown();
            return super.executeAsync(state, sql, session);
        }
    }

    private static final class UncertainCommitJournal implements QueryStateJournal {
        private final InMemoryQueryStateJournal delegate;

        private UncertainCommitJournal(InMemoryQueryStateJournal delegate) {
            this.delegate = delegate;
        }

        @Override
        public void appendQueryEvent(QueryStateEvent event) {
            delegate.appendQueryEvent(event);
        }

        @Override
        public void appendQueryEventSync(QueryStateEvent event) {
            delegate.appendQueryEventSync(event);
        }

        @Override
        public QueryAcceptanceAdmission acceptQuerySync(QueryStateEvent event) {
            delegate.acceptQuerySync(event);
            throw new JournalWriteException("Commit response lost", new IllegalStateException("lost response"),
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }

        @Override
        public void appendOwnedTerminalSync(QueryStateEvent event,
                org.pangrova.execution.api.ownership.ExecutionAnchor anchor) {
            delegate.appendOwnedTerminalSync(event, anchor);
        }

        @Override
        public void appendFencedAdmissionSync(QueryStateEvent event,
                org.pangrova.execution.api.ownership.ExecutionAnchor anchor) {
            delegate.appendFencedAdmissionSync(event, anchor);
        }

        @Override
        public QueryAcceptanceAdmission acceptFencedQuerySync(QueryStateEvent event,
                org.pangrova.execution.api.ownership.ExecutionAnchor anchor) {
            return delegate.acceptFencedQuerySync(event, anchor);
        }

        @Override
        public Optional<org.pangrova.query.api.journal.QueryAcceptance> findAcceptance(String principal,
                String idempotencyKeyDigest) {
            return delegate.findAcceptance(principal, idempotencyKeyDigest);
        }

        @Override
        public void appendTaskEvent(TaskStateEvent event) {
            delegate.appendTaskEvent(event);
        }

        @Override
        public void appendTaskIssuancesSync(List<TaskStateEvent> events) {
            delegate.appendTaskIssuancesSync(events);
        }

        @Override
        public void appendTaskEventSync(TaskStateEvent event) {
            delegate.appendTaskEventSync(event);
        }

        @Override
        public List<QueryStateEvent> replayActiveQueries() {
            return delegate.replayActiveQueries();
        }

        @Override
        public List<TaskStateEvent> replayActiveTasks() {
            return delegate.replayActiveTasks();
        }

        @Override
        public RecoveryBatch replayClaimedQueries(java.util.Set<String> queryIds) {
            return delegate.replayClaimedQueries(queryIds);
        }

        @Override
        public Optional<QueryStateEvent> latestQueryEvent(String queryId) {
            return delegate.latestQueryEvent(queryId);
        }

        @Override
        public Optional<String> latestQueryStateForPrincipal(String queryId, String principal) {
            return delegate.latestQueryStateForPrincipal(queryId, principal);
        }

        @Override
        public void purgeQuery(String queryId) {
            delegate.purgeQuery(queryId);
        }

        @Override
        public void purgeExpiredAcceptances() {
            delegate.purgeExpiredAcceptances();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public boolean isDrained() {
            return delegate.isDrained();
        }
    }
}
