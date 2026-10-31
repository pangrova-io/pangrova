package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.admission.QueryRejectedException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.scheduler.core.failure.NoAvailableWorkerException;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.query.api.state.JournalEventContext;
import org.pangrova.connector.spi.coordination.JournalWriteException;
import org.pangrova.query.api.state.QueryStateEvent;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual PostgreSQL commit/lock and exact control/admission owners, not a Native execution test. */
@Tag("integration")
class QueryManagerJournalTest {
    @Test void originalSessionStringsRoundTripThroughActualPostgresWithJsonEscaping() throws Exception {
        try (var store = new QueryTerminalJournalFixture()) {
            String queryId = "q-\"\\\n";
            String tenant = "tenant-\"\\\n\r\t";
            var session = new org.pangrova.coordinator.query.session.CoordinatorSessionContext(
                    queryId, java.util.Map.of("exact_planning_setting", "unchanged"), null, tenant);
            var owner = new QueryJournalLifecycle(store.journal());
            owner.setOwner("session-issuer");
            var state = new QueryStateMachine();
            owner.queued(queryId, "SELECT 1", session, state);
            QueryStateEvent committed = store.journal().latestQueryEvent(queryId).orElseThrow();
            var json = org.pangrova.common.json.provider.jackson.JsonMapperFactory.shared().readTree(committed.sessionJson());
            assertThat(json.size()).isEqualTo(2);
            assertThat(json.get("queryId").asText()).isEqualTo(queryId);
            assertThat(json.get("tenantId").asText()).isEqualTo(tenant);
            assertThat(committed.queryId()).isEqualTo(queryId);
            assertThat(committed.context().coordinatorId()).isEqualTo("session-issuer");
            assertThat(store.committedEvents(queryId)).isEqualTo(List.of(committed));
            assertThat(session.sessionProperties()).containsExactlyEntriesOf(java.util.Map.of("exact_planning_setting", "unchanged"));
        }
    }

    @Test void submitQueryJournalsQueuedSync() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            assertThatThrownBy(() -> fixture.manager.submitQuery("q-1", "SELECT 1", SourceFreeQueryFixture.session("q-1")))
                    .isInstanceOf(NoAvailableWorkerException.class);
            QueryStateEvent queued = fixture.store.committedEvents("q-1").get(0);
            assertThat(queued.state()).isEqualTo("QUEUED");
            assertThat(queued.queryId()).isEqualTo("q-1");
            assertThat(queued.sql()).isEqualTo("SELECT 1");
            assertThat(queued.context().coordinatorId()).isEqualTo("journal-local");
        }
    }

    @Test void failedQueuedJournalWriteRemovesUnownedQuery() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            assertThat(fixture.admission.admit(AdmissionRequest.of("fixture-tenant", "default", "survivor")).isAdmitted()).isTrue();
            fixture.store.journal().close(); // Actual admission seal, before transfer; not an accepted SQL failure.
            assertThatThrownBy(() -> fixture.manager.submitQuery("journal-failure", "SELECT 1",
                    SourceFreeQueryFixture.session("journal-failure"))).isInstanceOf(JournalWriteException.class)
                    .satisfies(error -> assertThat(((JournalWriteException) error).admissionOutcome())
                            .isEqualTo(JournalWriteException.AdmissionOutcome.REJECTED_BEFORE_TRANSFER));
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
            assertThat(fixture.manager.getQueryState("journal-failure")).isEmpty();
            assertThat(fixture.manager.cancelQuery("journal-failure")).isFalse();
            assertThat(fixture.admission.admittedCount("fixture-tenant")).isEqualTo(1);
            fixture.admission.release("journal-failure", "fixture-tenant");
            assertThat(fixture.slots.active("fixture-tenant")).isEqualTo(1);
            fixture.admission.release("survivor", "fixture-tenant");
            assertThat(fixture.admission.admittedCount("fixture-tenant")).isZero();
        }
    }

    @Test void deniedOwnershipNeverChangesAnotherCoordinatorsJournal() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            var foreign = new QueryStateEvent(JournalEventContext.create(1L, "other-coordinator"),
                    "shared-id", "SELECT 1", null, "RUNNING");
            fixture.store.journal().appendQueryEventSync(foreign);
            assertThat(fixture.foreignAuthority.acquireQueryOwnership("shared-id", "other-coordinator")).isTrue();
            assertThatThrownBy(() -> fixture.manager.submitQuery("shared-id", "SELECT 2", SourceFreeQueryFixture.session("shared-id")))
                    .isInstanceOf(QueryRejectedException.class).hasMessageContaining("Failed to acquire query ownership");
            assertThat(fixture.store.journal().latestQueryEvent("shared-id")).contains(foreign);
            assertThat(fixture.store.committedEvents("shared-id")).isEqualTo(List.of(foreign));
            assertThat(fixture.store.queryRows("shared-id")).isEqualTo(1);
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
            assertThat(fixture.admission.admittedCount("fixture-tenant")).isZero();
            assertThat(fixture.foreignAuthority.releaseQueryOwnership("shared-id", "other-coordinator")).isTrue();
        }
    }

    @Test void duplicateTerminalQueryIdPreservesRegisteredStateAndJournal() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            var registered = new QueryStateMachine();
            registered.tryTransition(QueryStateMachine.State.FAILED);
            fixture.manager.registerStateMachine("shared-id", registered);
            assertThatThrownBy(() -> fixture.manager.submitQuery("shared-id", "SELECT 2", SourceFreeQueryFixture.session("shared-id")))
                    .isInstanceOf(QueryRejectedException.class).hasMessageContaining("already registered");
            assertThat(fixture.manager.getStateMachine("shared-id")).isSameAs(registered);
            assertThat(fixture.store.committedEvents("shared-id")).isEmpty();
            assertThat(fixture.store.queryRows("shared-id")).isZero();
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
        }
    }

    @Test void deniedClusterAdmissionRemovesLocalReservationExactlyOnce() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(0)) {
            assertThatThrownBy(() -> fixture.manager.submitQuery("denied-admission", "SELECT 1",
                    SourceFreeQueryFixture.session("denied-admission"))).isInstanceOf(QueryRejectedException.class)
                    .hasMessageContaining("Cluster admission denied");
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
            assertThat(fixture.manager.getQueryState("denied-admission")).isEmpty();
            assertThat(fixture.slots.active("fixture-tenant")).isZero();
            assertThat(fixture.store.committedEvents("denied-admission")).isEmpty();
            assertThat(fixture.store.queryRows("denied-admission")).isZero();
        }
    }

    @Test void terminalJournalWriteDoesNotHoldStateMachineMonitor() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            QueryStateMachine state = fixture.running("slow-journal");
            var executor = Executors.newFixedThreadPool(2);
            try (var blocker = fixture.store.lockQueryInsert()) {
                var completion = executor.submit(() -> fixture.manager.onAllTasksFinished("slow-journal"));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(fixture.store::insertWaitingOnActualLock);
                executor.submit(() -> state.setBeforeTerminalTransition(() -> { })).get(1, TimeUnit.SECONDS);
                assertThat(completion.isDone()).isFalse();
                blocker.rollback();
                completion.get(5, TimeUnit.SECONDS);
            } finally {
                executor.shutdown();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test void onAllTasksFinishedJournalsFinishedSync() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            fixture.running("q-2");
            fixture.manager.onAllTasksFinished("q-2");
            var terminal = fixture.store.committedEvents("q-2").stream().filter(event -> event.state().equals("FINISHED")).toList();
            assertThat(terminal).hasSize(1);
            assertThat(terminal.get(0).queryId()).isEqualTo("q-2");
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
        }
    }

    @Test void onTaskFailedJournalsFailedSync() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            fixture.running("q-3");
            fixture.manager.onTaskFailed("q-3", new ErrorDetail("ERR", "test error", null));
            var terminal = fixture.store.committedEvents("q-3").stream().filter(event -> event.state().equals("FAILED")).toList();
            assertThat(terminal).hasSize(1);
            assertThat(terminal.get(0).queryId()).isEqualTo("q-3");
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
        }
    }

    @Test void acceptedQueuedSqlFailureRetainsExactJournalObligationUntilActualRetry() throws Exception {
        try (var fixture = new QueryManagerJournalFixture(2)) {
            fixture.store.denyQueryInsert();
            assertThatThrownBy(() -> fixture.manager.submitQuery("accepted-failure", "SELECT 1", SourceFreeQueryFixture.session("accepted-failure")))
                    .isInstanceOf(JournalWriteException.class).satisfies(error ->
                            assertThat(((JournalWriteException) error).admissionOutcome())
                                    .isEqualTo(JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED));
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.admission.admittedCount("fixture-tenant")).isEqualTo(1);
            fixture.store.allowQueryInsert();
            fixture.retry();
            assertThat(fixture.manager.awaitAllQueries(5000)).isTrue();
            assertThat(fixture.store.committedEvents("accepted-failure").stream().filter(event -> event.state().equals("QUEUED"))).hasSize(1);
            assertThat(fixture.store.terminalWrites("accepted-failure", "FAILED")).isEqualTo(1);
            assertThat(fixture.admission.admittedCount("fixture-tenant")).isZero();
        }
    }
}
