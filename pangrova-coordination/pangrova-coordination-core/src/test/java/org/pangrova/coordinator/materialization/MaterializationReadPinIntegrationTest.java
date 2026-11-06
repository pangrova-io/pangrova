package org.pangrova.coordinator.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.DynamicTablePublicationFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.materialization.store.jdbc.JdbcMaterializationRepository;
import org.pangrova.materialization.usecase.ReaderPinLeases;
import org.pangrova.query.api.lifecycle.QueryState;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.testkit.ContainerTestBase;

/** Actual query issuance and PostgreSQL failures; successful physical reads are covered by SQL E2E. */
class MaterializationReadPinIntegrationTest extends ContainerTestBase {

    @Test
    void absentLeaseServiceOrTextualQueryIdCannotAuthorizeGenerationRead() throws Exception {
        var session = session("text-is-not-authority");
        var candidate = unpublishedCandidate();
        assertThatThrownBy(() -> new MaterializationReadPin(Optional.empty(), session).accept(candidate))
                .hasMessageContaining("reader pin lease service");
        var leases = new ReaderPinLeases(repository(), Instant::now);
        assertThatThrownBy(() -> new MaterializationReadPin(Optional.of(leases), session).accept(candidate))
                .hasMessageContaining("admitted query authority");
    }

    @Test
    void terminalOwnerRejectsLateAcquisitionBeforeAnyStorageAccess() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        var state = new QueryStateMachine();
        var authority = state.bindConnectorAuthority(tracker.openQueryIssuance("late-pin"));
        var leases = new ReaderPinLeases(repository(), () -> {
            throw new AssertionError("Closed query must not start lease acquisition");
        });
        var session = SessionContextConverter.toConnectorSession(context(authority.queryId())
                .withQueryAuthority(authority));
        var pin = new MaterializationReadPin(Optional.of(leases), session);
        state.tryTransition(QueryState.CANCELLED);
        tracker.sealQueryIssuance(authority.queryId());
        assertThatThrownBy(() -> pin.accept(unpublishedCandidate()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("admission is closed");
        authority.requireDrained();
        tracker.retireQueryIssuance(authority.queryId());
    }

    @Test
    void acquisitionHoldsQueryDrainUntilTheActualOperationExitsEvenOnStorageFailure() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        var state = new QueryStateMachine();
        var authority = state.bindConnectorAuthority(tracker.openQueryIssuance("in-flight-pin"));
        var entered = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        var leases = new ReaderPinLeases(repository(), () -> {
            entered.countDown();
            try {
                if (!proceed.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Lease clock timed out");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            return Instant.now();
        });
        var session = SessionContextConverter.toConnectorSession(context(authority.queryId())
                .withQueryAuthority(authority));
        var pin = new MaterializationReadPin(Optional.of(leases), session);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var acquisition = executor.submit(() -> pin.accept(unpublishedCandidate()));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            state.tryTransition(QueryState.CANCELLED);
            tracker.sealQueryIssuance(authority.queryId());
            assertThatThrownBy(authority::requireDrained).isInstanceOf(IllegalStateException.class);
            proceed.countDown();
            assertThatThrownBy(() -> acquisition.get(10, TimeUnit.SECONDS))
                    .rootCause().hasMessageContaining("no longer exists and cannot be pinned");
            authority.requireDrained();
            tracker.retireQueryIssuance(authority.queryId());
        } finally {
            proceed.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static JdbcMaterializationRepository repository() throws java.sql.SQLException {
        return new JdbcMaterializationRepository(
                SharedPostgresContainer.migratedDataSource("read_pin_admission"));
    }

    private static PublishedMaterialization unpublishedCandidate() {
        return DynamicTablePublicationFixtures.create(DynamicTablePublicationFixtures.catalog(),
                "tenant", "lake.analytics.rollup", "SELECT 1");
    }

    private static ConnectorSession session(String queryId) {
        return SessionContextConverter.toConnectorSession(context(queryId));
    }

    private static QuerySession context(String queryId) {
        return new QuerySession(queryId, Map.of("queryId", queryId),
                new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }
}
