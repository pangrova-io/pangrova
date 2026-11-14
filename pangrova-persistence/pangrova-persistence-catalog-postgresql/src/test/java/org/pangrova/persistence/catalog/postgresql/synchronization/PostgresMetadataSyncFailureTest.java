package org.pangrova.persistence.catalog.postgresql.synchronization;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncOutcomeUnknownException;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Faults wrap public JDBC calls on a real database; no private-state reflection or mocked SQL. */
class PostgresMetadataSyncFailureTest {
    private static final long EPOCH = 1;
    private DataSource source;
    // Catalog repository tests exercise completion independently of the workflow provider.
    private void seedObservation(String catalog) throws java.sql.SQLException {
        try (var connection = source.getConnection(); var sql = connection.prepareStatement(
                "UPDATE catalog_definitions SET observation_epoch=? WHERE name=?")) {
            sql.setLong(1, EPOCH);
            sql.setString(2, catalog);
            assertThat(sql.executeUpdate()).isOne();
        }
    }
    private MetadataSyncCompletion completion;

    @BeforeEach
    void initialize() throws Exception {
        source = MigratedPostgres.create();
        var descriptor = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        new PostgresCatalogDeploymentRepository(source).saveCatalogDescriptor(descriptor);
        seedObservation(descriptor.name());
        var now = Instant.parse("2026-01-01T00:00:00Z");
        completion = new MetadataSyncCompletion(UUID.randomUUID(),
                new CatalogDeploymentIdentity(descriptor.name(), descriptor.incarnationId(), descriptor.revision()),
                Optional.empty(), Optional.of(now), new SyncResult("catalog", "INCREMENTAL", now, 1, 0, List.of(), 1));
    }

    @Test
    void lostCommitAcknowledgmentIsConfirmedByImmutableRunReceipt() {
        AtomicInteger closed = new AtomicInteger();
        var faulting = new PostgresMetadataSyncRepository(afterCallFault("commit", new SQLException("lost reply"), closed));
        Throwable failure = catchThrowable(() -> faulting.complete(EPOCH, completion));
        assertThat(failure).isInstanceOf(MetadataSyncOutcomeUnknownException.class);
        assertThat(((MetadataSyncOutcomeUnknownException) failure).runId()).isEqualTo(completion.runId());
        var confirmation = new PostgresMetadataSyncRepository(source);
        assertThat(confirmation.findCompletion(completion.runId())).contains(completion);
        confirmation.complete(EPOCH, completion);
        assertThat(confirmation.loadCheckpoint(completion.scope())).isEqualTo(completion.nextCheckpoint());
        assertThat(closed).hasValue(2);
    }

    @Test
    void closeFailureAfterCommitDoesNotTurnAcceptedCompletionIntoRetryableFailure() {
        AtomicInteger closed = new AtomicInteger();
        var faulting = new PostgresMetadataSyncRepository(afterCallFault("close", new SQLException("close failed"), closed));
        assertThat(catchThrowable(() -> faulting.complete(EPOCH, completion)))
                .isInstanceOf(MetadataSyncOutcomeUnknownException.class);
        assertThat(new PostgresMetadataSyncRepository(source).findCompletion(completion.runId())).contains(completion);
        assertThat(closed).hasValue(2);
    }

    @Test
    void rollbackRuntimeFailureDoesNotReplacePrimaryMutationFailure() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_sync_progress ADD CONSTRAINT reject_progress CHECK (revision < 0)");
        }
        var rollbackFailure = new IllegalStateException("rollback reply failed");
        AtomicInteger closed = new AtomicInteger();
        var faulting = new PostgresMetadataSyncRepository(afterCallFault("rollback", rollbackFailure, closed));
        Throwable failure = catchThrowable(() -> faulting.complete(EPOCH, completion));
        assertThat(failure).isInstanceOf(MetastoreException.class).isNotInstanceOf(MetadataSyncOutcomeUnknownException.class);
        assertThat(failure.getCause()).isInstanceOf(SQLException.class).hasMessageContaining("reject_progress");
        assertThat(failure.getCause().getSuppressed()).containsExactly(rollbackFailure);
        assertThat(new PostgresMetadataSyncRepository(source).findCompletion(completion.runId())).isEmpty();
        assertThat(closed).hasValue(2);
    }

    @Test
    void lostIntentReplyRecoversOriginalRunAndCheckpointInAnotherProvider() {
        var faulting = new PostgresMetadataSyncRepository(
                afterCallFault("commit", new SQLException("lost intent reply"), new AtomicInteger(), 1));
        assertThat(catchThrowable(() -> faulting.complete(EPOCH, completion)))
                .isInstanceOf(MetadataSyncOutcomeUnknownException.class);
        var restarted = new PostgresMetadataSyncRepository(source);
        assertThat(restarted.findCompletion(completion.runId())).isEmpty();
        assertThat(restarted.loadCheckpoint(completion.scope())).isEmpty();
        restarted.recoverPending(completion.scope());
        assertThat(restarted.findCompletion(completion.runId())).contains(completion);
        assertThat(restarted.loadCheckpoint(completion.scope())).isEqualTo(completion.nextCheckpoint());
        restarted.recoverPending(completion.scope());
        restarted.complete(EPOCH, completion);
        assertThat(restarted.findCompletion(completion.runId())).contains(completion);
    }

    @Test
    void pendingCompletionBlocksReplacementRunIdAndChangedContent() {
        var faulting = new PostgresMetadataSyncRepository(
                afterCallFault("commit", new SQLException("lost intent reply"), new AtomicInteger(), 1));
        assertThat(catchThrowable(() -> faulting.complete(EPOCH, completion)))
                .isInstanceOf(MetadataSyncOutcomeUnknownException.class);
        var restarted = new PostgresMetadataSyncRepository(source);
        var differentId = new MetadataSyncCompletion(UUID.randomUUID(), completion.scope(), completion.expectedCheckpoint(),
                completion.nextCheckpoint(), completion.result());
        assertThat(catchThrowable(() -> restarted.complete(EPOCH, differentId)))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetadataSyncPendingException.class);
        var differentContent = new MetadataSyncCompletion(completion.runId(), completion.scope(), completion.expectedCheckpoint(),
                Optional.of(completion.nextCheckpoint().orElseThrow().plusSeconds(1)), completion.result());
        assertThat(catchThrowable(() -> restarted.complete(EPOCH, differentContent)))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetadataSyncConflictException.class);
        assertThat(restarted.findCompletion(differentId.runId())).isEmpty();
        assertThat(restarted.loadCheckpoint(completion.scope())).isEmpty();
        restarted.recoverPending(completion.scope());
        assertThat(restarted.findCompletion(completion.runId())).contains(completion);
    }

    @Test
    void deploymentReplacementRejectsPendingCompletionWithoutMovingEitherCheckpoint() throws Exception {
        var faulting = new PostgresMetadataSyncRepository(
                afterCallFault("commit", new SQLException("lost intent reply"), new AtomicInteger(), 1));
        assertThat(catchThrowable(() -> faulting.complete(EPOCH, completion)))
                .isInstanceOf(MetadataSyncOutcomeUnknownException.class);
        var deployments = new PostgresCatalogDeploymentRepository(source);
        deployments.deleteCatalogDescriptor(completion.scope());
        var replacement = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "replacement", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        deployments.saveCatalogDescriptor(replacement);
        var restarted = new PostgresMetadataSyncRepository(source);
        restarted.recoverPending(replacement.identity());
        assertThat(catchThrowable(() -> restarted.complete(EPOCH, completion)))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetadataSyncConflictException.class);
        assertThat(restarted.findCompletion(completion.runId())).isEmpty();
        assertThat(restarted.loadCheckpoint(completion.scope())).isEmpty();
        assertThat(restarted.loadCheckpoint(replacement.identity())).isEmpty();
        var reuse = new MetadataSyncCompletion(completion.runId(), replacement.identity(), completion.expectedCheckpoint(),
                completion.nextCheckpoint(), completion.result());
        assertThat(catchThrowable(() -> restarted.complete(EPOCH, reuse)))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetadataSyncConflictException.class);
        var accepted = new MetadataSyncCompletion(UUID.randomUUID(), replacement.identity(), completion.expectedCheckpoint(),
                completion.nextCheckpoint(), completion.result());
        seedObservation(replacement.name());
        restarted.complete(EPOCH, accepted);
        assertThat(restarted.findCompletion(accepted.runId())).contains(accepted);
    }

    @Test
    void failedRecoveryRetainsOriginalCompletionUntilBackendFailureIsRepaired() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_sync_progress ADD CONSTRAINT reject_progress CHECK (revision < 0)");
        }
        var restarted = new PostgresMetadataSyncRepository(source);
        assertThat(catchThrowable(() -> restarted.complete(EPOCH, completion)))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetadataSyncPendingException.class);
        assertThat(catchThrowable(() -> restarted.recoverPending(completion.scope())))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetadataSyncPendingException.class);
        assertThat(restarted.findCompletion(completion.runId())).isEmpty();
        assertThat(restarted.loadCheckpoint(completion.scope())).isEmpty();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_sync_progress DROP CONSTRAINT reject_progress");
        }
        restarted.recoverPending(completion.scope());
        assertThat(restarted.findCompletion(completion.runId())).contains(completion);
        assertThat(restarted.loadCheckpoint(completion.scope())).isEqualTo(completion.nextCheckpoint());
    }

    private DataSource afterCallFault(String operation, Throwable failure, AtomicInteger closed) {
        return afterCallFault(operation, failure, closed, operation.equals("rollback") ? 1 : 2);
    }

    private DataSource afterCallFault(String operation, Throwable failure, AtomicInteger closed, int invocation) {
        AtomicInteger calls = new AtomicInteger();
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    try {
                        Object value = method.invoke(source, args);
                        if (!method.getName().equals("getConnection")) return value;
                        Connection delegate = (Connection) value;
                        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                                (connectionProxy, call, parameters) -> {
                                    try {
                                        Object result = call.invoke(delegate, parameters);
                                        if (call.getName().equals("close")) closed.incrementAndGet();
                                        if (call.getName().equals(operation) && calls.incrementAndGet() == invocation) throw failure;
                                        return result;
                                    } catch (InvocationTargetException wrapped) {
                                        throw wrapped.getCause();
                                    }
                                });
                    } catch (InvocationTargetException wrapped) {
                        throw wrapped.getCause();
                    }
                });
    }
}
