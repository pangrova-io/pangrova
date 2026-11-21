package org.pangrova.persistence.workflow.postgresql.projection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionOutcomeUnknownException;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresSchemaProjectionRepository;

/** Faults wrap public JDBC calls on a real database; no private-state reflection or mocked SQL. */
class PostgresMetadataProjectionFailureTest {
    private DataSource source;
    private long epoch;
    private MetadataProjectionMutation.UpsertTable mutation;

    @BeforeEach
    void initialize() throws Exception {
        source = MigratedPostgres.create();
        var descriptor = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        new PostgresCatalogDeploymentRepository(source).saveCatalogDescriptor(descriptor);
        var transactions = new PostgresMetadataProjectionTransactions(source);
        epoch = transactions.beginObservation(descriptor.identity());
        transactions.apply(epoch, new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), descriptor.identity(), List.of("sch")));
        mutation = new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), descriptor.identity(),
                new org.pangrova.metadata.api.object.TableMetadata("catalog", "sch", "tbl", List.of(), Map.of(), List.of(), Map.of(),
                        Instant.EPOCH, Instant.EPOCH));
    }

    @Test
    void lostCommitAcknowledgmentIsConfirmedByImmutableOperationReceipt() {
        AtomicInteger closed = new AtomicInteger();
        var faulting = new PostgresMetadataProjectionTransactions(afterCallFault("commit", new SQLException("lost reply"), closed));
        Throwable failure = catchThrowable(() -> faulting.apply(epoch, mutation));
        assertThat(failure).isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        assertThat(((MetadataProjectionOutcomeUnknownException) failure).operationId()).isEqualTo(mutation.operationId());
        var confirmation = new PostgresMetadataProjectionTransactions(source);
        assertThat(confirmation.findReceipt(mutation.operationId()).orElseThrow().mutation()).isEqualTo(mutation);
        confirmation.apply(epoch, mutation);
        assertThat(new PostgresTableProjectionRepository(source).loadTable("catalog", "sch", "tbl")).contains(mutation.table());
        assertThat(closed).hasValue(2);
    }

    @Test
    void closeFailureAfterCommitDoesNotTurnAcceptedMutationIntoRetryableFailure() {
        AtomicInteger closed = new AtomicInteger();
        var faulting = new PostgresMetadataProjectionTransactions(afterCallFault("close", new SQLException("close failed"), closed));
        assertThat(catchThrowable(() -> faulting.apply(epoch, mutation)))
                .isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        assertThat(new PostgresMetadataProjectionTransactions(source).findReceipt(mutation.operationId()).orElseThrow().mutation()).isEqualTo(mutation);
        assertThat(closed).hasValue(2);
    }

    @Test
    void rollbackRuntimeFailureDoesNotReplacePrimaryMutationFailure() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_projection_mutations ADD CONSTRAINT reject_progress CHECK (revision < 0) NOT VALID");
        }
        var rollbackFailure = new IllegalStateException("rollback reply failed");
        AtomicInteger closed = new AtomicInteger();
        var faulting = new PostgresMetadataProjectionTransactions(afterCallFault("rollback", rollbackFailure, closed));
        Throwable failure = catchThrowable(() -> faulting.apply(epoch, mutation));
        assertThat(failure).isInstanceOf(MetastoreException.class).isNotInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        assertThat(failure.getCause()).isInstanceOf(SQLException.class).hasMessageContaining("reject_progress");
        assertThat(failure.getCause().getSuppressed()).containsExactly(rollbackFailure);
        assertThat(new PostgresMetadataProjectionTransactions(source).findReceipt(mutation.operationId())).isEmpty();
        assertThat(closed).hasValue(2);
    }

    @Test
    void lostIntentAcknowledgmentIsRecoveredByAnotherProviderWithoutOriginalCallerState() {
        var faulting = new PostgresMetadataProjectionTransactions(
                afterCallFault("commit", new SQLException("lost intent reply"), new AtomicInteger(), 1));
        assertThat(catchThrowable(() -> faulting.apply(epoch, mutation)))
                .isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        var restarted = new PostgresMetadataProjectionTransactions(source);
        assertThat(restarted.findReceipt(mutation.operationId())).isEmpty();
        assertThat(new PostgresTableProjectionRepository(source).loadTable("catalog", "sch", "tbl")).isEmpty();
        restarted.recoverPending(mutation.deployment());
        var receipt = restarted.findReceipt(mutation.operationId()).orElseThrow();
        assertThat(receipt.mutation()).isEqualTo(mutation);
        assertThat(new PostgresTableProjectionRepository(source).loadTable("catalog", "sch", "tbl")).contains(mutation.table());
        restarted.recoverPending(mutation.deployment());
        assertThat(restarted.apply(epoch, mutation)).isEqualTo(receipt);
    }

    @Test
    void pendingOperationCannotBeBypassedWithAnotherIdOrDifferentPayload() {
        var faulting = new PostgresMetadataProjectionTransactions(
                afterCallFault("commit", new SQLException("lost intent reply"), new AtomicInteger(), 1));
        assertThat(catchThrowable(() -> faulting.apply(epoch, mutation)))
                .isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        var restarted = new PostgresMetadataProjectionTransactions(source);
        var differentId = new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), mutation.deployment(), mutation.table());
        assertThat(catchThrowable(() -> restarted.apply(epoch, differentId)))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException.class);
        var differentPayload = new MetadataProjectionMutation.ObserveSchemas(mutation.operationId(), mutation.deployment(), List.of("other"));
        assertThat(catchThrowable(() -> restarted.apply(epoch, differentPayload)))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        assertThat(restarted.findReceipt(mutation.operationId())).isEmpty();
        assertThat(restarted.findReceipt(differentId.operationId())).isEmpty();
        restarted.recoverPending(mutation.deployment());
        assertThat(restarted.findReceipt(mutation.operationId()).orElseThrow().mutation()).isEqualTo(mutation);
    }

    @Test
    void replacementRejectsOldIntentAndLateCallerCannotWriteIntoNewIncarnation() {
        var faulting = new PostgresMetadataProjectionTransactions(
                afterCallFault("commit", new SQLException("lost intent reply"), new AtomicInteger(), 1));
        assertThat(catchThrowable(() -> faulting.apply(epoch, mutation)))
                .isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        var deployments = new PostgresCatalogDeploymentRepository(source);
        deployments.deleteCatalogDescriptor(mutation.deployment());
        var replacement = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "replacement", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        deployments.saveCatalogDescriptor(replacement);
        var restarted = new PostgresMetadataProjectionTransactions(source);
        restarted.recoverPending(replacement.identity());
        assertThat(catchThrowable(() -> restarted.apply(epoch, mutation)))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        assertThat(catchThrowable(() -> restarted.apply(epoch, new MetadataProjectionMutation.ObserveSchemas(
                mutation.operationId(), replacement.identity(), List.of("reused-id")))))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        restarted.apply(restarted.beginObservation(replacement.identity()), new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), replacement.identity(), List.of("new")));
        assertThat(new PostgresSchemaProjectionRepository(source).loadSchemas("catalog")).containsExactly("new");
        assertThat(new PostgresTableProjectionRepository(source).loadTable("catalog", "sch", "tbl")).isEmpty();
        assertThat(restarted.findReceipt(mutation.operationId())).isEmpty();
    }

    @Test
    void concurrentRecoveryAndOriginalRetryPublishOneEffect() throws Exception {
        var faulting = new PostgresMetadataProjectionTransactions(
                afterCallFault("commit", new SQLException("lost intent reply"), new AtomicInteger(), 1));
        assertThat(catchThrowable(() -> faulting.apply(epoch, mutation)))
                .isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        long before = eventCount();
        var executor = java.util.concurrent.Executors.newFixedThreadPool(3);
        var start = new java.util.concurrent.CountDownLatch(1);
        try {
            var first = executor.submit(() -> { start.await(); new PostgresMetadataProjectionTransactions(source).recoverPending(mutation.deployment()); return null; });
            var second = executor.submit(() -> { start.await(); new PostgresMetadataProjectionTransactions(source).recoverPending(mutation.deployment()); return null; });
            var retry = executor.submit(() -> { start.await(); return new PostgresMetadataProjectionTransactions(source).apply(epoch, mutation); });
            start.countDown();
            first.get(10, java.util.concurrent.TimeUnit.SECONDS);
            second.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(retry.get(10, java.util.concurrent.TimeUnit.SECONDS).mutation()).isEqualTo(mutation);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
        assertThat(eventCount()).isEqualTo(before + 1);
    }

    @Test
    void failedRecoveryRetainsOriginalIntentAndNeverAcceptsNewWork() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_projection_mutations ADD CONSTRAINT reject_progress CHECK (revision < 0) NOT VALID");
        }
        var restarted = new PostgresMetadataProjectionTransactions(source);
        assertThat(catchThrowable(() -> restarted.apply(epoch, mutation)))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException.class);
        assertThat(catchThrowable(() -> restarted.beginObservation(mutation.deployment())))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        assertThat(catchThrowable(() -> restarted.recoverPending(mutation.deployment())))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException.class);
        assertThat(catchThrowable(() -> restarted.apply(epoch, new MetadataProjectionMutation.UpsertTable(
                UUID.randomUUID(), mutation.deployment(), mutation.table()))))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException.class);
        assertThat(restarted.findReceipt(mutation.operationId())).isEmpty();
        assertThat(new PostgresTableProjectionRepository(source).loadTable("catalog", "sch", "tbl")).isEmpty();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_projection_mutations DROP CONSTRAINT reject_progress");
        }
        restarted.recoverPending(mutation.deployment());
        assertThat(restarted.findReceipt(mutation.operationId()).orElseThrow().mutation()).isEqualTo(mutation);
    }

    private long eventCount() throws SQLException {
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM invalidation_log")) {
            rows.next();
            return rows.getLong(1);
        }
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
