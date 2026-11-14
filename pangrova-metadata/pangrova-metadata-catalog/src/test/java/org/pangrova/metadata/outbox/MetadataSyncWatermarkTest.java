package org.pangrova.metadata.outbox;

import org.pangrova.testing.testkit.metadata.CurrentCatalogProjectionFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.metadata.api.catalog.MetadataChangeBatch;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;

/** Exercises the real SQL progress owner through the sync service, not just a setter invocation. */
class MetadataSyncWatermarkTest {
    private static final Instant BOUND = Instant.parse("2026-01-01T00:00:00.123456789Z");
    private MetadataEventDispatcher dispatcher;
    private PostgresMetadataSyncRepository synchronization;
    private PostgresCatalogDeploymentRepository deployments;
    private PostgresTableProjectionRepository tables;
    private PostgresMetadataProjectionTransactions projections;
    private CurrentCatalogProjectionFixture projectionFixture;
    private MetadataSyncService service;
    private CatalogDescriptor deployment;
    private CatalogDeploymentIdentity scope;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = MigratedPostgres.create();
        deployments = new PostgresCatalogDeploymentRepository(dataSource);
        tables = new PostgresTableProjectionRepository(dataSource);
        projections = new PostgresMetadataProjectionTransactions(dataSource);
        projectionFixture = new CurrentCatalogProjectionFixture(deployments, projections);
        synchronization = new PostgresMetadataSyncRepository(dataSource);
        deployment = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "test-source", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        deployments.saveCatalogDescriptor(deployment);
        projectionFixture.saveSchemas("catalog", List.of("schema"));
        scope = new CatalogDeploymentIdentity(deployment.name(), deployment.incarnationId(), deployment.revision());
        dispatcher = new MetadataEventDispatcher();
        service = new MetadataSyncService(tables, projections, synchronization, new TieredMetadataCache(), dispatcher);
    }

    @AfterEach
    void close() {
        if (dispatcher != null) dispatcher.close();
    }

    @Test
    void supersededIncrementalDeploymentCannotWriteBeforeCompletionRejectsIt() {
        var source = new Inventory(new MetadataChangeBatch(List.of(change("stale")), Optional.of(BOUND)));
        source.beforeRead = this::advanceDeployment;

        assertThatThrownBy(() -> service.incrementalSync(deployment, source))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetastoreException.class)
                .hasMessageContaining("deployment");

        assertThat(tables.loadTable("catalog", "schema", "stale")).isEmpty();
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void supersededFullEnumerationCannotPublishTablesBeforeCompletionRejectsIt() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.empty()));
        source.beforeSchemas = this::advanceDeployment;

        assertThatThrownBy(() -> service.fullSync(deployment, source))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetastoreException.class)
                .hasMessageContaining("deployment");

        assertThat(tables.loadTable("catalog", "schema", "one")).isEmpty();
    }

    @Test
    void sourceForOneCatalogCannotDeleteAnotherCatalogProjection() {
        var other = new CatalogDescriptor("other", CatalogType.FEDERATION, "other-source", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        deployments.saveCatalogDescriptor(other);
        projectionFixture.saveSchemas("other", List.of("schema"));
        var existing = new TableMetadata("other", "schema", "protected", List.of(), Map.of(), List.of(),
                Map.of(), BOUND, BOUND);
        projectionFixture.saveTable(existing);
        var storedBefore = tables.loadTable("other", "schema", "protected");
        var deletion = new MetadataChange(MetadataChange.ChangeType.DELETE,
                new QualifiedTableRef("other", "schema", "protected"), null);
        var source = new Inventory(new MetadataChangeBatch(List.of(deletion), Optional.of(BOUND)));

        assertThatThrownBy(() -> service.incrementalSync(deployment, source))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tables.loadTable("other", "schema", "protected")).isEqualTo(storedBefore);
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void malformedIncrementalTargetRejectsWholeBatchBeforeEarlierValidWrites() {
        var mismatched = new MetadataChange(MetadataChange.ChangeType.UPDATE,
                new QualifiedTableRef("catalog", "schema", "expected"), change("wrong-table").metadata());
        var source = new Inventory(new MetadataChangeBatch(List.of(change("valid-first"), mismatched), Optional.of(BOUND)));
        assertThatThrownBy(() -> service.syncWithRetry(deployment, source))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionScopeException.class);
        assertThat(source.requested).containsExactly(Instant.EPOCH);
        assertThat(tables.loadTable("catalog", "schema", "valid-first")).isEmpty();
        assertThat(tables.loadTable("catalog", "schema", "wrong-table")).isEmpty();
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void pushForeignTargetIsRejectedBeforeFetchingSource() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.empty()));
        source.beforeFetch = () -> { throw new AssertionError("Foreign target must not be fetched"); };
        service.subscribeToPushNotifications(deployment, source);
        assertThatThrownBy(() -> source.listener.onChange(new MetadataChange(MetadataChange.ChangeType.DELETE,
                new QualifiedTableRef("other", "schema", "one"), null)))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionScopeException.class);
    }

    private void advanceDeployment() {
        deployments.saveCatalogDescriptor(new CatalogDescriptor(deployment.name(), deployment.type(),
                deployment.provider(), deployment.state(), deployment.properties(), deployment.comment(),
                deployment.allowedTenants(), deployment.incarnationId(), deployment.revision() + 1));
    }

    @Test
    void certifiedEmptyBatchAdvancesToSourceBoundary() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.of(BOUND)));
        assertThat(service.incrementalSync(deployment, source).changesApplied()).isZero();
        assertThat(synchronization.loadCheckpoint(scope)).contains(BOUND);
        service.incrementalSync(deployment, source);
        assertThat(source.requested).containsExactly(Instant.EPOCH, BOUND);
    }

    @Test
    void appliedUnboundedChangesDoNotUseLocalCompletionTime() {
        var source = new Inventory(new MetadataChangeBatch(List.of(change("one")), Optional.empty()));
        assertThat(service.incrementalSync(deployment, source).changesApplied()).isEqualTo(1);
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
        service.incrementalSync(deployment, source);
        assertThat(source.requested).containsExactly(Instant.EPOCH, Instant.EPOCH);
    }

    @Test
    void partialFailureRetainsCheckpointAndReplaysFailedRange() {
        var invalid = new MetadataChange(MetadataChange.ChangeType.ADD,
                new QualifiedTableRef("catalog", "schema", "broken"), null);
        var source = new Inventory(new MetadataChangeBatch(List.of(change("one"), invalid), Optional.of(BOUND)));
        assertThat(service.incrementalSync(deployment, source).errors()).isEqualTo(1);
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
        source.batch = new MetadataChangeBatch(List.of(change("one"), change("broken")), Optional.of(BOUND));
        assertThat(service.incrementalSync(deployment, source).errors()).isZero();
        assertThat(source.requested).containsExactly(Instant.EPOCH, Instant.EPOCH);
        assertThat(synchronization.loadCheckpoint(scope)).contains(BOUND);
        assertThat(tables.loadTable("catalog", "schema", "broken")).isPresent();
    }

    @Test
    void fullEnumerationDoesNotInventChangeLogBoundary() {
        var source = new Inventory(new MetadataChangeBatch(List.of(change("one")), Optional.of(BOUND)));
        assertThat(service.fullSync(deployment, source).changesApplied()).isEqualTo(1);
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void backwardsSourceBoundaryIsRejectedBeforeWritingProjection() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.of(BOUND)));
        service.incrementalSync(deployment, source);
        source.batch = new MetadataChangeBatch(List.of(change("late")), Optional.of(BOUND.minusSeconds(1)));
        assertThatThrownBy(() -> service.incrementalSync(deployment, source)).isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.loadTable("catalog", "schema", "late")).isEmpty();
        assertThat(synchronization.loadCheckpoint(scope)).contains(BOUND);
    }

    @Test
    void uncertainCompletionIsConfirmedWithoutRereadingOrReapplyingSource() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.of(BOUND)));
        var confirming = new MetadataSyncService(tables, projections, uncertainRepository(true), new TieredMetadataCache(), dispatcher);
        assertThat(confirming.syncWithRetry(deployment, source).errors()).isZero();
        assertThat(source.requested).containsExactly(Instant.EPOCH);
        assertThat(synchronization.loadCheckpoint(scope)).contains(BOUND);
    }

    @Test
    void unconfirmedCompletionDoesNotTriggerIncrementalRetryOrFullSync() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.of(BOUND)));
        var uncertain = new MetadataSyncService(tables, projections, uncertainRepository(false), new TieredMetadataCache(), dispatcher);
        assertThatThrownBy(() -> uncertain.syncWithRetry(deployment, source))
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetadataSyncOutcomeUnknownException.class);
        assertThat(source.requested).containsExactly(Instant.EPOCH);
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void uncertainProjectionCommitIsConfirmedWithoutReapplyingOrRereading() throws Exception {
        var source = new Inventory(new MetadataChangeBatch(List.of(change("one")), Optional.of(BOUND)));
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        var uncertain = new MetadataSyncService(tables, uncertainProjections(true, false, writes), synchronization,
                new TieredMetadataCache(), dispatcher);
        assertThat(uncertain.syncWithRetry(deployment, source).errors()).isZero();
        assertThat(writes).hasValue(1);
        assertThat(source.requested).containsExactly(Instant.EPOCH);
        assertThat(tables.loadTable("catalog", "schema", "one")).isPresent();
        assertThat(synchronization.loadCheckpoint(scope)).contains(BOUND);
    }

    @Test
    void unconfirmedProjectionNeverFallsBackOrAdvancesCheckpoint() {
        var source = new Inventory(new MetadataChangeBatch(List.of(change("one")), Optional.of(BOUND)));
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        var uncertain = new MetadataSyncService(tables, uncertainProjections(false, false, writes), synchronization,
                new TieredMetadataCache(), dispatcher);
        assertThatThrownBy(() -> uncertain.syncWithRetry(deployment, source))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionOutcomeUnknownException.class);
        assertThat(writes).hasValue(1);
        assertThat(source.requested).containsExactly(Instant.EPOCH);
        assertThat(tables.loadTable("catalog", "schema", "one")).isEmpty();
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void projectionConfirmationFailurePreservesUnknownOutcomeAndNeverRetries() {
        var source = new Inventory(new MetadataChangeBatch(List.of(change("one")), Optional.of(BOUND)));
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        var uncertain = new MetadataSyncService(tables, uncertainProjections(true, true, writes), synchronization,
                new TieredMetadataCache(), dispatcher);
        assertThatThrownBy(() -> uncertain.syncWithRetry(deployment, source))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionOutcomeUnknownException.class)
                .satisfies(failure -> assertThat(failure.getSuppressed()).singleElement().satisfies(suppressed -> assertThat(suppressed).hasMessage("confirmation unavailable")));
        assertThat(writes).hasValue(1);
        assertThat(source.requested).containsExactly(Instant.EPOCH);
        assertThat(tables.loadTable("catalog", "schema", "one")).isPresent();
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void pushDeleteDoesNotFetchAlreadyDeletedSourceTable() {
        projectionFixture.saveTable(change("one").metadata());
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.empty()));
        source.beforeFetch = () -> { throw new AssertionError("Deleted source table must not be fetched"); };
        service.subscribeToPushNotifications(deployment, source);
        source.listener.onChange(new MetadataChange(MetadataChange.ChangeType.DELETE,
                new QualifiedTableRef("catalog", "schema", "one"), null));
        assertThat(tables.loadTable("catalog", "schema", "one")).isEmpty();
    }

    @Test
    void pushSubscriptionRemainsBoundToOriginalDeployment() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.empty()));
        service.subscribeToPushNotifications(deployment, source);
        advanceDeployment();
        assertThatThrownBy(() -> source.listener.onChange(change("one")))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        assertThat(tables.loadTable("catalog", "schema", "one")).isEmpty();
    }

    @Test
    void driftRepairCannotPublishAfterSourceReadSupersedesDeployment() {
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.empty()));
        source.beforeFetch = this::advanceDeployment;
        assertThatThrownBy(() -> service.validateSingleTable(scope, source, change("one")))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        assertThat(tables.loadTable("catalog", "schema", "one")).isEmpty();
        assertThat(service.getSchemaDriftDetectedCount()).isZero();
    }

    private org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions uncertainProjections(
            boolean committed, boolean confirmationFails, java.util.concurrent.atomic.AtomicInteger writes) {
        return new org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions() {
            @Override public void recoverPending(CatalogDeploymentIdentity requestedScope) {
                projections.recoverPending(requestedScope);
            }
            @Override public org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt apply(
                    org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation mutation) {
                writes.incrementAndGet();
                if (committed) projections.apply(mutation);
                throw new org.pangrova.persistence.workflow.api.exception.MetadataProjectionOutcomeUnknownException(
                        mutation.operationId(), new java.sql.SQLException("lost acknowledgment"));
            }
            @Override public Optional<org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt> findReceipt(java.util.UUID id) {
                if (confirmationFails) throw new IllegalStateException("confirmation unavailable");
                return projections.findReceipt(id);
            }
        };
    }

    private MetadataSyncRepository uncertainRepository(boolean committed) {
        return new MetadataSyncRepository() {
            @Override public void recoverPending(CatalogDeploymentIdentity requestedScope) {
                synchronization.recoverPending(requestedScope);
            }
            @Override public Optional<Instant> loadCheckpoint(CatalogDeploymentIdentity requestedScope) {
                return synchronization.loadCheckpoint(requestedScope);
            }
            @Override public void complete(org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion completion) {
                if (committed) synchronization.complete(completion);
                throw new org.pangrova.persistence.catalog.api.exception.MetadataSyncOutcomeUnknownException(
                        completion.runId(), new java.sql.SQLException("lost acknowledgment"));
            }
            @Override public Optional<org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion>
                    findCompletion(java.util.UUID id) {
                return synchronization.findCompletion(id);
            }
        };
    }

    private static MetadataChange change(String table) {
        var metadata = new TableMetadata("catalog", "schema", table, List.of(), Map.of(), List.of(), Map.of(), BOUND, BOUND);
        return new MetadataChange(MetadataChange.ChangeType.ADD, new QualifiedTableRef("catalog", "schema", table), metadata);
    }

    private static final class Inventory implements FederatedCatalog {
        private MetadataChangeBatch batch;
        private final List<Instant> requested = new ArrayList<>();
        private Runnable beforeRead = () -> {};
        private Runnable beforeSchemas = () -> {};
        private Runnable beforeFetch = () -> {};
        private org.pangrova.metadata.api.catalog.ChangeListener listener;
        Inventory(MetadataChangeBatch batch) { this.batch = batch; }
        @Override public List<String> listSchemas() { beforeSchemas.run(); return List.of("schema"); }
        @Override public List<String> listTables(String schema) { return List.of("one"); }
        @Override public TableMetadata getTable(String schema, String table) { beforeFetch.run(); return change(table).metadata(); }
        @Override public TableStatistics getTableStatistics(String schema, String table) { return TableStatistics.UNKNOWN; }
        @Override public MetadataChangeBatch readChangesSince(Instant since) {
            requested.add(since);
            beforeRead.run();
            return batch;
        }
        @Override public void subscribeToChanges(org.pangrova.metadata.api.catalog.ChangeListener callback) { listener = callback; }
        @Override public void close() {}
    }
}
