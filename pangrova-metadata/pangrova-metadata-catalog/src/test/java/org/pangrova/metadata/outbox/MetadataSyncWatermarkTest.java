package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.enums.MetadataChangeType;
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
    private javax.sql.DataSource database;
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
        database = MigratedPostgres.create();
        var dataSource = database;
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
        var deletion = new MetadataChange(MetadataChangeType.DELETE,
                new QualifiedTableRef("other", "schema", "protected"), null);
        var source = new Inventory(new MetadataChangeBatch(List.of(deletion), Optional.of(BOUND)));

        assertThatThrownBy(() -> service.incrementalSync(deployment, source))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tables.loadTable("other", "schema", "protected")).isEqualTo(storedBefore);
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    @Test
    void malformedIncrementalTargetRejectsWholeBatchBeforeEarlierValidWrites() {
        var mismatched = new MetadataChange(MetadataChangeType.UPDATE,
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
        assertThatThrownBy(() -> source.listener.onChange(new MetadataChange(MetadataChangeType.DELETE,
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
        var invalid = new MetadataChange(MetadataChangeType.ADD,
                new QualifiedTableRef("catalog", "schema", "broken"), null);
        var source = new Inventory(new MetadataChangeBatch(List.of(change("one"), invalid), Optional.of(BOUND)));
        source.unavailable = "broken";
        assertThat(service.incrementalSync(deployment, source).errors()).isEqualTo(1);
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
        source.unavailable = null;
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
    void pushDeleteRequiresAuthoritativeAbsence() {
        projectionFixture.saveTable(change("one").metadata());
        var source = new Inventory(new MetadataChangeBatch(List.of(), Optional.empty()));
        source.absent = true;
        service.subscribeToPushNotifications(deployment, source);
        source.listener.onChange(new MetadataChange(MetadataChangeType.DELETE,
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
        assertThatThrownBy(() -> service.validateSingleTable(scope, service.beginObservation(scope), source, change("one")))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        assertThat(tables.loadTable("catalog", "schema", "one")).isEmpty();
        assertThat(service.getSchemaDriftDetectedCount()).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"FULL", "INCREMENTAL", "PUSH", "SAMPLE", "EMPTY"})
    void laterCoordinatorFencesPausedObservationOnSameDeployment(String kind) throws Exception {
        var captured = new java.util.concurrent.CountDownLatch(1);
        var resume = new java.util.concurrent.CountDownLatch(1);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        Runnable pause = () -> {
            captured.countDown();
            try {
                if (!resume.await(20, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("No resume");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        };
        var hint = change("one");
        var slow = new Inventory(new MetadataChangeBatch(kind.equals("EMPTY") ? List.of() : List.of(hint),
                Optional.of(BOUND)));
        var stale = new TableMetadata("catalog", "schema", "one",
                List.of(new org.pangrova.metadata.api.object.ColumnMetadata("obsolete",
                        org.pangrova.metadata.api.object.ColumnType.of("BIGINT"), true, null, null)),
                Map.of(), List.of(), Map.of(), BOUND, BOUND);
        slow.pointResult = Optional.of(stale);
        if (kind.equals("EMPTY")) slow.beforeRead = pause;
        else slow.beforeFetch = pause;
        try (var peerEvents = new MetadataEventDispatcher()) {
            var peer = new MetadataSyncService(new PostgresTableProjectionRepository(database),
                    new PostgresMetadataProjectionTransactions(database), new PostgresMetadataSyncRepository(database),
                    new TieredMetadataCache(), peerEvents);
            var first = executor.submit(() -> {
                switch (kind) {
                    case "FULL" -> service.fullSync(deployment, slow);
                    case "INCREMENTAL", "EMPTY" -> service.incrementalSync(deployment, slow);
                    case "SAMPLE" -> service.validateSingleTable(scope, service.beginObservation(scope), slow, hint);
                    case "PUSH" -> {
                        service.subscribeToPushNotifications(deployment, slow);
                        slow.listener.onChange(hint);
                    }
                    default -> throw new AssertionError(kind);
                }
            });
            assertThat(captured.await(20, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var current = new Inventory(new MetadataChangeBatch(List.of(hint), Optional.empty()));
            assertThat(peer.incrementalSync(deployment, current).errors()).isZero();
            var afterNewer = publicationCounts();
            resume.countDown();
            assertThatThrownBy(() -> first.get(20, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(kind.equals("EMPTY")
                            ? org.pangrova.persistence.catalog.api.exception.MetadataSyncConflictException.class
                            : org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
            assertThat(publicationCounts()).isEqualTo(afterNewer);
            assertThat(tables.loadTable("catalog", "schema", "one")).contains(hint.metadata());
            assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
        } finally {
            resume.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(20, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void delayedDeleteRechecksRecreatedTableAndLookupFailurePreservesIt() throws Exception {
        var hint = new MetadataChange(MetadataChangeType.DELETE,
                new QualifiedTableRef("catalog", "schema", "one"), null);
        var source = new Inventory(new MetadataChangeBatch(List.of(hint), Optional.of(BOUND)));
        var prior = new TableMetadata("catalog", "schema", "one", List.of(), Map.of(), List.of(),
                Map.of("incarnation", "previous"), Instant.EPOCH, Instant.EPOCH);
        projectionFixture.saveTable(prior);
        service.subscribeToPushNotifications(deployment, source);
        source.listener.onChange(hint);
        assertThat(tables.loadTable("catalog", "schema", "one")).contains(change("one").metadata());
        var afterRecreate = publicationCounts();
        source.unavailable = "one";
        assertThatThrownBy(() -> source.listener.onChange(hint)).hasMessage("Source lookup unavailable");
        assertThat(publicationCounts()).isEqualTo(afterRecreate);
        assertThat(service.incrementalSync(deployment, source).errors()).isOne();
        assertThat(tables.loadTable("catalog", "schema", "one")).contains(change("one").metadata());
        assertThat(synchronization.loadCheckpoint(scope)).isEmpty();
    }

    private List<Long> publicationCounts() throws java.sql.SQLException {
        try (var connection = database.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT (SELECT count(*) FROM metadata_projection_mutations), "
                        + "(SELECT count(*) FROM invalidation_log), (SELECT count(*) FROM metadata_sync_runs), "
                        + "(SELECT count(*) FROM metadata_projection_intents), (SELECT count(*) FROM metadata_sync_intents)")) {
            assertThat(rows.next()).isTrue();
            return List.of(rows.getLong(1), rows.getLong(2), rows.getLong(3), rows.getLong(4), rows.getLong(5));
        }
    }

    private org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions uncertainProjections(
            boolean committed, boolean confirmationFails, java.util.concurrent.atomic.AtomicInteger writes) {
        return new org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions() {
            @Override public long beginObservation(CatalogDeploymentIdentity requestedScope) {
                return projections.beginObservation(requestedScope);
            }
            @Override public void recoverPending(CatalogDeploymentIdentity requestedScope) {
                projections.recoverPending(requestedScope);
            }
            @Override public org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt apply(
                    long epoch, org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation mutation) {
                writes.incrementAndGet();
                if (committed) projections.apply(epoch, mutation);
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
            @Override public void complete(long epoch, org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion completion) {
                if (committed) synchronization.complete(epoch, completion);
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
        return new MetadataChange(MetadataChangeType.ADD, new QualifiedTableRef("catalog", "schema", table), metadata);
    }

    private static final class Inventory implements FederatedCatalog {
        private MetadataChangeBatch batch;
        private final List<Instant> requested = new ArrayList<>();
        private Runnable beforeRead = () -> {};
        private Runnable beforeSchemas = () -> {};
        private Optional<TableMetadata> pointResult;
        private boolean absent;
        private String unavailable;
        private Runnable beforeFetch = () -> {};
        private org.pangrova.metadata.api.catalog.ChangeListener listener;
        Inventory(MetadataChangeBatch batch) { this.batch = batch; }
        @Override public List<String> listSchemas() { beforeSchemas.run(); return List.of("schema"); }
        @Override public List<String> listTables(String schema) { return List.of("one"); }
        @Override public Optional<TableMetadata> observeTable(String schema, String table) {
            if (table.equals(unavailable)) throw new IllegalStateException("Source lookup unavailable");
            if (pointResult != null) {
                beforeFetch.run();
                return pointResult;
            }
            return absent ? Optional.empty() : Optional.of(getTable(schema, table));
        }
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
