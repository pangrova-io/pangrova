package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.enums.MetadataChangeType;

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
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionOutcomeUnknownException;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncOutcomeUnknownException;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncPendingException;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Production sync entry point with independently recreated providers and real persistent recovery. */
class MetadataSyncRecoveryIntegrationTest {
    private static final Instant BOUND = Instant.parse("2026-01-01T00:00:00.123456789Z");
    private DataSource database;
    private CatalogDescriptor deployment;
    private PostgresTableProjectionRepository tables;
    private MetadataEventDispatcher events;
    private Inventory inventory;

    @BeforeEach void initialize() throws Exception {
        database = MigratedPostgres.create();
        deployment = new CatalogDescriptor("catalog", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        new PostgresCatalogDeploymentRepository(database).saveCatalogDescriptor(deployment);
        var projections = new PostgresMetadataProjectionTransactions(database);
        projections.apply(projections.beginObservation(deployment.identity()),
                new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), deployment.identity(), List.of("schema")));
        tables = new PostgresTableProjectionRepository(database);
        events = new MetadataEventDispatcher();
        inventory = new Inventory();
    }

    @AfterEach void close() { events.close(); }

    @Test void nextRoundRecoversProjectionBeforeReadingSourceAgain() {
        var first = service(lostReplyAfterInsertCommit("metadata_projection_intents"), database);
        var failure = catchThrowable(() -> first.syncWithRetry(deployment, inventory));
        assertThat(failure).isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        UUID originalId = ((MetadataProjectionOutcomeUnknownException) failure).operationId();
        assertThat(tables.loadTable("catalog", "schema", "table")).isEmpty();
        assertThat(inventory.requested).containsExactly(Instant.EPOCH);

        inventory.batch = new MetadataChangeBatch(List.of(), Optional.of(BOUND));
        inventory.beforeRead = () -> assertThat(tables.loadTable("catalog", "schema", "table")).isPresent();
        var restarted = service(database, database);
        assertThat(restarted.syncWithRetry(deployment, inventory).errors()).isZero();
        assertThat(new PostgresMetadataProjectionTransactions(database).findReceipt(originalId)).isPresent();
        assertThat(inventory.requested).containsExactly(Instant.EPOCH, Instant.EPOCH);
        assertThat(new PostgresMetadataSyncRepository(database).loadCheckpoint(deployment.identity())).contains(BOUND);
    }

    @Test void nextRoundRecoversOriginalCompletionBeforeChoosingSourceCursor() {
        var first = service(database, lostReplyAfterInsertCommit("metadata_sync_intents"));
        var failure = catchThrowable(() -> first.syncWithRetry(deployment, inventory));
        assertThat(failure).isInstanceOf(MetadataSyncOutcomeUnknownException.class);
        UUID originalRun = ((MetadataSyncOutcomeUnknownException) failure).runId();
        assertThat(new PostgresMetadataSyncRepository(database).loadCheckpoint(deployment.identity())).isEmpty();
        inventory.batch = new MetadataChangeBatch(List.of(), Optional.of(BOUND.plusSeconds(1)));
        var restarted = service(database, database);
        assertThat(restarted.syncWithRetry(deployment, inventory).errors()).isZero();
        assertThat(inventory.requested).containsExactly(Instant.EPOCH, BOUND);
        var recovered = new PostgresMetadataSyncRepository(database).findCompletion(originalRun).orElseThrow();
        assertThat(recovered.nextCheckpoint()).contains(BOUND);
        assertThat(recovered.result().changesApplied()).isEqualTo(1);
    }

    @Test void unresolvedProjectionBlocksIncrementalRetryAndFullEnumerationAcrossRounds() throws Exception {
        reject("metadata_projection_mutations", "reject_receipt");
        assertThat(catchThrowable(() -> service(database, database).syncWithRetry(deployment, inventory)))
                .isInstanceOf(MetadataProjectionPendingException.class);
        assertThat(inventory.requested).containsExactly(Instant.EPOCH);
        assertThat(catchThrowable(() -> service(database, database).syncWithRetry(deployment, inventory)))
                .isInstanceOf(MetadataProjectionPendingException.class);
        assertThat(catchThrowable(() -> service(database, database).fullSync(deployment, inventory)))
                .isInstanceOf(MetadataProjectionPendingException.class);
        assertThat(inventory.requested).containsExactly(Instant.EPOCH);
        assertThat(inventory.schemaReads).isZero();
        assertThat(tables.loadTable("catalog", "schema", "table")).isEmpty();
    }

    @Test void unresolvedCompletionBlocksNewSourceReadsAndNewRunIdsAcrossRounds() throws Exception {
        reject("metadata_sync_progress", "reject_progress");
        assertThat(catchThrowable(() -> service(database, database).syncWithRetry(deployment, inventory)))
                .isInstanceOf(MetadataSyncPendingException.class);
        assertThat(inventory.requested).containsExactly(Instant.EPOCH);
        assertThat(catchThrowable(() -> new PostgresMetadataProjectionTransactions(database).beginObservation(deployment.identity())))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException.class);
        assertThat(catchThrowable(() -> service(database, database).syncWithRetry(deployment, inventory)))
                .isInstanceOf(MetadataSyncPendingException.class);
        assertThat(catchThrowable(() -> service(database, database).fullSync(deployment, inventory)))
                .isInstanceOf(MetadataSyncPendingException.class);
        assertThat(inventory.requested).containsExactly(Instant.EPOCH);
        assertThat(inventory.schemaReads).isZero();
        assertThat(new PostgresMetadataSyncRepository(database).loadCheckpoint(deployment.identity())).isEmpty();
    }

    @Test void unknownRecoveryCommitPreventsSourceObservationUntilNextConfirmation() {
        var first = service(lostReplyAfterInsertCommit("metadata_projection_intents"), database);
        assertThat(catchThrowable(() -> first.syncWithRetry(deployment, inventory)))
                .isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        var recovering = service(lostReplyAfterInsertCommit("metadata_projection_mutations"), database);
        assertThat(catchThrowable(() -> recovering.syncWithRetry(deployment, inventory)))
                .isInstanceOf(MetadataProjectionOutcomeUnknownException.class);
        assertThat(inventory.requested).containsExactly(Instant.EPOCH);
        assertThat(tables.loadTable("catalog", "schema", "table")).isPresent();
        inventory.batch = new MetadataChangeBatch(List.of(), Optional.of(BOUND));
        assertThat(service(database, database).syncWithRetry(deployment, inventory).errors()).isZero();
        assertThat(inventory.requested).containsExactly(Instant.EPOCH, Instant.EPOCH);
    }

    private MetadataSyncService service(DataSource projectionSource, DataSource syncSource) {
        return new MetadataSyncService(tables, new PostgresMetadataProjectionTransactions(projectionSource),
                new PostgresMetadataSyncRepository(syncSource), new TieredMetadataCache(), events);
    }

    private void reject(String table, String constraint) throws SQLException {
        try (var connection = database.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE " + table + " ADD CONSTRAINT " + constraint + " CHECK (revision < 0) NOT VALID");
        }
    }

    /** Public JDBC seam loses only the acknowledgement after a real database commit. */
    private DataSource lostReplyAfterInsertCommit(String table) {
        AtomicBoolean injected = new AtomicBoolean();
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    try {
                        Object value = method.invoke(database, args);
                        if (!method.getName().equals("getConnection")) return value;
                        Connection delegate = (Connection) value;
                        AtomicBoolean wrote = new AtomicBoolean();
                        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                                (connectionProxy, call, parameters) -> {
                                    try {
                                        if (call.getName().equals("prepareStatement")
                                                && parameters[0].toString().startsWith("INSERT INTO " + table + "(")) {
                                            wrote.set(true);
                                        }
                                        Object result = call.invoke(delegate, parameters);
                                        if (call.getName().equals("commit") && wrote.get() && injected.compareAndSet(false, true)) {
                                            throw new SQLException("lost durable acknowledgement");
                                        }
                                        return result;
                                    } catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                                });
                    } catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                });
    }

    private static TableMetadata table() {
        return new TableMetadata("catalog", "schema", "table", List.of(), Map.of(), List.of(), Map.of(), BOUND, BOUND);
    }

    private static final class Inventory implements FederatedCatalog {
        private MetadataChangeBatch batch = new MetadataChangeBatch(List.of(new MetadataChange(
                MetadataChangeType.ADD, new QualifiedTableRef("catalog", "schema", "table"), table())), Optional.of(BOUND));
        private final List<Instant> requested = new ArrayList<>();
        private Runnable beforeRead = () -> {};
        private int schemaReads;
        @Override public MetadataChangeBatch readChangesSince(Instant since) {
            beforeRead.run();
            requested.add(since);
            return batch;
        }
        @Override public List<String> listSchemas() { schemaReads++; return List.of("schema"); }
        @Override public List<String> listTables(String schema) { return List.of("table"); }
        @Override public java.util.Optional<TableMetadata> observeTable(String schema, String table) {
            return java.util.Optional.of(getTable(schema, table));
        }

        @Override public TableMetadata getTable(String schema, String table) { return table(); }
        @Override public TableStatistics getTableStatistics(String schema, String table) { return TableStatistics.UNKNOWN; }
        @Override public void close() {}
    }
}
