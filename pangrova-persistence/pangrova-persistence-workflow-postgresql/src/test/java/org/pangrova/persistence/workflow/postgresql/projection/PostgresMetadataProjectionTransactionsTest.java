package org.pangrova.persistence.workflow.postgresql.projection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionScopeException;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresSchemaProjectionRepository;

/** Public atomic port tested on a canonically initialized, isolated real PostgreSQL database. */
class PostgresMetadataProjectionTransactionsTest {
    private DataSource source;
    private long epoch;
    private PostgresCatalogDeploymentRepository deployments;
    private PostgresMetadataProjectionTransactions transactions;
    private PostgresTableProjectionRepository tables;
    private CatalogDescriptor descriptor;

    @BeforeEach void initialize() throws Exception {
        source = MigratedPostgres.create();
        deployments = new PostgresCatalogDeploymentRepository(source);
        transactions = new PostgresMetadataProjectionTransactions(source);
        tables = new PostgresTableProjectionRepository(source);
        descriptor = new CatalogDescriptor("cat", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of("tenant-a"));
        deployments.saveCatalogDescriptor(descriptor);
        epoch = transactions.beginObservation(descriptor.identity());
        transactions.apply(epoch, new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), descriptor.identity(), List.of("sch")));
    }

    @Test void newTableAtomicallyObservesItsPreviouslyUnknownParentSchema() throws Exception {
        var table = new TableMetadata("cat", "new_schema", "new_table", List.of(), Map.of(), List.of(),
                Map.of(), Instant.EPOCH, Instant.EPOCH);
        var mutation = upsert(table);
        long events = count("invalidation_log");
        var receipt = transactions.apply(epoch, mutation);
        assertThat(new PostgresSchemaProjectionRepository(source).loadSchemas("cat")).containsExactly("new_schema", "sch");
        assertThat(tables.loadTable("cat", "new_schema", "new_table")).contains(table);
        assertThat(transactions.findReceipt(mutation.operationId())).contains(receipt);
        assertThat(count("invalidation_log")).isEqualTo(events + 1);
    }

    @Test void failedTablePublicationAlsoRollsBackImplicitSchemaObservation() throws Exception {
        var table = new TableMetadata("cat", "new_schema", "new_table", List.of(), Map.of(), List.of(),
                Map.of(), Instant.EPOCH, Instant.EPOCH);
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_projection_mutations ADD CONSTRAINT deny_receipt CHECK (revision < 0) NOT VALID");
        }
        var mutation = upsert(table);
        assertThatThrownBy(() -> transactions.apply(epoch, mutation))
                .isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException.class);
        assertThat(new PostgresSchemaProjectionRepository(source).loadSchemas("cat")).containsExactly("sch");
        assertThat(tables.loadTable("cat", "new_schema", "new_table")).isEmpty();
        assertThat(transactions.findReceipt(mutation.operationId())).isEmpty();
    }

    @Test void invalidIdentifiersCannotCreateAnUnrecoverableIntent() throws Exception {
        long intents = count("metadata_projection_intents");
        for (String name : List.of("nul\0name", "x".repeat(256))) {
            var mutation = new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), descriptor.identity(), List.of(name));
            assertThatThrownBy(() -> transactions.apply(epoch, mutation)).isInstanceOf(MetadataProjectionScopeException.class);
        }
        assertThat(count("metadata_projection_intents")).isEqualTo(intents);
        String unicodeName = "\uD83C\uDF3F".repeat(255);
        transactions.apply(epoch, new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), descriptor.identity(), List.of(unicodeName, "", "  ")));
        assertThat(new PostgresSchemaProjectionRepository(source).loadSchemas("cat")).contains(unicodeName, "", "  ");
    }

    @Test void supersededEpochCanConfirmHistoryButCannotReserveNewWork() throws Exception {
        var original = upsert(table("original"));
        var receipt = transactions.apply(epoch, original);
        long newer = new PostgresMetadataProjectionTransactions(source).beginObservation(descriptor.identity());
        assertThat(newer).isGreaterThan(epoch);
        long events = count("invalidation_log");
        assertThat(transactions.apply(epoch, original)).isEqualTo(receipt);
        for (long refused : List.of(epoch, 0L)) {
            assertThatThrownBy(() -> transactions.apply(refused, upsert(table("stale"))))
                    .isInstanceOf(MetadataProjectionConflictException.class);
        }
        assertThat(count("invalidation_log")).isEqualTo(events);
        assertThat(tables.loadTable("cat", "sch", "tbl")).contains(table("original"));
    }

    @Test void updateReceiptCapturesPreviousProjectionAndCurrentTenantScope() {
        var original = table("original");
        transactions.apply(epoch, upsert(original));
        var receipt = transactions.apply(epoch, upsert(table("updated")));
        assertThat(receipt.previousTable()).contains(original);
        assertThat(receipt.allowedTenants()).containsExactly("tenant-a");
        assertThat(receipt.changed()).isTrue();
        assertThat(transactions.findReceipt(receipt.mutation().operationId())).contains(receipt);
    }

    @Test void exactReplayAfterReplacementConfirmsHistoryWithoutChangingCurrentProjection() throws Exception {
        var mutation = upsert(table("one"));
        var receipt = transactions.apply(epoch, mutation);
        deployments.deleteCatalogDescriptor(descriptor.identity());
        var replacement = new CatalogDescriptor("cat", CatalogType.FEDERATION, "replacement", CatalogState.ACTIVE,
                Map.of(), "", Set.of("tenant-b"));
        deployments.saveCatalogDescriptor(replacement);
        long events = count("invalidation_log");
        assertThat(transactions.apply(epoch, mutation)).isEqualTo(receipt);
        assertThat(tables.loadTable("cat", "sch", "tbl")).isEmpty();
        assertThat(count("invalidation_log")).isEqualTo(events);
        assertThat(transactions.findReceipt(mutation.operationId())).contains(receipt);
        assertThatThrownBy(() -> transactions.apply(epoch, upsert(table("new-id"))))
                .isInstanceOf(MetadataProjectionConflictException.class);
    }

    @Test void sameIdDifferentContentCannotMutateOrInvalidate() throws Exception {
        var mutation = upsert(table("one"));
        var receipt = transactions.apply(epoch, mutation);
        long events = count("invalidation_log");
        assertThat(transactions.apply(epoch, mutation)).isEqualTo(receipt);
        var conflict = new MetadataProjectionMutation.UpsertTable(mutation.operationId(), descriptor.identity(), table("two"));
        assertThatThrownBy(() -> transactions.apply(epoch, conflict)).isInstanceOf(MetadataProjectionConflictException.class);
        assertThat(tables.loadTable("cat", "sch", "tbl")).contains(table("one"));
        assertThat(count("invalidation_log")).isEqualTo(events);
    }

    @Test void revisionChangeFencesAllMutationKindsWithoutNewEventsOrReceipts() throws Exception {
        transactions.apply(epoch, upsert(table("original")));
        advanceRevision();
        long events = count("invalidation_log");
        long receipts = count("metadata_projection_mutations");
        var mutations = List.of(upsert(table("stale")),
                new MetadataProjectionMutation.DeleteTable(UUID.randomUUID(), descriptor.identity(), new QualifiedTableRef("cat", "sch", "tbl")),
                new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), descriptor.identity(), List.of("forbidden")));
        for (var mutation : mutations) {
            assertThatThrownBy(() -> transactions.apply(epoch, mutation)).isInstanceOf(MetadataProjectionConflictException.class);
            assertThat(transactions.findReceipt(mutation.operationId())).isEmpty();
        }
        assertThat(tables.loadTable("cat", "sch", "tbl")).contains(table("original"));
        assertThat(new PostgresSchemaProjectionRepository(source).loadSchemas("cat")).containsExactly("sch");
        assertThat(count("invalidation_log")).isEqualTo(events);
        assertThat(count("metadata_projection_mutations")).isEqualTo(receipts);
    }

    @Test void deletedDeploymentCannotMutateEvenAnEmptyObservation() {
        deployments.deleteCatalogDescriptor(descriptor.identity());
        var operation = new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), descriptor.identity(), List.of());
        assertThatThrownBy(() -> transactions.apply(epoch, operation)).isInstanceOf(MetadataProjectionConflictException.class);
        assertThat(transactions.findReceipt(operation.operationId())).isEmpty();
    }

    @Test void foreignCatalogRejectedBeforeAnySql() {
        var other = new QualifiedTableRef("other", "sch", "tbl");
        assertThatThrownBy(() -> new MetadataProjectionMutation.DeleteTable(UUID.randomUUID(), descriptor.identity(), other))
                .isInstanceOf(MetadataProjectionScopeException.class);
        var foreign = new TableMetadata("other", "sch", "tbl", List.of(), Map.of(), List.of(), Map.of(), Instant.EPOCH, Instant.EPOCH);
        assertThatThrownBy(() -> new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), descriptor.identity(), foreign))
                .isInstanceOf(MetadataProjectionScopeException.class);
    }

    @Test void normalizedSchemaObservationReplaysAndNeverDeletesMissingNames() throws Exception {
        UUID id = UUID.randomUUID();
        var receipt = transactions.apply(epoch, new MetadataProjectionMutation.ObserveSchemas(id, descriptor.identity(), List.of("z", "a", "z")));
        long events = count("invalidation_log");
        assertThat(transactions.apply(epoch, new MetadataProjectionMutation.ObserveSchemas(id, descriptor.identity(), List.of("a", "z")))).isEqualTo(receipt);
        var empty = transactions.apply(epoch, new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), descriptor.identity(), List.of()));
        assertThat(empty.changed()).isFalse();
        assertThat(transactions.findReceipt(empty.mutation().operationId())).contains(empty);
        assertThat(new PostgresSchemaProjectionRepository(source).loadSchemas("cat")).containsExactly("a", "sch", "z");
        assertThat(count("invalidation_log")).isEqualTo(events);
    }

    @Test void missingTableDeletionHasDurableNoopReceipt() throws Exception {
        var mutation = new MetadataProjectionMutation.DeleteTable(UUID.randomUUID(), descriptor.identity(), new QualifiedTableRef("cat", "sch", "absent"));
        long events = count("invalidation_log");
        var receipt = transactions.apply(epoch, mutation);
        assertThat(receipt.changed()).isFalse();
        assertThat(receipt.previousTable()).isEmpty();
        assertThat(transactions.apply(epoch, mutation)).isEqualTo(receipt);
        assertThat(count("invalidation_log")).isEqualTo(events);
    }

    @Test void failedReceiptInsertRollsBackProjectionAndOutbox() throws Exception {
        long events = count("invalidation_log");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE metadata_projection_mutations ADD CONSTRAINT deny_receipt CHECK (revision < 0) NOT VALID");
        }
        var mutation = upsert(table("denied"));
        assertThatThrownBy(() -> transactions.apply(epoch, mutation)).isInstanceOf(MetastoreException.class)
                .hasRootCauseInstanceOf(SQLException.class)
                .satisfies(failure -> assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("23514"));
        assertThat(transactions.findReceipt(mutation.operationId())).isEmpty();
        assertThat(tables.loadTable("cat", "sch", "tbl")).isEmpty();
        assertThat(count("invalidation_log")).isEqualTo(events);
    }

    @Test void committedRevisionUpdateWinsAgainstAlreadyWaitingProjection() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        var mutation = upsert(table("stale"));
        long events = count("invalidation_log");
        try (var blocker = source.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) {
                statement.execute("UPDATE catalog_definitions SET revision=revision+1, allowed_tenants='[\"tenant-b\"]' WHERE name='cat'");
            }
            var writer = executor.submit(() -> catchThrowable(() -> transactions.apply(epoch, mutation)));
            try {
                await().atMost(Duration.ofSeconds(10)).until(this::projectionWaitsForDeployment);
                assertThat(tables.loadTable("cat", "sch", "tbl")).isEmpty();
                assertThat(transactions.findReceipt(mutation.operationId())).isEmpty();
            } finally { blocker.commit(); }
            assertThat(writer.get(10, TimeUnit.SECONDS)).isInstanceOf(MetadataProjectionConflictException.class);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(count("invalidation_log")).isEqualTo(events);
    }

    @Test void concurrentIdenticalOperationProducesOneReceiptAndOneEvent() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        var mutation = upsert(table("one"));
        long events = count("invalidation_log");
        try (var blocker = source.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) { statement.execute("SELECT 1 FROM catalog_definitions WHERE name='cat' FOR UPDATE"); }
            var first = executor.submit(() -> transactions.apply(epoch, mutation));
            var second = executor.submit(() -> transactions.apply(epoch, mutation));
            try { await().atMost(Duration.ofSeconds(10)).until(() -> waitingProjections() == 2); }
            finally { blocker.commit(); }
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(second.get(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(count("invalidation_log")).isEqualTo(events + 1);
    }

    @Test void transactionPinsReadCommittedForReceiptRechecksRegardlessOfPoolDefault() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION require_projection_isolation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + "IF current_setting('transaction_isolation') <> 'read committed' THEN RAISE EXCEPTION 'wrong projection isolation'; END IF; RETURN NEW; END $$");
            statement.execute("CREATE TRIGGER require_projection_isolation BEFORE INSERT ON metadata_projection_mutations "
                    + "FOR EACH ROW EXECUTE FUNCTION require_projection_isolation()");
        }
        DataSource serializableDefault = (DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                    try {
                        Object value = method.invoke(source, arguments);
                        if (method.getName().equals("getConnection")) {
                            ((java.sql.Connection) value).setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
                        }
                        return value;
                    } catch (java.lang.reflect.InvocationTargetException wrapped) { throw wrapped.getCause(); }
                });
        var mutation = upsert(table("isolation"));
        var receipt = new PostgresMetadataProjectionTransactions(serializableDefault).apply(epoch, mutation);
        assertThat(transactions.findReceipt(mutation.operationId())).contains(receipt);
    }

    @Test void replayConfirmsConcurrentCommitEvenWhenDeploymentWasReplacedAfterInitialReceiptRead() throws Exception {
        var mutation = upsert(table("committed"));
        var interleaved = new java.util.concurrent.atomic.AtomicBoolean();
        DataSource interleaving = (DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                    try {
                        Object value = method.invoke(source, arguments);
                        if (!method.getName().equals("getConnection")) return value;
                        var delegate = (java.sql.Connection) value;
                        return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[]{java.sql.Connection.class}, (connectionProxy, call, parameters) -> {
                                    try {
                                        if (call.getName().equals("prepareStatement") && parameters[0].toString().startsWith("SELECT incarnation_id")
                                                && interleaved.compareAndSet(false, true)) {
                                            transactions.apply(epoch, mutation);
                                            deployments.deleteCatalogDescriptor(descriptor.identity());
                                            deployments.saveCatalogDescriptor(new CatalogDescriptor("cat", CatalogType.FEDERATION,
                                                    "replacement", CatalogState.ACTIVE, Map.of(), "", Set.of()));
                                        }
                                        return call.invoke(delegate, parameters);
                                    } catch (java.lang.reflect.InvocationTargetException wrapped) { throw wrapped.getCause(); }
                                });
                    } catch (java.lang.reflect.InvocationTargetException wrapped) { throw wrapped.getCause(); }
                });
        var replay = new PostgresMetadataProjectionTransactions(interleaving).apply(epoch, mutation);
        assertThat(interleaved).isTrue();
        assertThat(replay).isEqualTo(transactions.findReceipt(mutation.operationId()).orElseThrow());
        assertThat(tables.loadTable("cat", "sch", "tbl")).isEmpty();
    }

    @Test void concurrentOperationIdCollisionAcrossCatalogsRollsBackLosingProjectionAndEvent() throws Exception {
        var peer = new CatalogDescriptor("peer", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE, Map.of(), "", Set.of());
        deployments.saveCatalogDescriptor(peer);
        long peerEpoch = transactions.beginObservation(peer.identity());
        transactions.apply(peerEpoch, new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), peer.identity(), List.of("sch")));
        UUID id = UUID.randomUUID();
        var firstMutation = new MetadataProjectionMutation.UpsertTable(id, descriptor.identity(), table("cat"));
        var peerTable = new TableMetadata("peer", "sch", "tbl", List.of(), Map.of(), List.of(), Map.of(), Instant.EPOCH, Instant.EPOCH);
        var secondMutation = new MetadataProjectionMutation.UpsertTable(id, peer.identity(), peerTable);
        long events = count("invalidation_log");
        long receipts = count("metadata_projection_mutations");
        var executor = Executors.newFixedThreadPool(2);
        try (var blocker = source.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) { statement.execute("LOCK TABLE invalidation_log IN SHARE MODE"); }
            var first = executor.submit(() -> catchThrowable(() -> transactions.apply(epoch, firstMutation)));
            var second = executor.submit(() -> catchThrowable(() -> transactions.apply(peerEpoch, secondMutation)));
            try {
                await().atMost(Duration.ofSeconds(10)).until(() -> {
                    try (var connection = source.getConnection(); var statement = connection.createStatement();
                            var rows = statement.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                                    + "AND wait_event_type='Lock' AND query LIKE 'INSERT INTO invalidation_log %'")) {
                        rows.next(); return rows.getLong(1) == 1 && (first.isDone() || second.isDone());
                    }
                });
            } finally { blocker.commit(); }
            var outcomes = java.util.Arrays.asList(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            assertThat(outcomes.stream().filter(java.util.Objects::isNull)).hasSize(1);
            assertThat(outcomes.stream().filter(java.util.Objects::nonNull)).singleElement()
                    .isInstanceOf(MetadataProjectionConflictException.class);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        var winner = transactions.findReceipt(id).orElseThrow().mutation();
        String winningCatalog = winner.deployment().catalogName();
        String losingCatalog = winningCatalog.equals("cat") ? "peer" : "cat";
        assertThat(tables.loadTable(winningCatalog, "sch", "tbl")).isPresent();
        assertThat(tables.loadTable(losingCatalog, "sch", "tbl")).isEmpty();
        assertThat(count("invalidation_log")).isEqualTo(events + 1);
        assertThat(count("metadata_projection_mutations")).isEqualTo(receipts + 1);
    }

    private boolean projectionWaitsForDeployment() throws SQLException { return waitingProjections() > 0; }
    private long waitingProjections() throws SQLException {
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        + "AND wait_event_type='Lock' AND query LIKE 'SELECT incarnation_id, revision, allowed_tenants%FOR UPDATE'")) {
            rows.next(); return rows.getLong(1);
        }
    }
    private void advanceRevision() {
        deployments.saveCatalogDescriptor(new CatalogDescriptor(descriptor.name(), descriptor.type(), descriptor.provider(),
                descriptor.state(), descriptor.properties(), descriptor.comment(), Set.of("tenant-b"), descriptor.incarnationId(), descriptor.revision()+1));
    }
    private MetadataProjectionMutation.UpsertTable upsert(TableMetadata table) {
        return new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), descriptor.identity(), table);
    }
    private static TableMetadata table(String marker) {
        return new TableMetadata("cat", "sch", "tbl", List.of(), Map.of(), List.of(), Map.of("marker", marker), Instant.EPOCH, Instant.EPOCH);
    }
    private long count(String relation) throws SQLException {
        try (var connection = source.getConnection(); var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT count(*) FROM " + relation)) {
            rows.next(); return rows.getLong(1);
        }
    }
}
