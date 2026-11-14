package org.pangrova.persistence.table.postgresql.governancebinding;

import org.pangrova.table.api.policy.GovernanceBinding;
import org.pangrova.table.api.enums.GovernanceBindingType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import org.pangrova.table.api.exception.TableControlException;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@Tag("integration")
class JdbcTableControlGovernanceBindingStoreTest extends ContainerTestBase {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private DataSource dataSource;
    private JdbcTableControlGovernanceBindingStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = MigratedPostgres.create();
        store = new JdbcTableControlGovernanceBindingStore(dataSource);
    }

    @Test
    void bindingSurvivesARestartAndKeepsItsTableId() {
        UUID tableId = UUID.randomUUID();
        GovernanceBinding binding = binding(tableId,
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of("value", "EMAIL"));

        store.save(binding);

        GovernanceBinding reloaded = new JdbcTableControlGovernanceBindingStore(dataSource)
                .find(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type").orElseThrow();
        assertThat(reloaded.bindingId()).isEqualTo(binding.bindingId());
        assertThat(reloaded.tableId()).isEqualTo(tableId);
        assertThat(reloaded.config()).containsEntry("value", "EMAIL");
        assertThat(reloaded.createdBy()).isEqualTo("alice");
        assertThat(reloaded.createdAt()).isEqualTo(NOW);
    }

    @Test
    void upsertReplacesConfigWithoutChangingCreationMetadata() {
        UUID tableId = UUID.randomUUID();
        store.save(binding(tableId,
                GovernanceBindingType.QUALITY_RULE, "non_null_id", Map.of("max_null", "0")));

        store.save(new GovernanceBinding(UUID.randomUUID(), tableId,
                GovernanceBindingType.QUALITY_RULE, "non_null_id",
                Map.of("max_null", "5"), "bob", NOW, NOW.plusSeconds(30)));

        GovernanceBinding stored = store.find(tableId,
                GovernanceBindingType.QUALITY_RULE, "non_null_id").orElseThrow();
        assertThat(stored.config()).containsEntry("max_null", "5");
        assertThat(stored.createdBy()).isEqualTo("alice");
        assertThat(stored.createdAt()).isEqualTo(NOW);
        assertThat(stored.updatedAt()).isEqualTo(NOW.plusSeconds(30));
    }

    @Test
    void bindingsAreScopedByTableId() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        store.save(binding(first, GovernanceBindingType.COLUMN_MASK, "email_mask", Map.of()));
        store.save(binding(second, GovernanceBindingType.COLUMN_MASK, "email_mask", Map.of()));

        assertThat(store.list(first)).extracting(GovernanceBinding::tableId).containsOnly(first);
        assertThat(store.list(second)).extracting(GovernanceBinding::tableId).containsOnly(second);
    }

    @Test
    void deleteRemovesOnlyTheNamedBinding() {
        UUID tableId = UUID.randomUUID();
        store.save(binding(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of()));
        store.save(binding(tableId, GovernanceBindingType.LINEAGE, "upstream", Map.of()));

        store.delete(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type");

        assertThat(store.find(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type")).isEmpty();
        assertThat(store.find(tableId, GovernanceBindingType.LINEAGE, "upstream")).isPresent();
    }

    @Test
    void deleteAllRetiresEveryBindingForTheTable() {
        UUID tableId = UUID.randomUUID();
        store.save(binding(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of()));
        store.save(binding(tableId, GovernanceBindingType.ROW_FILTER, "tenant_filter", Map.of()));
        store.save(binding(UUID.randomUUID(),
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of()));

        store.deleteAll(tableId);

        assertThat(store.list(tableId)).isEmpty();
    }

    @Test
    void upsertReceiptPreservesTheFirstCreationIdentity() {
        UUID tableId = UUID.randomUUID();
        var first = binding(tableId, GovernanceBindingType.QUALITY_RULE, "rule", Map.of("value", "first"));
        store.save(first);
        var update = new GovernanceBinding(UUID.randomUUID(), tableId,
                first.bindingType(), first.bindingRef(), Map.of("value", "updated"),
                "bob", NOW.plusSeconds(20), NOW.plusSeconds(30));

        var saved = store.save(update);

        assertThat(saved.bindingId()).isEqualTo(first.bindingId());
        assertThat(saved.createdBy()).isEqualTo(first.createdBy());
        assertThat(saved.createdAt()).isEqualTo(first.createdAt());
        assertThat(saved.config()).isEqualTo(update.config());
        assertThat(saved.updatedAt()).isEqualTo(update.updatedAt());
        assertThat(store.find(tableId, first.bindingType(), first.bindingRef())).contains(saved);
    }

    @Test
    void competingInitialUpsertsReturnOneCreationIdentity() throws Exception {
        UUID tableId = UUID.randomUUID();
        var first = binding(tableId, GovernanceBindingType.QUALITY_RULE, "rule", Map.of("value", "first"));
        var second = new GovernanceBinding(UUID.randomUUID(), tableId,
                first.bindingType(), first.bindingRef(), Map.of("value", "second"),
                "bob", NOW.plusSeconds(20), NOW.plusSeconds(30));
        var competingStore = new JdbcTableControlGovernanceBindingStore(dataSource);
        var ready = new java.util.concurrent.CountDownLatch(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var a = pool.submit(() -> { ready.countDown(); start.await(); return store.save(first); });
            var b = pool.submit(() -> { ready.countDown(); start.await(); return competingStore.save(second); });
            assertThat(ready.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            start.countDown();
            var savedA = a.get(10, java.util.concurrent.TimeUnit.SECONDS);
            var savedB = b.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(savedA.bindingId()).isEqualTo(savedB.bindingId());
            assertThat(savedA.createdBy()).isEqualTo(savedB.createdBy());
            assertThat(savedA.createdAt()).isEqualTo(savedB.createdAt());
            assertThat(savedA.config()).isEqualTo(first.config());
            assertThat(savedB.config()).isEqualTo(second.config());
            var current = store.find(tableId, first.bindingType(), first.bindingRef()).orElseThrow();
            assertThat(current).isIn(savedA, savedB);
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest
    @MethodSource("deletionFailures")
    void failedDeletionPreservesBindingsAndPrimaryFailure(boolean deleteAll, Throwable failure,
            SQLException rollbackFailure) {
        UUID tableId = UUID.randomUUID();
        UUID otherId = UUID.randomUUID();
        var target = binding(tableId, GovernanceBindingType.COLUMN_TAG, "pii", Map.of());
        var retained = binding(tableId, GovernanceBindingType.ROW_FILTER, "tenant", Map.of());
        var other = binding(otherId, GovernanceBindingType.COLUMN_TAG, "pii", Map.of());
        store.save(target);
        store.save(retained);
        store.save(other);
        var before = store.list(tableId);
        var failingStore = new JdbcTableControlGovernanceBindingStore(JdbcMutationFault.afterMutation(
                dataSource, "DELETE FROM table_control_governance_binding", failure, rollbackFailure));

        Throwable caught = catchThrowable(() -> {
            if (deleteAll) failingStore.deleteAll(tableId);
            else failingStore.delete(tableId, target.bindingType(), target.bindingRef());
        });

        assertThat(store.list(tableId)).containsExactlyElementsOf(before);
        assertThat(store.list(otherId)).containsExactly(other);
        if (failure instanceof SQLException) {
            assertThat(caught).isInstanceOf(TableControlException.class).hasCause(failure);
        } else {
            assertThat(caught).isSameAs(failure);
        }
        assertThat(failure.getSuppressed()).containsExactly(
                rollbackFailure == null ? new Throwable[0] : new Throwable[]{rollbackFailure});
        // A fresh mutation must acquire the authority lock after failure cleanup.
        store.delete(tableId, target.bindingType(), target.bindingRef());
        assertThat(store.list(tableId)).containsExactly(retained);
        store.deleteAll(tableId);
        assertThat(store.list(tableId)).isEmpty();
    }

    @Test
    void publicationParticipantReadsUncommittedChangesWithoutTakingConnectionOwnership() throws Exception {
        UUID tableId = UUID.randomUUID();
        var original = binding(tableId, GovernanceBindingType.COLUMN_TAG, "pii", Map.of("value", "EMAIL"));
        store.save(original);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement(
                    "DELETE FROM table_control_governance_binding WHERE table_id=?")) {
                statement.setObject(1, tableId);
                statement.executeUpdate();
            }
            assertThat(JdbcTableControlGovernanceBindingStore.list(connection, tableId)).isEmpty();
            assertThat(connection.isClosed()).isFalse();
            assertThat(connection.getAutoCommit()).isFalse();
            assertThat(store.list(tableId)).containsExactly(original);
            connection.rollback();
            assertThat(JdbcTableControlGovernanceBindingStore.list(connection, tableId)).containsExactly(original);
        }
        assertThat(store.list(tableId)).containsExactly(original);
    }

    private static Stream<Arguments> deletionFailures() {
        return Stream.of(false, true).flatMap(deleteAll -> Stream.of(false, true).flatMap(failRollback -> Stream.of(
                new SQLException("after deletion"), new IllegalStateException("after deletion"),
                new AssertionError("after deletion")).map(failure -> Arguments.of(deleteAll, failure,
                        failRollback ? new SQLException("rollback unavailable") : null))));
    }

    private static GovernanceBinding binding(UUID tableId, GovernanceBindingType type,
            String ref, Map<String, String> config) {
        return new GovernanceBinding(UUID.randomUUID(), tableId, type, ref, config,
                "alice", NOW, NOW);
    }
}
