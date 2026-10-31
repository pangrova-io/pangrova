package org.pangrova.connector.lance;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.spi.coordination.WriteCommitEntry;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.query.store.jdbc.anchor.JdbcExecutionTransactionFence;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises model-port transaction ownership through real PG source publication and SDK objects. */
class ManagedLanceModelAuthorityIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test
    void modelAuthorityUsesExecutionTransactionAndDenialRollsBackPublication() throws Exception {
        var models = new RecordingModelAuthority();
        var fence = new JdbcExecutionTransactionFence();
        var fixture = new LanceManagedSourceFixture("model-fence-" + UUID.randomUUID(), directory, models,
                (connection, execution, owner, incarnation, token) -> {
                    models.executionConnection = connection;
                    return fence.requireCurrent(connection, execution, owner, incarnation, token);
                });
        var prepared = LancePgSdkTestInputs.write(fixture, directory, directory.resolve("model-fence.lance").toString(),
                "model-fence", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 7, 9);
        assertThat(prepared.pages()).hasSize(1);
        var allocation = prepared.allocation();
        var publication = fixture.registerObjects(allocation.executionId(), prepared.pages().get(0));
        var original = fixture.journal().getEntry(allocation.executionId()).orElseThrow();
        models.reject = true;

        assertThatThrownBy(() -> fixture.acceptRegistered(allocation.executionId(), publication))
                .isInstanceOf(SecurityException.class).hasMessage("Model authorization revoked in transaction");

        assertThat(models.deniedConnection).isSameAs(models.executionConnection);
        assertThat(models.deniedConnection.isClosed()).isTrue();
        assertThat(models.denials).isEqualTo(1);
        var head = fixture.store().load(fixture.catalog().getCatalogName(), LanceManagedSourceFixture.TENANT,
                allocation.output().table().table());
        assertThat(head.version()).isZero();
        assertThat(head.rowCount()).isZero();
        var current = fixture.journal().getEntry(original.queryId()).orElseThrow();
        assertThat(current.state()).isEqualTo(WriteCommitEntry.State.PENDING);
        assertThat(current.publication()).isNull();
        assertThat(current.commitFingerprint()).isEqualTo(original.commitFingerprint());
        assertThat(current.writeOutputBinding()).isEqualTo(original.writeOutputBinding());
        assertThat(current.fragments().get(0)).isEqualTo(original.fragments().get(0));
        try (var connection = fixture.dataSource().getConnection()) {
            assertPublicationUnchanged(connection, original.commitFingerprint());
            try (var statement = connection.prepareStatement("SELECT to_regclass(?)")) {
                statement.setString(1, models.markerTable);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getString(1)).isNull();
                }
            }
        }

        models.reject = false;
        var accepted = fixture.acceptRegistered(allocation.executionId(), publication);
        assertThat(accepted.getVersion()).isEqualTo(1);
        assertThat(fixture.reference(accepted.getDatasetUri(), 0)).isEqualTo(accepted);
        assertThat(fixture.journal().getEntry(original.queryId()).orElseThrow().state())
                .isEqualTo(WriteCommitEntry.State.COMMITTED);
        try (var allocator = new RootAllocator()) {
            assertThat(prepared.sdk().read(accepted, allocator)).containsExactly(List.of(7), List.of(9));
        }
    }

    private static void assertPublicationUnchanged(Connection connection, String fingerprint) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT state, registered_count, object_count "
                + "FROM managed_source_publications WHERE fingerprint=?")) {
            statement.setString(1, fingerprint);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString(1)).isEqualTo("PREPARED");
                assertThat(result.getLong(2)).isPositive().isEqualTo(result.getLong(3));
                assertThat(result.next()).isFalse();
            }
        }
    }

    private static final class RecordingModelAuthority implements CurrentModelAuthority {
        private final CurrentModelAuthority emptyModels = GenerationAuthorityFixtures.modelAuthority();
        private final String markerTable = "model_rejection_" + UUID.randomUUID().toString().replace("-", "");
        private Connection executionConnection;
        private Connection deniedConnection;
        private boolean reject;
        private int denials;

        @Override
        public List<ModelSnapshot> observe(Connection connection, String principal, Set<String> names)
                throws SQLException {
            return emptyModels.observe(connection, principal, names);
        }

        @Override
        public void requireCurrent(Connection connection, String principal, List<ModelSnapshot> expected)
                throws SQLException {
            assertThat(connection).isSameAs(executionConnection);
            assertThat(connection.getAutoCommit()).isFalse();
            assertThat(connection.isClosed()).isFalse();
            assertThat(principal).isEqualTo(LanceManagedSourceFixture.PRINCIPAL);
            emptyModels.requireCurrent(connection, principal, expected);
            if (reject) {
                deniedConnection = connection;
                denials++;
                // PostgreSQL DDL is transactional: this marker must disappear with the rejected publication.
                try (var statement = connection.createStatement()) {
                    statement.executeUpdate("CREATE TABLE " + markerTable + " (id INTEGER PRIMARY KEY)");
                    statement.executeUpdate("INSERT INTO " + markerTable + " VALUES (1)");
                }
                throw new SecurityException("Model authorization revoked in transaction");
            }
        }
    }
}
