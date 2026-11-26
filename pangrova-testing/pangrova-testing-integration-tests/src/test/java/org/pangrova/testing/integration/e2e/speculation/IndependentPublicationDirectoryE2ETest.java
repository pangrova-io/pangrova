package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.persistence.materialization.postgresql.JdbcMaterializationCatalog;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.testing.integration.e2e.speculation.ProcessQueryResults.awaitResult;
import static org.pangrova.testing.integration.e2e.speculation.ProcessQueryResults.consumeAndDrain;

/** Exercises publication visibility and durable reader pins across two Coordinator processes. */
@E2ETest
class IndependentPublicationDirectoryE2ETest extends ContainerTestBase {
    private static final String SOURCE = "spec_catalog.default.publication_source";
    private static final String DYNAMIC = "spec_catalog.default.publication_test";
    private static final String DIRECTORY = "WITH registry AS (%FROM registry LEFT JOIN published ON TRUE%";
    private static final String READ_DYNAMIC = "SELECT id FROM " + DYNAMIC;

    @Test
    void observesPublishSuspendDropAndNameReuseWithoutLosingAnAdmittedRead() throws Exception {
        try (var writer = new IndependentLocalProcesses()) {
            writer.startCoordinatorAndFirstWorker();
            writer.createSourceCatalog();
            writer.query("CREATE TABLE " + SOURCE + " (id BIGINT)", ResultProtocol.GRPC_TLS);
            writer.query("INSERT INTO " + SOURCE + " VALUES (41)", ResultProtocol.GRPC_TLS);
            try (var reader = writer.startPeer()) {
                try {
                    verifyPublication(writer, reader);
                } catch (Exception | Error failure) {
                    failure.addSuppressed(new IllegalStateException(
                            "Publication diagnostics at " + writer.retainDiagnostics()
                                    + " and " + reader.retainDiagnostics()));
                    throw failure;
                }
            }
        }
    }

    private static void verifyPublication(IndependentLocalProcesses writer, IndependentLocalProcesses reader)
            throws Exception {
        writer.enableMetadataStatementCounters();
        var catalog = new JdbcMaterializationCatalog(writer.metadataDataSource());
        var name = MaterializationId.parse(ProcessSecurityAuthority.TENANT + "/" + DYNAMIC);
        assertThat(catalog.findPublished(name)).isEmpty();
        requireAbsent(reader);
        writer.query("CREATE DYNAMIC TABLE " + DYNAMIC + " TARGET_LAG = 'PT5M' AS SELECT id FROM " + SOURCE,
                ResultProtocol.GRPC_TLS);
        var original = catalog.findPublished(name).orElseThrow().generation().value();

        try (var client = reader.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            var admitted = client.beginQuery(READ_DYNAMIC, Map.of());
            var endpoint = awaitResult(admitted, deadline);
            String queryId = admitted.submission().queryId();
            assertThat(readerPins(writer.metadataDataSource(), queryId)).containsExactly(original);
            long steady = writer.metadataStatementCalls(DIRECTORY);
            var repeated = client.beginQuery(READ_DYNAMIC, Map.of());
            consumeAndDrain(client, repeated, awaitResult(repeated, deadline), List.of(41L), deadline);
            assertThat(writer.metadataStatementCalls(DIRECTORY) - steady).isZero();

            writer.query("ALTER DYNAMIC TABLE " + DYNAMIC + " SUSPEND", ResultProtocol.GRPC_TLS);
            // The DDL and the rejected lookup together temporarily need the second tenant slot.
            assertThat(catalog.findPublished(name)).isEmpty();
            requireAbsent(reader);
            assertThat(readerPins(writer.metadataDataSource(), queryId)).containsExactly(original);
            consumeAndDrain(client, admitted, endpoint, List.of(41L), deadline);
            assertThat(readerPins(writer.metadataDataSource(), queryId)).isEmpty();

            writer.query("DROP DYNAMIC TABLE " + DYNAMIC, ResultProtocol.GRPC_TLS);
            requireAbsent(reader);
            writer.query("CREATE DYNAMIC TABLE " + DYNAMIC
                    + " TARGET_LAG = 'PT5M' AS SELECT id + 1 AS id FROM " + SOURCE, ResultProtocol.GRPC_TLS);
            assertThat(catalog.findPublished(name).orElseThrow().generation().value()).isNotEqualTo(original);
            var recreated = client.beginQuery(READ_DYNAMIC, Map.of());
            consumeAndDrain(client, recreated, awaitResult(recreated, deadline), List.of(42L), deadline);
            assertThat(writer.activeTenantReservations()).isZero();
        }
    }

    private static void requireAbsent(IndependentLocalProcesses reader) throws Exception {
        try (var client = reader.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            var source = client.beginQuery("SELECT id FROM " + SOURCE, Map.of());
            consumeAndDrain(client, source, awaitResult(source, deadline), List.of(41L), deadline);
        }
        var rejected = reader.executeSql(READ_DYNAMIC, ResultProtocol.GRPC_TLS, Duration.ofSeconds(30));
        assertThat(rejected.exitCode()).isNotZero();
        assertThat(rejected.errors().toLowerCase(Locale.ROOT)).contains("validation failed", "not found");
    }

    private static List<UUID> readerPins(DataSource authority, String queryId) throws SQLException {
        try (var connection = authority.getConnection();
             var statement = connection.prepareStatement("""
                     SELECT generation_id FROM materialization.reader_pins
                     WHERE reader_kind='QUERY' AND reader_id=?
                     """)) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                var generations = new ArrayList<UUID>();
                while (rows.next()) generations.add(rows.getObject(1, UUID.class));
                return generations;
            }
        }
    }
}
