package org.pangrova.testing.integration.chaos.cdc;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.persistence.streaming.postgresql.JdbcCdcCommitStateStore;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.commit.CdcBatchCheckpoint;
import org.pangrova.streaming.api.commit.OffsetRange;
import org.pangrova.streaming.api.commit.PrepareToken;
import org.pangrova.streaming.api.commit.SourceId;
import org.pangrova.streaming.api.enums.CdcCommitState;
import org.pangrova.streaming.commit.CommitParticipant;
import org.pangrova.streaming.commit.TwoPhaseCommitCoordinator;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the PostgreSQL CDC coordinator against a sink with attempt-scoped staged rows.
 * Abandoned prepares leave the checkpoint unchanged and staged rows invisible. Recovery
 * and replay publish exactly one row for each consumed offset. Toxiproxy disconnects the
 * coordinator's database connection before the publication claim in the network case.
 */
@DisplayName("CDC EOS Chaos Integration — 2PC Fault Injection")
@Execution(ExecutionMode.SAME_THREAD)
class CdcEosChaosIntegrationTest {

    private static final SourceId SOURCE = new SourceId("cdc-source-orders");
    private static final String TOPIC = "orders";
    private static final int PARTITION = 0;
    private static final String POSTGRES_NETWORK_ALIAS = "postgres";

    private static Network network;
    private static PostgreSQLContainer<?> postgres;
    private static ToxiproxyContainer toxiproxy;

    private static PGSimpleDataSource database;

    private Proxy postgresProxy;
    private PGSimpleDataSource proxyDatabase;

    @BeforeAll
    static void startContainers() throws SQLException {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException(
                    "CDC chaos tests require a reachable Docker daemon; refusing to run unexercised");
        }

        network = Network.newNetwork();
        postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
                .withDatabaseName("pangrova_cdc")
                .withUsername("pangrova")
                .withPassword("pangrova_test");
        postgres.start();

        toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0"))
                .withNetwork(network);
        toxiproxy.start();

        database = MigratedPostgres.create(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        initDownstream();
    }

    @AfterAll
    static void stopContainers() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    private static void initDownstream() throws SQLException {
        try (Connection conn = database.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE cdc_downstream_rows (
                        attempt VARCHAR(36) NOT NULL,
                        offset_value BIGINT NOT NULL,
                        published BOOLEAN NOT NULL DEFAULT FALSE,
                        PRIMARY KEY(attempt, offset_value)
                    )
                    """);
            stmt.execute("CREATE UNIQUE INDEX published_source_offsets "
                    + "ON cdc_downstream_rows(offset_value) WHERE published");
        }
    }

    @BeforeEach
    void setupProxy() throws IOException {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(
                toxiproxy.getHost(), toxiproxy.getControlPort());
        postgresProxy = toxiproxyClient.createProxy(
                "postgres-proxy",
                "0.0.0.0:8666",
                POSTGRES_NETWORK_ALIAS + ":5432");
        proxyDatabase = new PGSimpleDataSource();
        proxyDatabase.setURL("jdbc:postgresql://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666)
                + "/" + postgres.getDatabaseName());
        proxyDatabase.setUser(postgres.getUsername());
        proxyDatabase.setPassword(postgres.getPassword());
        proxyDatabase.setCurrentSchema(database.getCurrentSchema());
        proxyDatabase.setConnectTimeout(2);
        proxyDatabase.setSocketTimeout(2);
    }

    @AfterEach
    void cleanupProxy() throws IOException, SQLException {
        if (postgresProxy != null) {
            postgresProxy.delete();
        }
        try (var connection = database.getConnection(); var statement = connection.createStatement()) {
            statement.execute("TRUNCATE cdc_downstream_rows, cdc_commit_state");
        }
    }

    @Test
    void coordinatorCrashBetweenPrepareAndCommit() throws Exception {
        var token = prepare(database, 0, 9);
        stageRows(token, 0, 9);
        verifyRecoveryAndReplay(token);
    }

    @Test
    void workerCrashWithPartiallyStagedOutput() throws Exception {
        var token = prepare(database, 10, 19);
        stageRows(token, 10, 14);
        verifyRecoveryAndReplay(token);
    }

    @Test
    void networkPartitionBeforePublicationClaim() throws Exception {
        var token = prepare(proxyDatabase, 20, 29);
        stageRows(token, 20, 29);
        postgresProxy.disable();
        try {
            assertThatThrownBy(() -> publish(proxyDatabase, token))
                    .isInstanceOf(PangrovaException.class).hasCauseInstanceOf(SQLException.class);
        } finally {
            postgresProxy.enable();
        }
        assertThat(new JdbcCdcCommitStateStore(database).findStateById(token.id()))
                .contains(CdcCommitState.PREPARED);
        verifyRecoveryAndReplay(token);
    }

    private static TwoPhaseCommitCoordinator coordinator(DataSource source) {
        return new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(source), Duration.ofMinutes(1));
    }

    private static PrepareToken prepare(DataSource source, long first, long last) {
        return coordinator(source).prepare(SOURCE,
                new CdcBatchCheckpoint(List.of(new OffsetRange(TOPIC, PARTITION, first, last))));
    }

    private static void verifyRecoveryAndReplay(PrepareToken abandoned) throws SQLException {
        try (var connection = database.getConnection(); var expire = connection.prepareStatement(
                "UPDATE cdc_commit_state SET deadline = CURRENT_TIMESTAMP - INTERVAL '1 minute' WHERE id = ?")) {
            expire.setString(1, abandoned.id().toString());
            assertThat(expire.executeUpdate()).isOne();
        }
        var restarted = coordinator(database);
        assertThat(restarted.recover(SOURCE)).extracting(PrepareToken::id).containsExactly(abandoned.id());
        assertThat(new JdbcCdcCommitStateStore(database).findStateById(abandoned.id()))
                .contains(CdcCommitState.ROLLED_BACK);
        assertThat(restarted.findLastCommittedOffsets(SOURCE)).isEmpty();
        assertThat(visibleOffsets()).isEmpty();

        OffsetRange range = abandoned.checkpoint().ranges().get(0);
        PrepareToken retry = restarted.prepare(SOURCE, abandoned.checkpoint());
        assertThat(retry.id()).isNotEqualTo(abandoned.id());
        assertThat(publish(database, retry)).isTrue();
        assertThat(publish(database, retry)).isFalse();
        assertThat(coordinator(database).findLastCommittedOffsets(SOURCE))
                .containsExactlyEntriesOf(java.util.Map.of(new TopicPartitionOffset(TOPIC, PARTITION),
                        range.endOffset() + 1));
        assertThat(visibleOffsets()).containsExactlyInAnyOrderElementsOf(
                java.util.stream.LongStream.rangeClosed(range.startOffset(), range.endOffset()).boxed().toList());
    }

    /** The test sink stages rows by attempt, just as an unpublished file is scoped to its writer. */
    private static boolean publish(DataSource source, PrepareToken token) {
        OffsetRange range = token.checkpoint().ranges().get(0);
        return coordinator(source).executeAtomicCommit(SOURCE, token.checkpoint(), List.of(new CommitParticipant() {
            public String name() { return "downstream"; }

            public void prepare() throws SQLException {
                stageRows(token, range.startOffset(), range.endOffset());
            }

            public void commit() throws SQLException {
                try (var connection = database.getConnection(); var publish = connection.prepareStatement(
                        "UPDATE cdc_downstream_rows SET published = TRUE WHERE attempt = ?")) {
                    publish.setString(1, token.id().toString());
                    assertThat((long) publish.executeUpdate()).isEqualTo(range.count());
                }
            }

            public void abort() {
                try (var connection = database.getConnection(); var discard = connection.prepareStatement(
                        "DELETE FROM cdc_downstream_rows WHERE attempt = ? AND NOT published")) {
                    discard.setString(1, token.id().toString());
                    discard.executeUpdate();
                } catch (SQLException failure) {
                    throw new IllegalStateException("Could not discard staged test rows", failure);
                }
            }
        }));
    }

    private static void stageRows(PrepareToken token, long first, long last) throws SQLException {
        try (var connection = database.getConnection(); PreparedStatement stage = connection.prepareStatement(
                "INSERT INTO cdc_downstream_rows (attempt, offset_value) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
            for (long offset = first; offset <= last; offset++) {
                stage.setString(1, token.id().toString());
                stage.setLong(2, offset);
                stage.addBatch();
            }
            stage.executeBatch();
        }
    }

    private static Set<Long> visibleOffsets() throws SQLException {
        try (var connection = database.getConnection(); var statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT offset_value FROM cdc_downstream_rows WHERE published")) {
            Set<Long> offsets = new HashSet<>();
            while (rows.next()) {
                assertThat(offsets.add(rows.getLong(1))).isTrue();
            }
            return offsets;
        }
    }
}
