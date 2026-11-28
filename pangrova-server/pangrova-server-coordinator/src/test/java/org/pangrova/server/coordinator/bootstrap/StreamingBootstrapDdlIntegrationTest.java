package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.CreateStreamStatement;
import org.pangrova.persistence.streaming.postgresql.JdbcRunningStreamFeed;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class StreamingBootstrapDdlIntegrationTest extends ContainerTestBase {
    private static final String TENANT = "tenant";
    private static final String STREAM = "orders";

    @Test
    void createWritesOneCompleteRunningDefinition() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("stream_create_definition");
        try (var connection = source.getConnection(); var sql = connection.createStatement()) {
            sql.execute("CREATE TABLE stream_ddl_writes (operation text, state text)");
            sql.execute("""
                    CREATE FUNCTION audit_stream_ddl_write() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        INSERT INTO stream_ddl_writes VALUES (TG_OP, NEW.lifecycle_state);
                        RETURN NEW;
                    END;
                    $$
                    """);
            sql.execute("""
                    CREATE TRIGGER audit_stream_ddl_write
                    AFTER INSERT OR UPDATE ON ingestion_stream_state
                    FOR EACH ROW EXECUTE FUNCTION audit_stream_ddl_write()
                    """);
        }
        var streaming = new StreamingBootstrap(source);

        streaming.getDdlExecutor().create(createStatement(), TENANT, "creator");

        var durable = streaming.stateStore().loadState(STREAM).orElseThrow();
        assertThat(durable.lifecycleState()).isEqualTo(StreamLifecycleState.RUNNING);
        assertThat(durable.sourceProvider()).isEqualTo("kafka");
        assertThat(durable.sourceFormat()).isEqualTo("debezium-json");
        assertThat(durable.targetTable()).isEqualTo("catalog.db.orders");
        assertThat(durable.creatorIdentity()).isEqualTo("creator");
        try (var connection = source.getConnection(); var sql = connection.createStatement();
             var writes = sql.executeQuery("SELECT operation, state FROM stream_ddl_writes")) {
            assertThat(writes.next()).isTrue();
            assertThat(writes.getString("operation")).isEqualTo("INSERT");
            assertThat(writes.getString("state")).isEqualTo("RUNNING");
            assertThat(writes.next()).isFalse();
        }
    }

    @Test
    void anotherCoordinatorCanFenceAndResumeTheSameCheckpoint() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("stream_ddl_authority");
        var owner = new StreamingBootstrap(source);
        var ddlCoordinator = new StreamingBootstrap(source);
        var ddl = ddlCoordinator.getDdlExecutor();
        ddl.create(createStatement(), TENANT, "creator");
        var original = owner.stateStore().loadState(STREAM).orElseThrow();
        var partition = new TopicPartitionOffset(STREAM, 0);
        owner.offsetStore().acknowledge(original, Map.of(partition, 2L));
        assertThat(owner.stateStore().createState(original.toBuilder()
                .streamName("paused_orders").checkpointId("paused_orders_checkpoint")
                .lifecycleState(StreamLifecycleState.SUSPENDED).build())).isTrue();

        try (var feed = startFeed(source, "cdc_ddl_authority")) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(feed.snapshot()).containsOnlyKeys(STREAM));

            ddl.alter(new AlterStreamStatement(Identifier.of(STREAM), new AlterStreamAction.Suspend()), TENANT);
            await().atMost(Duration.ofSeconds(10)).until(() -> feed.snapshot().isEmpty());
            assertThatThrownBy(() -> owner.offsetStore().acknowledge(original, Map.of(partition, 3L)))
                    .isInstanceOf(StaleEpochException.class);

            ddl.alter(new AlterStreamStatement(Identifier.of(STREAM), new AlterStreamAction.Resume()), TENANT);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(feed.snapshot()).containsKey(STREAM));
            var resumed = feed.snapshot().get(STREAM);
            assertThat(resumed.checkpointId()).isEqualTo(original.checkpointId());
            assertThat(resumed.epoch()).isEqualTo(original.epoch() + 2);
            assertThat(owner.offsetStore().loadOffsets(STREAM)).containsExactlyEntriesOf(Map.of(partition, 2L));
            owner.offsetStore().acknowledge(resumed, Map.of(partition, 3L));
            assertThat(owner.offsetStore().loadOffsets(STREAM)).containsExactlyEntriesOf(Map.of(partition, 3L));

            assertThat(ddlCoordinator.stateStore().delete(resumed)).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(() -> feed.snapshot().isEmpty());
        }
    }

    @Test
    void reconnectRebuildsFromCommittedDefinitions() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("stream_feed_reconnect");
        var ddl = new StreamingBootstrap(source).getDdlExecutor();
        ddl.create(createStatement(), TENANT, "creator");
        String application = "cdc_feed_reconnect";
        try (var feed = startFeed(source, application)) {
            await().atMost(Duration.ofSeconds(10)).until(() -> feed.snapshot().containsKey(STREAM));
            int predecessor = listenerPid(source, application).orElseThrow();
            try (var connection = source.getConnection(); var sql = connection.createStatement()) {
                connection.setAutoCommit(false);
                sql.execute("LOCK TABLE ingestion_stream_state IN ACCESS EXCLUSIVE MODE");
                assertThat(sql.executeUpdate("UPDATE ingestion_stream_state SET epoch = epoch + 1")).isOne();
                try (var admin = source.getConnection();
                     var terminate = admin.prepareStatement("SELECT pg_terminate_backend(?)")) {
                    terminate.setInt(1, predecessor);
                    try (var result = terminate.executeQuery()) {
                        assertThat(result.next()).isTrue();
                        assertThat(result.getBoolean(1)).isTrue();
                    }
                }
                await().atMost(Duration.ofSeconds(10)).until(() ->
                        listenerPid(source, application).stream().noneMatch(pid -> pid == predecessor));
                connection.commit();
            }
            await().atMost(Duration.ofSeconds(15)).until(() ->
                    listenerPid(source, application).stream().anyMatch(pid -> pid != predecessor));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(feed.snapshot()).hasEntrySatisfying(STREAM,
                            state -> assertThat(state.epoch()).isEqualTo(1)));
        }
    }

    private static JdbcRunningStreamFeed startFeed(DataSource source, String application) throws SQLException {
        var jdbc = source.unwrap(PGSimpleDataSource.class);
        String url = jdbc.getURL();
        String separator = url.contains("?") ? "&" : "?";
        var feed = new JdbcRunningStreamFeed(url + separator + "ApplicationName=" + application,
                jdbc.getUser(), jdbc.getPassword());
        feed.start();
        feed.setActive(true);
        return feed;
    }

    private static OptionalInt listenerPid(DataSource source, String application) throws SQLException {
        try (var connection = source.getConnection();
             var statement = connection.prepareStatement("SELECT pid FROM pg_stat_activity WHERE application_name = ?")) {
            statement.setString(1, application);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? OptionalInt.of(rows.getInt(1)) : OptionalInt.empty();
            }
        }
    }

    private static CreateStreamStatement createStatement() {
        return new CreateStreamStatement(Identifier.of(STREAM), false, "kafka",
                Map.of("bootstrap.servers", "unused:9092", "topic", STREAM), "debezium-json",
                Identifier.of("catalog", "db", STREAM), Map.of());
    }
}
