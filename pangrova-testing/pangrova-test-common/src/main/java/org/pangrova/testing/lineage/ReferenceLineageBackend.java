package org.pangrova.testing.lineage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.JsonMapperFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Owned real OpenLineage reference server and its PostgreSQL journal; no replacement HTTP handlers. */
public final class ReferenceLineageBackend implements AutoCloseable {
    private static final long STARTUP_MINUTES = 2;
    private final Network network = Network.newNetwork();
    private final String password = UUID.randomUUID().toString();
    private final GenericContainer<?> database = new GenericContainer<>("postgres:16-alpine")
            .withNetwork(network).withNetworkAliases("lineage-postgres").withExposedPorts(5432)
            .withEnv("POSTGRES_DB", "lineage").withEnv("POSTGRES_USER", "lineage")
            .withEnv("POSTGRES_PASSWORD", password)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    private GenericContainer<?> server;
    private final ObjectMapper json = JsonMapperFactory.shared();
    private final String namespace = "pangrova-lineage-" + UUID.randomUUID();

    /** Starts actual server health checks; the caller additionally exercises the SDK ingestion it needs. */
    public ReferenceLineageBackend() {
        try {
            database.start();
            String configuration = """
                    server:
                      applicationConnectors:
                        - type: http
                          port: 8080
                      adminConnectors:
                        - type: http
                          port: 8081
                    db:
                      driverClass: org.postgresql.Driver
                      url: jdbc:postgresql://lineage-postgres:5432/lineage
                      user: lineage
                      password: %s
                    migrateOnStartup: true
                    search:
                      enabled: false
                    graphql:
                      enabled: false
                    logging:
                      level: WARN
                    """.formatted(password);
            String image = System.getProperty("pangrova.test.marquez.image", "marquezproject/marquez:0.50.0");
            server = new GenericContainer<>(DockerImageName.parse(image))
                    .withNetwork(network).withExposedPorts(8080, 8081)
                    .withEnv("MARQUEZ_CONFIG", "/opt/marquez/pangrova-test.yml")
                    .withEnv("JAVA_OPTS", "-Xmx384m")
                    .withCopyToContainer(Transferable.of(configuration), "/opt/marquez/pangrova-test.yml")
                    .waitingFor(Wait.forHttp("/healthcheck").forPort(8081).forStatusCode(200))
                    .withStartupTimeout(Duration.ofMinutes(STARTUP_MINUTES));
            server.start();
        } catch (RuntimeException | Error failure) {
            try {
                close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Returns the actual mapped HTTP API root. */
    public URI endpoint() {
        return URI.create("http://" + server.getHost() + ":" + server.getMappedPort(8080));
    }

    /** Returns the isolated job namespace for this reference server owner. */
    public String namespace() { return namespace; }

    /** Opens a real backend database connection, including for actual row-lock failure experiments. */
    public Connection connection() throws java.sql.SQLException {
        String jdbc = "jdbc:postgresql://" + database.getHost() + ":"
                + database.getMappedPort(5432) + "/lineage";
        return DriverManager.getConnection(jdbc, "lineage", password);
    }

    /** Reads actual persisted events for an exact run UUID rather than counting transport invocations. */
    public List<JsonNode> events(UUID runId) throws Exception {
        try (var connection = connection(); var query = connection.prepareStatement("""
                SELECT event::text FROM lineage_events
                WHERE run_uuid = ? AND _event_type = 'RUN_EVENT'
                ORDER BY event_time, CASE WHEN event_type = 'START' THEN 0 ELSE 1 END
                """)) {
            query.setObject(1, runId);
            try (var rows = query.executeQuery()) {
                List<JsonNode> events = new ArrayList<>();
                while (rows.next()) events.add(json.readTree(rows.getString(1)));
                return List.copyOf(events);
            }
        }
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        for (AutoCloseable resource : new AutoCloseable[]{server, database, network}) {
            if (resource == null) continue;
            try {
                resource.close();
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = new IllegalStateException("Reference lineage resource cleanup failed", cleanup);
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure != null) throw failure;
    }
}
