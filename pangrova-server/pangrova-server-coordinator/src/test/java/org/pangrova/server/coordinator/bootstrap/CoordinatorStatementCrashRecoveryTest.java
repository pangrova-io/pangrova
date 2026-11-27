package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.testinfra.SignedOidcIssuer;
import org.pangrova.persistence.jdbc.support.SchemaMigrationRunner;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.persistence.query.postgresql.state.JdbcQueryStateJournal;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;
import org.postgresql.ds.PGSimpleDataSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

class CoordinatorStatementCrashRecoveryTest {
    private static final String TENANT = "crash-slot";
    private static final String AUDIENCE = "bootstrap-tests";

    @TempDir
    Path tempDirectory;

    @Test
    void killedIndependentCoordinatorKeepsSlotUntilSameDomainPublishesExit() throws Exception {
        try (var issuer = new SignedOidcIssuer()) {
            var configuration = withAuthority(configuration("prior-coordinator", issuer.uri(), null));
            var jdbc = configuration.metadataJdbcConfig();
            var source = source(jdbc);
            SchemaMigrationRunner.migrate(source);
            createTenant(source);
            var originalDirectory = tempDirectory.resolve("original");
            var child = startChild(jdbc, issuer.uri(), originalDirectory);
            try {
                int port = awaitChild(child);
                var receipt = submit(issuer, port);
                assertThat(receipt.resultEndpoint()).isNotNull();
                String query = receipt.queryId();
                var anchors = new JdbcExecutionAnchorStore(source);
                var owner = anchors.current(query).orElseThrow();
                var stateConfig = new StateJournalConfig("JDBC", jdbc.jdbcUrl(), 20, 20, 24, 1024);
                try (var journal = new JdbcQueryStateJournal(source, stateConfig)) {
                    await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                            assertThat(journal.latestQueryEvent(query)).hasValueSatisfying(event -> {
                                assertThat(event.state()).isEqualTo("FINISHED");
                                assertThat(event.terminalOutcome().resultDisposition())
                                        .isEqualTo(ResultDisposition.UNAVAILABLE);
                            }));
                }
                assertThat(slots(source)).isOne();

                child.destroyForcibly();
                assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
                try (var connection = source.getConnection();
                     var expire = connection.prepareStatement("UPDATE execution_anchor "
                             + "SET expires_at=clock_timestamp(), version=version+1 WHERE execution_id=?")) {
                    expire.setString(1, query);
                    assertThat(expire.executeUpdate()).isOne();
                }

                var foreign = new CoordinatorBootstrap(configuration("foreign-coordinator", issuer.uri(), jdbc),
                        tempDirectory.resolve("foreign"));
                foreign.start();
                try {
                    assertThat(anchors.claimExpired("foreign-probe", "foreign-probe", Duration.ofSeconds(30), 10))
                            .isEmpty();
                    assertThat(slots(source)).isOne();
                    assertUnresolved(source, query, owner.ownerIncarnation());
                    assertSameDomainReleases(source, issuer.uri(), jdbc, originalDirectory, query);
                } finally {
                    foreign.shutdown();
                }
            } finally {
                if (child.isAlive()) child.destroyForcibly();
                assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static void assertSameDomainReleases(PGSimpleDataSource source, String issuer,
            CoordinatorBootstrapConfig.MetadataJdbcConfig jdbc, Path directory, String query) throws Exception {
        var restart = new CoordinatorBootstrap(configuration("replacement-coordinator", issuer, jdbc), directory);
        restart.start();
        try {
            await().atMost(Duration.ofSeconds(45)).until(() -> slots(source) == 0);
            try (var connection = source.getConnection();
                 var statement = connection.prepareStatement("SELECT 1 FROM execution_anchor WHERE execution_id=?")) {
                statement.setString(1, query);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isFalse();
                }
            }
        } finally {
            restart.shutdown();
        }
    }

    private static void assertUnresolved(PGSimpleDataSource source, String query, String incarnation) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                SELECT owner_incarnation, coordinator_work_incarnation, coordinator_work_domain, coordinator_work_exited
                FROM execution_anchor WHERE execution_id=?
                """)) {
            statement.setString(1, query);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo(incarnation);
                assertThat(rows.getString(2)).isEqualTo(incarnation);
                assertThat(rows.getString(3)).isNotBlank();
                assertThat(rows.getBoolean(4)).isFalse();
            }
        }
    }

    private Process startChild(CoordinatorBootstrapConfig.MetadataJdbcConfig jdbc, String issuer, Path directory)
            throws Exception {
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx512m", "-XX:MaxDirectMemorySize=256m", "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                OwnerProcess.class.getName(), issuer, directory.toString(), tempDirectory.resolve("ready").toString())
                .redirectErrorStream(true).redirectOutput(tempDirectory.resolve("owner.log").toFile());
        builder.environment().put("TEST_COORDINATOR_JDBC_URL", jdbc.jdbcUrl());
        builder.environment().put("TEST_COORDINATOR_JDBC_USER", jdbc.username());
        builder.environment().put("TEST_COORDINATOR_JDBC_PASSWORD", jdbc.password());
        return builder.start();
    }

    private int awaitChild(Process child) throws Exception {
        Path readiness = tempDirectory.resolve("ready");
        await().atMost(Duration.ofSeconds(35)).until(() ->
                (Files.exists(readiness) && Files.size(readiness) > 0) || !child.isAlive());
        assertThat(child.isAlive()).as("Coordinator child log at %s", tempDirectory.resolve("owner.log")).isTrue();
        return Integer.parseInt(Files.readString(readiness));
    }

    private static CoordinatorBootstrapConfig configuration(String id, String issuer,
            CoordinatorBootstrapConfig.MetadataJdbcConfig jdbc) {
        var core = new CoordinatorBootstrapConfig.CoreConfig(id, 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                CoordinatorDeploymentRole.QUERY, "127.0.0.1");
        var base = CoordinatorBootstrapConfig.FeatureFlags.allDisabled();
        var flags = new CoordinatorBootstrapConfig.FeatureFlags(false, jdbc != null, jdbc != null, false, false,
                false, true, false, false, false, false, false, false,
                base.shuffleMode(), List.of(), base.shuffleProvider(), Map.of());
        var security = new CoordinatorBootstrapConfig.SecurityConfig("OAUTH2", "crash-test-internal-service-secret",
                Map.of("issuerUri", issuer, "audience", AUDIENCE));
        return new CoordinatorBootstrapConfig(core, flags, CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), jdbc, security, null);
    }

    private static PGSimpleDataSource source(CoordinatorBootstrapConfig.MetadataJdbcConfig jdbc) {
        var source = new PGSimpleDataSource();
        source.setURL(jdbc.jdbcUrl());
        source.setUser(jdbc.username());
        source.setPassword(jdbc.password());
        return source;
    }

    private static void createTenant(PGSimpleDataSource source) {
        var store = new JdbcTenantStore(source);
        var quota = new ResourceQuota(1L << 30, 1, 100, 60_000, 1L << 30, 100, 1L << 40, 1L << 40);
        store.createTenant(new Tenant(TENANT, "Crash recovery", TenantStatus.ACTIVE, quota,
                List.of(), List.of(), null, Map.of(), Instant.EPOCH, "test"));
        store.addUserToTenant(TENANT, "alice", TenantRole.MEMBER);
    }

    private static int slots(PGSimpleDataSource source) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                SELECT s.active_count, (SELECT COUNT(*) FROM tenant_concurrency_queries q
                    WHERE q.tenant_id=s.tenant_id AND q.execution_bound)
                FROM tenant_concurrency_slots s WHERE s.tenant_id=?
                """)) {
            statement.setString(1, TENANT);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(rows.getInt(2));
                return rows.getInt(1);
            }
        }
    }

    private static SubmitQueryResponse submit(SignedOidcIssuer issuer, int port) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/query/submit"))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + issuer.token(AUDIENCE, true))
                .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SHOW DYNAMIC TABLES\"}")).build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
        return JsonMapperFactory.create().readValue(response.body(), SubmitQueryResponse.class);
    }

    public static final class OwnerProcess {
        private OwnerProcess() {}

        public static void main(String[] args) throws Exception {
            var jdbc = new CoordinatorBootstrapConfig.MetadataJdbcConfig(System.getenv("TEST_COORDINATOR_JDBC_URL"),
                    System.getenv("TEST_COORDINATOR_JDBC_USER"), System.getenv("TEST_COORDINATOR_JDBC_PASSWORD"));
            var coordinator = new CoordinatorBootstrap(configuration("prior-coordinator", args[0], jdbc),
                    Path.of(args[1]));
            coordinator.start();
            Files.writeString(Path.of(args[2]), Integer.toString(coordinator.getHttpPort()));
            coordinator.awaitShutdown();
        }
    }
}
