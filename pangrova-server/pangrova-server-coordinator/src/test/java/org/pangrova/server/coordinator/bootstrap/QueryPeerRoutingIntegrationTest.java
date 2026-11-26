package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.query.QueryOwnerEndpoint;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.coordinator.testinfra.SignedOidcIssuer;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcCoordinatorRegistry;
import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.transport.client.FlightResultFetcher;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

class QueryPeerRoutingIntegrationTest extends ContainerTestBase {

    private static final String WORKER_TOKEN = "test-worker-shared-service-credential";
    private static final String PEER_KEY = "test-query-peer-signing-credential-distinct-from-workers";
    private static final String AUDIENCE = "query-peer-test";

    @Test
    void authenticatedIngressRelaysStatusProfileAndCancellationToTheExactOwner() throws Exception {
        try (var issuer = new SignedOidcIssuer()) {
            var config = config(issuer);
            var owner = new CoordinatorBootstrap(config);
            owner.start();
            try {
                var ingress = new CoordinatorBootstrap(new CoordinatorBootstrapConfig(
                        core("query-ingress"), config.featureFlags(), config.query(), config.transport(),
                        config.workerHealth(), config.metadataJdbcConfig(), config.securityConfig(),
                        config.observabilityBootstrapConfig()));
                ingress.start();
                try {
                    var http = HttpClient.newHttpClient();
                    String alice = "Bearer " + issuer.token(AUDIENCE, true, "alice");
                    String bob = "Bearer " + issuer.token(AUDIENCE, true, "bob");
                    exerciseWaitsAndProfiles(http, owner, ingress.getHttpPort(), alice, bob);
                    verifyActiveCancellation(http, owner, ingress.getHttpPort(), alice);
                } finally {
                    ingress.shutdown();
                }
            } finally {
                owner.shutdown();
            }
        }
    }

    @Test
    void exactReceiptRecoversAfterTheKnownOwnerStopsAndPhysicalDrainCommits() throws Exception {
        try (var issuer = new SignedOidcIssuer()) {
            var config = config(issuer);
            var owner = new CoordinatorBootstrap(config);
            owner.start();
            var ingress = new CoordinatorBootstrap(new CoordinatorBootstrapConfig(
                    core("query-ingress"), config.featureFlags(), config.query(), config.transport(),
                    config.workerHealth(), config.metadataJdbcConfig(), config.securityConfig(),
                    config.observabilityBootstrapConfig()));
            try {
                ingress.start();
                enableCounters(ingress);
                var http = HttpClient.newHttpClient();
                String alice = "Bearer " + issuer.token(AUDIENCE, true, "alice");
                String bob = "Bearer " + issuer.token(AUDIENCE, true, "bob");
                int ingressPort = ingress.getHttpPort();
                SubmitQueryResponse receipt;
                try (var barrier = lockPublication(owner)) {
                    receipt = submit(http, owner.getHttpPort(), "recover-original-receipt", alice);
                    String path = recovery(receipt);
                    String ticket = receipt.ownerEndpoint().routeTicket();
                    long before = lookupCalls(ingress);
                    assertThat(read(http, ingressPort, path, bob, ticket).statusCode()).isEqualTo(404);
                    assertThat(read(http, ingressPort, path, alice, ticket + "x").statusCode()).isEqualTo(404);
                    assertThat(lookupCalls(ingress) - before).isZero();
                    var pending = read(http, ingressPort, path, alice, ticket);
                    assertThat(pending.statusCode()).as("%s", pending.body()).isEqualTo(503);
                    assertThat(pending.body()).contains("QUERY_RECOVERY_PENDING");
                    assertThat(lookupCalls(ingress) - before).isEqualTo(1);
                    barrier.commit();
                }

                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                    var state = owner.getQueryManager().getStateMachine(receipt.queryId()).status();
                    assertThat(state.endpoint()).isNotNull();
                });
                var stateMachine = owner.getQueryManager().getStateMachine(receipt.queryId());
                try (var fetcher = new FlightResultFetcher();
                     var cursor = fetcher.open(stateMachine.status().endpoint(), System.currentTimeMillis() + 5_000)) {
                    assertThat(cursor.columns()).isNotEmpty();
                    assertThat(cursor.next()).isFalse();
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(8)).until(
                        () -> stateMachine.status().drained());
                owner.shutdown();

                String ticket = receipt.ownerEndpoint().routeTicket();
                long before = lookupCalls(ingress);
                var live = read(http, ingressPort, status(receipt) + "?afterVersion=0", alice, ticket);
                assertThat(live.statusCode()).as("%s", live.body()).isEqualTo(503);
                assertThat(live.body()).contains("QUERY_OWNER_UNAVAILABLE");
                assertThat(lookupCalls(ingress) - before).isZero();
                var reply = read(http, ingressPort, recovery(receipt), alice, ticket);
                assertThat(reply.statusCode()).as("%s", reply.body()).isEqualTo(200);
                var terminal = JsonMapperFactory.shared().readValue(reply.body(), QueryStatusResponse.class);
                assertThat(terminal.queryId()).isEqualTo(receipt.queryId());
                assertThat(terminal.state()).isEqualTo("FINISHED");
                assertThat(terminal.recovered()).isTrue();
                assertThat(terminal.resultDisposition()).isEqualTo(ResultDisposition.UNAVAILABLE);
                assertThat(lookupCalls(ingress) - before).isEqualTo(1);
            } finally {
                try {
                    ingress.shutdown();
                } finally {
                    owner.shutdown();
                }
            }
        }
    }

    private static String recovery(SubmitQueryResponse receipt) {
        return "/v1/query/" + receipt.queryId() + "/receipt-recovery";
    }

    private static HttpResponse<String> read(HttpClient http, int port, String path, String auth, String ticket)
            throws Exception {
        return http.send(control(port, path, auth, ticket).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void exerciseWaitsAndProfiles(HttpClient http, CoordinatorBootstrap owner, int ingressPort,
            String alice, String bob) throws Exception {
        enableCounters(owner);
        long fencedWrites = fencedAcceptanceWrites(owner);
        SubmitQueryResponse receipt;
        try (var barrier = lockPublication(owner)) {
            receipt = submit(http, owner.getHttpPort(), "peer-observe", alice);
            assertThat(fencedAcceptanceWrites(owner) - fencedWrites).isEqualTo(1);
            String observer = UUID.randomUUID().toString();
            String path = status(receipt) + "?afterVersion=0&waitMs=5000&until=RESULT_OR_TERMINAL";
            var wait = control(ingressPort, path, alice, receipt.ownerEndpoint().routeTicket())
                    .header("X-Pangrova-Query-Observer", observer).GET().build();
            var first = http.sendAsync(wait, HttpResponse.BodyHandlers.ofString());
            var second = http.sendAsync(wait, HttpResponse.BodyHandlers.ofString());
            var duplicate = (HttpResponse<?>) CompletableFuture.anyOf(first, second).get(4, TimeUnit.SECONDS);
            assertThat(duplicate.statusCode()).as("%s", duplicate.body()).isEqualTo(409);
            assertThat(duplicate.body().toString()).contains("QUERY_STATUS_WAIT_EXISTS");
            var admitted = first.isDone() && first.join().statusCode() == 409 ? second : first;
            barrier.commit();
            var ready = admitted.get(8, TimeUnit.SECONDS);
            assertThat(ready.statusCode()).as("%s", ready.body()).isEqualTo(200);
            var status = JsonMapperFactory.shared().readValue(ready.body(), QueryStatusResponse.class);
            assertThat(status.queryId()).isEqualTo(receipt.queryId());
            try (var fetcher = new FlightResultFetcher();
                 var cursor = fetcher.open(status.resultEndpoint(), System.currentTimeMillis() + 5_000)) {
                assertThat(cursor.columns()).isNotEmpty();
                assertThat(cursor.next()).isFalse();
            }
        }

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(
                () -> owner.getQueryManager().getStateMachine(receipt.queryId()).status().drained());
        var registry = new JdbcCoordinatorRegistry(owner.metadataDataSource());
        var registered = registry.getCoordinator(receipt.ownerEndpoint().coordinatorId()).orElseThrow();
        assertThat(registered.httpPort()).isEqualTo(owner.getHttpPort()).isEqualTo(receipt.ownerEndpoint().port());
        assertThat(registered.incarnation()).isEqualTo(receipt.ownerEndpoint().incarnation());
        var peer = registry.getCoordinator("query-ingress").orElseThrow();
        assertThat(peer.httpPort()).isEqualTo(ingressPort);
        assertThat(peer.incarnation()).isNotBlank().isNotEqualTo(registered.incarnation());
        assertThat(submit(http, ingressPort, "peer-observe", alice)).isEqualTo(receipt);
        assertReadBudget(http, owner, ingressPort, alice, receipt);
        assertUnauthorizedRoutes(http, ingressPort, alice, bob, receipt);
    }

    private static void assertReadBudget(HttpClient http, CoordinatorBootstrap owner, int ingressPort,
            String alice, SubmitQueryResponse receipt) throws Exception {
        long before = lookupCalls(owner);
        String ticket = receipt.ownerEndpoint().routeTicket();
        var requests = IntStream.range(0, 32).mapToObj(index -> {
            String path = switch (index % 3) {
                case 0 -> status(receipt);
                case 1 -> "/v1/query/profile/" + receipt.queryId() + "/json";
                default -> "/api/queries/" + receipt.queryId() + "/profile";
            };
            return http.sendAsync(control(ingressPort, path, alice, ticket).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
        }).toList();
        for (var request : requests) {
            var reply = request.get(8, TimeUnit.SECONDS);
            assertThat(reply.statusCode()).as("%s", reply.body()).isEqualTo(200);
            assertThat(reply.body()).contains(receipt.queryId());
        }
        assertThat(lookupCalls(owner) - before).isZero();
    }

    private static void assertUnauthorizedRoutes(HttpClient http, int ingressPort, String alice, String bob,
            SubmitQueryResponse receipt) throws Exception {
        String ticket = receipt.ownerEndpoint().routeTicket();
        assertThat(http.send(control(ingressPort, status(receipt), bob, ticket).GET().build(),
                HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(404);
        String corrupted = (ticket.charAt(0) == 'A' ? "B" : "A") + ticket.substring(1);
        assertThat(http.send(control(ingressPort, status(receipt), alice, corrupted).GET().build(),
                HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(404);
        assertThat(http.send(control(ingressPort, status(receipt), null, ticket).GET().build(),
                HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(401);
        var worker = control(ingressPort, "/v1/internal/query-peer", "Bearer " + WORKER_TOKEN, null)
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{}")).build();
        assertThat(http.send(worker, HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(403);
    }

    private static void verifyActiveCancellation(HttpClient http, CoordinatorBootstrap owner, int ingressPort,
            String alice) throws Exception {
        try (var barrier = lockPublication(owner)) {
            var receipt = submit(http, owner.getHttpPort(), "peer-cancel", alice);
            var cancel = http.send(control(ingressPort, "/v1/query/" + receipt.queryId() + "/cancel",
                            alice, receipt.ownerEndpoint().routeTicket()).POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(cancel.statusCode()).as("%s", cancel.body()).isEqualTo(202);
            assertThat(cancel.body()).contains("CANCEL_ACCEPTED");
            barrier.commit();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                var status = owner.getQueryManager().getStateMachine(receipt.queryId()).status();
                assertThat(status.state().name()).isEqualTo("CANCELLED");
                assertThat(status.drained()).isTrue();
            });
        }
    }

    private static SubmitQueryResponse submit(HttpClient http, int ownerPort, String key, String authorization)
            throws Exception {
        var request = control(ownerPort, "/v1/query/submit", authorization, null)
                .header("Idempotency-Key", key).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SHOW DYNAMIC TABLES\"}")).build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
        var receipt = JsonMapperFactory.shared().readValue(response.body(), SubmitQueryResponse.class);
        assertThat(receipt.ownerEndpoint().routeTicket()).isNotBlank();
        return receipt;
    }

    private static String status(SubmitQueryResponse receipt) {
        return "/v1/query/" + receipt.queryId() + "/state";
    }

    private static HttpRequest.Builder control(int port, String path, String auth, String ticket) {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10));
        if (auth != null) request.header("Authorization", auth);
        if (ticket != null) request.header(QueryOwnerEndpoint.ROUTE_HEADER, ticket);
        return request;
    }

    private static Connection lockPublication(CoordinatorBootstrap owner) throws Exception {
        var connection = owner.metadataDataSource().getConnection();
        try (var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("LOCK TABLE materialization.objects IN ACCESS EXCLUSIVE MODE");
            return connection;
        } catch (Exception failure) {
            connection.close();
            throw failure;
        }
    }

    private static void enableCounters(CoordinatorBootstrap owner) throws Exception {
        try (var connection = owner.metadataDataSource().getConnection();
             var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public");
        }
    }

    private static long fencedAcceptanceWrites(CoordinatorBootstrap owner) throws Exception {
        try (var connection = owner.metadataDataSource().getConnection();
             var statement = connection.createStatement();
             var rows = statement.executeQuery("""
                     SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                       AND query LIKE 'WITH locked AS MATERIALIZED (%INSERT INTO query_state_journal%'
                     """)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private static long lookupCalls(CoordinatorBootstrap owner) throws Exception {
        try (var connection = owner.metadataDataSource().getConnection();
             var statement = connection.prepareStatement("""
                     SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                       AND (query LIKE ? OR query LIKE ? OR query LIKE ?)
                     """)) {
            statement.setString(1, "SELECT%FROM query_state_journal%");
            statement.setString(2, "SELECT%FROM query_audit_records%");
            statement.setString(3, "SELECT%FROM coordinator_registry%");
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }

    private static CoordinatorBootstrapConfig config(SignedOidcIssuer issuer) {
        var security = SecurityConfigurationLoader.load(PangrovaConfiguration.fromYaml(Map.of(
                "security.enabled", "true",
                "security.auth.type", "OAUTH2",
                "security.auth.properties.issuer-uri", issuer.uri(),
                "security.auth.properties.audience", AUDIENCE,
                "coordinator.internal-auth-secret", WORKER_TOKEN,
                "coordinator.query-peer-key", PEER_KEY)));
        return withAuthority(new CoordinatorBootstrapConfig(core("query-owner"),
                CoordinatorBootstrapConfig.FeatureFlags.allDisabled(), CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(), CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, security, null));
    }

    private static CoordinatorBootstrapConfig.CoreConfig core(String id) {
        return new CoordinatorBootstrapConfig.CoreConfig(id, 0, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                CoordinatorDeploymentRole.QUERY, "127.0.0.1");
    }
}
