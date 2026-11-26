package org.pangrova.server.coordinator.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.jdbc.support.SchemaMigrationRunner;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.persistence.query.postgresql.state.JdbcQueryStateJournal;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskIssuance;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.protocol.checksum.ProtocolChecksums;
import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.WorkerExecutionIdentity;
import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;
import org.pangrova.transport.client.ColumnDescriptor;
import org.pangrova.transport.client.FlightResultFetcher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Verifies role-based coordinator deployment: a QUERY-only coordinator
 * starts HTTP + QueryManager but does NOT start indexing or streaming subsystems.
 */
@DisplayName("Role-Based Coordinator Deployment")
class RoleBasedDeploymentTest {
    private static final String PROFILE_LOOKUP = "SELECT%FROM query_audit_records WHERE query_id =%AND user_id =%";
    private static final String JOURNAL_LOOKUP = "SELECT%FROM query_state_journal%";

    private CoordinatorBootstrap bootstrap;

    @Test
    void queryRecoveryReleasesAnEmptyClaimAndRetainsTerminalOrTaskEvidence() throws Exception {
        var configuration = withAuthority(queryRole("recovery-successor"));
        var jdbc = configuration.metadataJdbcConfig();
        var source = new PGSimpleDataSource();
        source.setURL(jdbc.jdbcUrl());
        source.setUser(jdbc.username());
        source.setPassword(jdbc.password());
        SchemaMigrationRunner.migrate(source);

        var terminal = "failed-before-drain";
        var taskOnly = "task-without-query-projection";
        var empty = "before-admission";
        var priorContext = JournalEventContext.create(System.currentTimeMillis(), "prior-coordinator");
        var journalConfig = new StateJournalConfig("JDBC", jdbc.jdbcUrl(), 20, 20, 24, 1024);
        try (var journal = new JdbcQueryStateJournal(source, journalConfig)) {
            journal.appendQueryEventSync(new QueryStateEvent(priorContext, terminal, null, null, "FAILED"));
            journal.appendTaskEventSync(new TaskStateEvent(priorContext, taskOnly,
                    "unresolved-task", "prior-worker", "RUNNING", 0, 1));
        }
        var anchors = new JdbcExecutionAnchorStore(source);
        var terminalClaim = expireAnchor(anchors, terminal);
        var taskClaim = expireAnchor(anchors, taskOnly);
        expireAnchor(anchors, empty);

        bootstrap = new CoordinatorBootstrap(configuration);
        bootstrap.start();

        assertThat(anchors.current(terminal)).hasValueSatisfying(anchor -> {
            assertThat(anchor.ownerId()).isEqualTo("recovery-successor");
            assertThat(anchor.fencingToken()).isGreaterThan(terminalClaim);
        });
        assertThat(anchors.current(taskOnly)).hasValueSatisfying(anchor -> {
            assertThat(anchor.ownerId()).isEqualTo("recovery-successor");
            assertThat(anchor.fencingToken()).isGreaterThan(taskClaim);
        });
        try (var connection = source.getConnection(); var statement = connection.prepareStatement(
                "SELECT execution_id FROM execution_anchor WHERE execution_id = ?")) {
            statement.setString(1, empty);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isFalse();
            }
        }
    }

    @Test
    void queryRoleRecoversACommittedSealAndPersistsTheMatchingExitReceipt() throws Exception {
        var configuration = withAuthority(queryRole("task-drain-successor"));
        var jdbc = configuration.metadataJdbcConfig();
        var source = new PGSimpleDataSource();
        source.setURL(jdbc.jdbcUrl());
        source.setUser(jdbc.username());
        source.setPassword(jdbc.password());
        SchemaMigrationRunner.migrate(source);
        var anchors = new JdbcExecutionAnchorStore(source);
        var prior = anchors.tryAcquire("terminal-with-issuance", "prior", "prior-incarnation", Duration.ofMinutes(1))
                .orElseThrow();
        var seal = recoverySeal(prior);
        var receipt = CanonicalTaskAdmissionSeals.closedWithoutOwner(seal).toByteArray();
        var delivered = new CompletableFuture<TaskAdmissionSealRequest>();
        var peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peer.createContext("/v1/task/recovery-attempt", exchange -> {
            try (exchange) {
                if (!"DELETE".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                delivered.complete(TaskAdmissionSealRequest.parseFrom(exchange.getRequestBody().readAllBytes()));
                exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf");
                exchange.sendResponseHeaders(200, receipt.length);
                exchange.getResponseBody().write(receipt);
            }
        });
        peer.start();
        try {
            var context = JournalEventContext.create(System.currentTimeMillis(), prior.ownerId());
            var issuance = new TaskIssuance("127.0.0.1", peer.getAddress().getPort(), prior.ownerIncarnation(),
                    prior.fencingToken(), seal.toByteArray());
            var journalConfig = new StateJournalConfig("JDBC", jdbc.jdbcUrl(), 20, 20, 24, 1024);
            try (var journal = new JdbcQueryStateJournal(source, journalConfig)) {
                journal.appendQueryEventSync(new QueryStateEvent(context, prior.executionId(), null, null, "FAILED"));
                journal.appendTaskIssuancesSync(java.util.List.of(new TaskStateEvent(
                        JournalEventContext.create(context.timestampMs(), prior.ownerId()), prior.executionId(),
                        "recovery-attempt", "prior-worker", "SCHEDULED", 0, 0, issuance)));
            }
            assertThat(anchors.yield(prior)).isTrue();
            bootstrap = new CoordinatorBootstrap(configuration);
            bootstrap.start();

            assertThat(delivered.get(10, TimeUnit.SECONDS)).isEqualTo(seal);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(retainedWorkerReceipt(source, prior.executionId())).isEqualTo(receipt));
            assertThat(anchors.current(prior.executionId())).hasValueSatisfying(anchor ->
                    assertThat(anchor.ownerId()).isEqualTo("task-drain-successor"));
        } finally {
            peer.stop(0);
        }
    }

    private static byte[] retainedWorkerReceipt(PGSimpleDataSource source, String queryId) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement(
                "SELECT recovery_drain_receipt FROM task_state_journal WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? rows.getBytes(1) : null;
            }
        }
    }

    private static TaskAdmissionSealRequest recoverySeal(ExecutionAnchor prior) {
        var owner = ExecutionOwnerIdentity.newBuilder().setOwnerId(prior.ownerId())
                .setOwnerIncarnation(prior.ownerIncarnation()).setFencingToken(prior.fencingToken());
        var worker = WorkerExecutionIdentity.newBuilder().setAdmissionDomainId(UUID.randomUUID().toString())
                .setIncarnationId(UUID.randomUUID().toString());
        return TaskAdmissionSealRequest.newBuilder().setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(TaskAttemptId.newBuilder().setTaskId("recovery-attempt")
                        .setQueryOrJobId(prior.executionId()).setPlanEpoch(1).setFencingToken(1)
                        .setControlEpoch(1).setExecutionOwner(owner).setWorkerExecutionIdentity(worker))
                .setDeadlineEpochMs(prior.expiresAt().toEpochMilli())
                .setCanonicalRequestChecksum(ProtocolChecksums.sha256(new byte[] {1})).build();
    }

    private static long expireAnchor(JdbcExecutionAnchorStore anchors, String queryId) {
        var anchor = anchors.tryAcquire(queryId, "prior-coordinator", "prior-incarnation", Duration.ofMinutes(1))
                .orElseThrow();
        assertThat(anchors.yield(anchor)).isTrue();
        return anchor.fencingToken();
    }

    @AfterEach
    void tearDown() {
        if (bootstrap != null && bootstrap.isStarted()) {
            bootstrap.shutdown();
        }
    }

    @Test
    @DisplayName("QUERY-only coordinator starts HTTP and QueryManager")
    void queryOnlyCoordinatorStartsQueryPath() throws Exception {
        bootstrap = new CoordinatorBootstrap(withAuthority(queryRole("query-coord-1")));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();

        var receipt = submitShow(HttpClient.newHttpClient(), bootstrap.getHttpPort(), null);
        assertThat(receipt.ownerEndpoint().port()).isEqualTo(bootstrap.getHttpPort());
        assertThat(receipt.resultDisposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(receipt.resultEndpoint()).isNotNull();
        try (var fetcher = new FlightResultFetcher();
             var cursor = fetcher.open(receipt.resultEndpoint(), System.currentTimeMillis() + 5_000)) {
            assertThat(cursor.columns()).extracting(ColumnDescriptor::name).contains("dt_name", "schedule_state");
            assertThat(cursor.next()).isFalse();
        }
        assertProfileReadBudget(receipt.queryId());
    }

    @Test
    void durableReplayUsesTheOriginalCoordinatorIncarnationAndBoundPort() throws Exception {
        var config = withAuthority(queryRole("route-owner"));
        bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();
        var peer = new CoordinatorBootstrap(new CoordinatorBootstrapConfig(
                queryRole("route-peer").core(), config.featureFlags(), config.query(), config.transport(),
                config.workerHealth(), config.metadataJdbcConfig(), config.securityConfig(),
                config.observabilityBootstrapConfig()));
        peer.start();
        try {
            var client = HttpClient.newHttpClient();
            var issued = submitShow(client, bootstrap.getHttpPort(), "shared-request");
            var replay = submitShow(client, peer.getHttpPort(), "shared-request");
            assertThat(replay).isEqualTo(issued);
            var owner = replay.ownerEndpoint();
            assertThat(owner.coordinatorId()).isEqualTo("route-owner");
            assertThat(owner.incarnation()).isNotBlank();
            assertThat(owner.port()).isEqualTo(bootstrap.getHttpPort()).isNotEqualTo(peer.getHttpPort());
            var uri = owner.origin("http").resolve(
                    "/v1/query/" + replay.queryId() + "/state?afterVersion=0&until=RESULT_OR_TERMINAL&waitMs=5000");
            var request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
                    .header("X-Pangrova-Query-Observer", java.util.UUID.randomUUID().toString()).GET().build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
            var status = JsonMapperFactory.create().readValue(response.body(), QueryStatusResponse.class);
            assertThat(status.queryId()).isEqualTo(issued.queryId());
            assertThat(status.resultDisposition()).isEqualTo(ResultDisposition.STREAM);
            try (var fetcher = new FlightResultFetcher();
                 var cursor = fetcher.open(status.resultEndpoint(), System.currentTimeMillis() + 5_000)) {
                assertThat(cursor.next()).isFalse();
            }
            assertThat(peer.getQueryManager().getStateMachine(issued.queryId())).isNull();
        } finally {
            peer.shutdown();
        }
    }

    private static CoordinatorBootstrapConfig queryRole(String coordinatorId) {
        return new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(coordinatorId, 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, CoordinatorDeploymentRole.QUERY,
                        "0.0.0.0"),
                CoordinatorBootstrapConfig.FeatureFlags.allDisabled(), CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(), CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);
    }

    private static SubmitQueryResponse submitShow(HttpClient client, int port, String idempotencyKey) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/query/submit"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SHOW DYNAMIC TABLES\"}"));
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        var response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
        return JsonMapperFactory.create().readValue(response.body(), SubmitQueryResponse.class);
    }

    private void assertProfileReadBudget(String queryId) throws Exception {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(
                () -> bootstrap.getQueryManager().getStateMachine(queryId).status().drained());
        try (var connection = bootstrap.metadataDataSource().getConnection();
             var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public");
        }
        long before = postgresCalls(PROFILE_LOOKUP);
        String direct = "/v1/query/profile/" + queryId + "/json";
        String dashboard = "/api/queries/" + queryId + "/profile";
        var client = HttpClient.newHttpClient();
        var local = queryProfile(client.send(getRequest(direct), HttpResponse.BodyHandlers.ofString()));
        assertThat(local.path("queryId").asText()).isEqualTo(queryId);
        assertThat(local.path("sql").asText()).isEqualTo("SHOW DYNAMIC TABLES");
        var requests = IntStream.range(0, 32)
                .mapToObj(index -> client.sendAsync(getRequest(index % 2 == 0 ? direct : dashboard),
                        HttpResponse.BodyHandlers.ofString()))
                .toList();
        for (var request : requests) {
            assertThat(queryProfile(request.get(5, TimeUnit.SECONDS))).isEqualTo(local);
        }
        assertThat(postgresCalls(PROFILE_LOOKUP) - before).isZero();

        assertThat(bootstrap.getQueryManager().cleanupTerminalQueries(Duration.ZERO)).isOne();
        long journalBefore = postgresCalls(JOURNAL_LOOKUP);
        assertThat(client.send(getRequest(direct), HttpResponse.BodyHandlers.discarding()).statusCode())
                .isEqualTo(404);
        assertThat(client.send(getRequest("/v1/query/" + queryId + "/state"),
                HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(404);
        assertThat(postgresCalls(JOURNAL_LOOKUP) - journalBefore).isZero();
        assertThat(postgresCalls(PROFILE_LOOKUP) - before).isZero();
        var history = client.send(getRequest(direct + "?history=true"), HttpResponse.BodyHandlers.ofString());
        assertThat(queryProfile(history)).isEqualTo(local);
        assertThat(postgresCalls(PROFILE_LOOKUP) - before).isOne();
    }

    private HttpRequest getRequest(String path) {
        var uri = URI.create("http://127.0.0.1:" + bootstrap.getHttpPort() + path);
        return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).GET().build();
    }

    private static JsonNode queryProfile(HttpResponse<String> response) throws Exception {
        assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
        return JsonMapperFactory.create().readTree(response.body());
    }

    private long postgresCalls(String pattern) throws java.sql.SQLException {
        try (var connection = bootstrap.metadataDataSource().getConnection();
             var statement = connection.prepareStatement("""
                     SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                       AND query LIKE ?
                     """)) {
            statement.setString(1, pattern);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }

    @Test
    @DisplayName("QUERY-only coordinator does not start indexing or streaming")
    void queryOnlyCoordinatorSkipsMaintenanceSubsystems() {
        // Indexing and streaming require metadata + datasource which we don't provide.
        // A QUERY-only coordinator should NOT attempt to start them even if feature flags
        // are enabled, because role.isMaintenance() returns false.
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "query-coord-2", 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorDeploymentRole.QUERY, "0.0.0.0"),
                // Enable streaming + indexing flags — but QUERY role should skip them
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, true, false, false, false, false,
                        false, false, true),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        // Coordinator starts successfully — proves it didn't try to initialize
        // indexing/streaming subsystems (which would fail without metadata datasource)
        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
    }

    @Test
    @DisplayName("COMBINED role starts both query and maintenance paths")
    void combinedRoleStartsBothPaths() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("combined-1", 0);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();
    }

    @Test
    @DisplayName("CoordinatorDeploymentRole.QUERY returns isQuery=true, isMaintenance=false")
    void queryRolePredicates() {
        assertThat(CoordinatorDeploymentRole.QUERY.isQuery()).isTrue();
        assertThat(CoordinatorDeploymentRole.QUERY.isMaintenance()).isFalse();
    }

    @Test
    @DisplayName("CoordinatorDeploymentRole.MAINTENANCE returns isQuery=false, isMaintenance=true")
    void maintenanceRolePredicates() {
        assertThat(CoordinatorDeploymentRole.MAINTENANCE.isQuery()).isFalse();
        assertThat(CoordinatorDeploymentRole.MAINTENANCE.isMaintenance()).isTrue();
    }

    @Test
    @DisplayName("CoordinatorDeploymentRole.COMBINED returns both true")
    void combinedRolePredicates() {
        assertThat(CoordinatorDeploymentRole.COMBINED.isQuery()).isTrue();
        assertThat(CoordinatorDeploymentRole.COMBINED.isMaintenance()).isTrue();
    }
}
