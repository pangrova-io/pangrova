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
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;
import org.pangrova.transport.client.ColumnDescriptor;
import org.pangrova.transport.client.FlightResultFetcher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.pangrova.coordinator.testinfra.SignedOidcIssuer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
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
        var beforeDispatch = "admitted-before-dispatch";
        var coordinatorStatement = "admitted-local-statement";
        var priorContext = JournalEventContext.create(System.currentTimeMillis(), "prior-coordinator");
        var journalConfig = new StateJournalConfig("JDBC", jdbc.jdbcUrl(), 20, 20, 24, 1024);
        try (var journal = new JdbcQueryStateJournal(source, journalConfig)) {
            journal.appendQueryEventSync(new QueryStateEvent(priorContext, terminal, null, null, "FAILED"));
            journal.appendTaskEventSync(new TaskStateEvent(priorContext, taskOnly,
                    "unresolved-task", "prior-worker", "RUNNING", 0, 1));
        }
        var anchors = new JdbcExecutionAnchorStore(source);
        var lease = Duration.ofMinutes(1);
        var planningGrant = anchors.tryAcquireLimitedQuery(beforeDispatch, "prior", "prior-incarnation", lease,
                "planned-tenant", 1).orElseThrow().anchor();
        var controlGrant = anchors.tryAcquireLimitedQuery(coordinatorStatement, "prior", "prior-incarnation", lease,
                "control-tenant", 1).orElseThrow().anchor();
        try (var journal = new JdbcQueryStateJournal(source, journalConfig)) {
            for (var grant : java.util.List.of(planningGrant, controlGrant)) {
                var admitted = JournalEventContext.create(System.currentTimeMillis(), grant.ownerId());
                journal.appendFencedAdmissionSync(new QueryStateEvent(admitted, grant.executionId(), "SELECT 1",
                        "{}", "QUEUED"), grant);
            }
            assertThat(journal.reserveCoordinatorStatement(controlGrant)).isTrue();
        }
        assertThat(anchors.yield(planningGrant)).isTrue();
        assertThat(anchors.yield(controlGrant)).isTrue();

        var terminalClaim = expireAnchor(anchors, terminal);
        var taskClaim = expireAnchor(anchors, taskOnly);
        var beforeAdmission = anchors.tryAcquireLimitedQuery(empty, "prior", "prior-incarnation",
                Duration.ofMinutes(1), "limited-tenant", 1).orElseThrow().anchor();
        assertThat(anchors.yield(beforeAdmission)).isTrue();

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
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(
                () -> anchors.current(beforeDispatch).isEmpty());
        assertThat(anchors.current(coordinatorStatement)).hasValueSatisfying(anchor ->
                assertThat(anchor.ownerId()).isEqualTo("recovery-successor"));
        assertRetainedSlots(source, empty, "limited-tenant", 0);
        assertRetainedSlots(source, beforeDispatch, "planned-tenant", 0);
        assertRetainedSlots(source, coordinatorStatement, "control-tenant", 1);
    }

    private static void assertRetainedSlots(PGSimpleDataSource source, String execution, String tenant, int expected)
            throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                SELECT active_count, EXISTS (SELECT 1 FROM execution_anchor WHERE execution_id = ?)
                FROM tenant_concurrency_slots WHERE tenant_id = ?
                """)) {
            statement.setString(1, execution);
            statement.setString(2, tenant);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(expected);
                assertThat(rows.getBoolean(2)).isEqualTo(expected > 0);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"FAILED", "RUNNING"})
    void recoveredPredecessorRetainsItsSlotUntilTheExactWorkerExitReceipt(String priorState) throws Exception {
        var configuration = withAuthority(queryRole("task-drain-successor"));
        var jdbc = configuration.metadataJdbcConfig();
        var source = new PGSimpleDataSource();
        source.setURL(jdbc.jdbcUrl());
        source.setUser(jdbc.username());
        source.setPassword(jdbc.password());
        SchemaMigrationRunner.migrate(source);
        var anchors = new JdbcExecutionAnchorStore(source);
        var prior = anchors.tryAcquireLimitedQuery("terminal-with-issuance", "prior", "prior-incarnation",
                Duration.ofMinutes(1), "recovery-slot", 1).orElseThrow().anchor();
        var seal = recoverySeal(prior);
        var receipt = CanonicalTaskAdmissionSeals.closedWithoutOwner(seal).toByteArray();
        var delivered = new CompletableFuture<TaskAdmissionSealRequest>();
        var exited = new java.util.concurrent.atomic.AtomicBoolean();
        var peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peer.createContext("/v1/task/recovery-attempt", exchange -> {
            try (exchange) {
                if (!"DELETE".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                delivered.complete(TaskAdmissionSealRequest.parseFrom(exchange.getRequestBody().readAllBytes()));
                if (!exited.get()) {
                    exchange.sendResponseHeaders(202, -1);
                    return;
                }
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
                journal.appendQueryEventSync(new QueryStateEvent(context, prior.executionId(), null, null, priorState));
                journal.appendTaskIssuancesSync(java.util.List.of(new TaskStateEvent(
                        JournalEventContext.create(context.timestampMs(), prior.ownerId()), prior.executionId(),
                        "recovery-attempt", "prior-worker", "SCHEDULED", 0, 0, issuance)));
            }
            assertThat(anchors.yield(prior)).isTrue();
            bootstrap = new CoordinatorBootstrap(configuration);
            bootstrap.start();

            assertThat(delivered.get(10, TimeUnit.SECONDS)).isEqualTo(seal);
            assertThat(anchors.current(prior.executionId())).hasValueSatisfying(anchor ->
                    assertThat(anchor.ownerId()).isEqualTo("task-drain-successor"));
            assertRecoveredResources(source, prior.executionId(), 1);
            exited.set(true);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertRecoveredResources(source, prior.executionId(), 0));
        } finally {
            peer.stop(0);
        }
    }

    private static void assertRecoveredResources(PGSimpleDataSource source, String queryId, int expected)
            throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                WITH target(id) AS (VALUES (CAST(? AS text)))
                SELECT (SELECT COUNT(*) FROM execution_anchor a, target WHERE a.execution_id = target.id),
                       (SELECT COUNT(*) FROM task_state_journal t, target WHERE t.query_id = target.id),
                       (SELECT COUNT(*) FROM tenant_concurrency_queries q, target WHERE q.query_id = target.id),
                       (SELECT active_count FROM tenant_concurrency_slots WHERE tenant_id = 'recovery-slot')
                """)) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                for (int i = 1; i <= 4; i++) assertThat(rows.getInt(i)).isEqualTo(expected);
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
    void remoteModeWithNoAllocatedShuffleDrainsWithoutRpcOrIdleDatabaseScans() throws Exception {
        try (var unusedShufflePort = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
                var postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                        .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements")) {
            postgres.start();
            var base = queryRole("remote-idle");
            var flags = new CoordinatorBootstrapConfig.FeatureFlags(
                    false, false, false, false, false, false, false, false, false, false, false,
                    false, false, org.pangrova.config.enums.ShuffleMode.REMOTE,
                    java.util.List.of("127.0.0.1:" + unusedShufflePort.getLocalPort()), "pangrova", java.util.Map.of());
            var metadata = new CoordinatorBootstrapConfig.MetadataJdbcConfig(postgres.getJdbcUrl(),
                    postgres.getUsername(), postgres.getPassword());
            var config = withAuthority(new CoordinatorBootstrapConfig(base.core(), flags, base.query(),
                    base.transport(), new CoordinatorBootstrapConfig.WorkerHealthConfig(20_000, 60_000, 100),
                    metadata, null, null));
            try {
                startRemoteQueryCoordinator(config);
                String intentRetirement = "%DELETE FROM shuffle.shuffle_cleanup_intent%";
                long before = postgresCalls(intentRetirement);
                var receipt = submitShow(HttpClient.newHttpClient(), bootstrap.getHttpPort(), null);
                assertThat(receipt.resultDisposition()).isEqualTo(ResultDisposition.STREAM);
                try (var fetcher = new FlightResultFetcher();
                        var cursor = fetcher.open(receipt.resultEndpoint(), System.currentTimeMillis() + 5_000)) {
                    assertThat(cursor.next()).isFalse();
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(
                        () -> bootstrap.getQueryManager().getStateMachine(receipt.queryId()).status().drained());
                assertThat(postgresCalls(intentRetirement) - before).isOne();
                assertNoShuffleCleanupPolling();
                unusedShufflePort.setSoTimeout(150);
                org.assertj.core.api.Assertions.assertThatThrownBy(unusedShufflePort::accept)
                        .isInstanceOf(java.net.SocketTimeoutException.class);
            } finally {
                if (bootstrap != null && bootstrap.isStarted()) bootstrap.shutdown();
            }
        }
    }

    private void startRemoteQueryCoordinator(CoordinatorBootstrapConfig config) throws Exception {
        var source = new PGSimpleDataSource();
        var metadata = config.metadataJdbcConfig();
        source.setURL(metadata.jdbcUrl());
        source.setUser(metadata.username());
        source.setPassword(metadata.password());
        SchemaMigrationRunner.migrate(source);
        org.flywaydb.core.Flyway.configure().dataSource(source).schemas("shuffle").defaultSchema("shuffle")
                .locations("classpath:org/pangrova/shuffle/migration").load().migrate();
        bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public");
        }
    }

    private void assertNoShuffleCleanupPolling() throws Exception {
        String pattern = "%FROM shuffle.shuffle_cleanup_intent%";
        long before = postgresCalls(pattern);
        org.awaitility.Awaitility.await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(postgresCalls(pattern)).isEqualTo(before));
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

    @Test
    void concurrentHttpOwnersShareTheBoundSlotUntilQueryDrain() throws Exception {
        try (var issuer = new SignedOidcIssuer()) {
            exerciseTwoCoordinatorQuota(issuer);
        }
    }

    private void exerciseTwoCoordinatorQuota(SignedOidcIssuer issuer) throws Exception {
        var base = queryRole("quota-primary");
        var flags = new CoordinatorBootstrapConfig.FeatureFlags(
                false, false, false, false, false, false, true, false, false,
                false, false, false, false, base.featureFlags().shuffleMode(), java.util.List.of(),
                base.featureFlags().shuffleProvider(), java.util.Map.of());
        var security = new CoordinatorBootstrapConfig.SecurityConfig("OAUTH2", "quota-test-internal-secret",
                java.util.Map.of("issuerUri", issuer.uri(), "audience", "bootstrap-tests"));
        var configuration = withAuthority(new CoordinatorBootstrapConfig(base.core(), flags, base.query(),
                base.transport(), base.workerHealth(), null, security, base.observabilityBootstrapConfig()));
        String authorization = "Bearer " + issuer.token("bootstrap-tests", true);
        bootstrap = new CoordinatorBootstrap(configuration);
        bootstrap.start();

        var source = bootstrap.metadataDataSource();
        var quota = new ResourceQuota(1L << 30, 1, 100, 60_000, 1L << 30, 100, 1L << 40, 1L << 40);
        var tenants = new JdbcTenantStore(source);
        tenants.createTenant(new Tenant("http-slot", "HTTP slot", TenantStatus.ACTIVE, quota,
                java.util.List.of(), java.util.List.of(), null, java.util.Map.of(), java.time.Instant.EPOCH, "test"));
        tenants.addUserToTenant("http-slot", "alice", TenantRole.MEMBER);

        var peer = new CoordinatorBootstrap(new CoordinatorBootstrapConfig(queryRole("quota-peer").core(),
                configuration.featureFlags(), configuration.query(), configuration.transport(),
                configuration.workerHealth(), configuration.metadataJdbcConfig(), configuration.securityConfig(),
                configuration.observabilityBootstrapConfig()));
        peer.start();
        try {
            try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                statement.execute("""
                        CREATE FUNCTION wait_before_query_journal() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                            PERFORM pg_advisory_xact_lock(hashtext(current_schema()), 713);
                            RETURN NEW;
                        END;
                        $$;
                        CREATE TRIGGER wait_before_query_journal BEFORE INSERT ON query_state_journal
                        FOR EACH ROW WHEN (NEW.state = 'QUEUED') EXECUTE FUNCTION wait_before_query_journal()
                        """);
            }
            var client = HttpClient.newHttpClient();
            CompletableFuture<HttpResponse<String>> submitted;
            try (var guard = source.getConnection(); var statement = guard.createStatement()) {
                guard.setAutoCommit(false);
                statement.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema()), 713)");
                submitted = client.sendAsync(showRequest(bootstrap.getHttpPort(), null, authorization),
                        HttpResponse.BodyHandlers.ofString());
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(6))
                        .until(() -> boundHttpQuota(source) == 1);
                var rejected = client.send(showRequest(peer.getHttpPort(), null, authorization),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(rejected.statusCode()).as("%s", rejected.body()).isEqualTo(429);
                assertThat(rejected.body()).contains("TENANT-0001");
                assertThat(submitted).isNotDone();
                guard.rollback();
            }
            var response = submitted.get(10, TimeUnit.SECONDS);
            assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
            var receipt = JsonMapperFactory.create().readValue(response.body(), SubmitQueryResponse.class);
            var resultOwner = bootstrap.getQueryManager().getStateMachine(receipt.queryId());
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(
                    () -> assertThat(resultOwner.status().state().name()).isEqualTo("FINISHED"));
            assertThat(resultOwner.status().drained()).isFalse();
            assertThat(boundHttpQuota(source)).isOne();
            var deniedWhileUnread = client.send(showRequest(peer.getHttpPort(), null, authorization),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(deniedWhileUnread.statusCode()).as("%s", deniedWhileUnread.body()).isEqualTo(429);
            try (var fetcher = new FlightResultFetcher();
                 var cursor = fetcher.open(receipt.resultEndpoint(), System.currentTimeMillis() + 5_000)) {
                assertThat(cursor.next()).isFalse();
            }
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(8))
                    .until(() -> boundHttpQuota(source) == 0
                            && bootstrap.getQueryManager().getStateMachine(receipt.queryId()).status().drained());

            var unopened = client.send(showRequest(bootstrap.getHttpPort(), null, authorization),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(unopened.statusCode()).as("%s", unopened.body()).isEqualTo(200);
            var unopenedReceipt = JsonMapperFactory.create().readValue(unopened.body(), SubmitQueryResponse.class);
            assertThat(unopenedReceipt.resultEndpoint()).isNotNull();
            assertThat(boundHttpQuota(source)).isOne();
            bootstrap.shutdown();
            assertThat(boundHttpQuota(peer.metadataDataSource())).isZero();
        } finally {
            peer.shutdown();
        }
    }

    private static int boundHttpQuota(javax.sql.DataSource source) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement();
             var result = statement.executeQuery("""
                     SELECT COUNT(*), COUNT(a.execution_id) FILTER (WHERE q.execution_bound)
                     FROM tenant_concurrency_queries q
                     LEFT JOIN execution_anchor a ON a.execution_id = q.query_id
                     WHERE q.tenant_id = 'http-slot'
                     """)) {
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(result.getInt(2));
            return result.getInt(1);
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
        var response = client.send(showRequest(port, idempotencyKey, null), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
        return JsonMapperFactory.create().readValue(response.body(), SubmitQueryResponse.class);
    }

    private static HttpRequest showRequest(int port, String idempotencyKey, String authorization) {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/query/submit"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SHOW DYNAMIC TABLES\"}"));
        if (idempotencyKey != null) request.header("Idempotency-Key", idempotencyKey);
        if (authorization != null) request.header("Authorization", authorization);
        return request.build();
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
