package org.pangrova.testing.integration.e2e.speculation;

import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.authentication.ClientAuthentication;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.config.enums.ShuffleMode;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.rest.worker.WorkerControlResponse;
import org.pangrova.testing.integration.e2e.SharedTestContainers;
import org.pangrova.testing.testkit.TestProjectPaths;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.persistence.coordination.postgresql.admission.JdbcClusterConcurrencySlots;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Three production Main classes in three owned local JVMs; the test JVM never owns an engine. */
final class IndependentLocalProcesses implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(IndependentLocalProcesses.class);
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(120);
    private final Path workspace;
    private final String internalSecret = UUID.randomUUID().toString();
    private final Path warehouse;
    private final Path javaExecutable;
    private final LocalRuntimeArtifacts artifacts;
    private final org.pangrova.worker.function.NativeFunctionConfig functions;
    private final boolean indexing;
    private final Inet4Address advertisedAddress;
    private final boolean speculation;
    private final String cdcKafkaBootstrap;
    private final ShuffleMode shuffleMode;
    private final ProcessSecurityAuthority security;
    private final boolean ownsSecurity;
    private final String nodeName;
    private final ProcessStatusReportRelay reports;
    private final TestTlsAuthority tls;
    private final TestTlsAuthority.Identity coordinatorIdentity;
    private final TestTlsAuthority.Identity clientIdentity;
    private final Map<String, TestTlsAuthority.Identity> workerIdentities = new LinkedHashMap<>();
    private final Map<String, OwnedProcess> workers = new LinkedHashMap<>();
    private final Map<String, RegisteredWorker> acceptedWorkers = new LinkedHashMap<>();
    private final List<OwnedProcess> owned = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();
    private OwnedProcess coordinator;
    private ProcessRemoteShuffle remoteShuffle;
    private final int coordinatorPort;
    private boolean closed;
    private boolean preserveDiagnostics;
    private boolean reportsClosed;
    private boolean securityClosed;
    private boolean tlsClosed;

    IndependentLocalProcesses() throws Exception {
        this(org.pangrova.worker.function.NativeFunctionConfig.DEFAULT);
    }

    IndependentLocalProcesses(org.pangrova.worker.function.NativeFunctionConfig functions) throws Exception {
        this(functions, false);
    }

    IndependentLocalProcesses(boolean speculation) throws Exception {
        this(org.pangrova.worker.function.NativeFunctionConfig.DEFAULT, speculation);
    }

    private IndependentLocalProcesses(org.pangrova.worker.function.NativeFunctionConfig functions,
                                      boolean speculation) throws Exception {
        this(functions, speculation, ShuffleMode.NATIVE);
    }

    static IndependentLocalProcesses diskShuffle() throws Exception {
        return new IndependentLocalProcesses(
                org.pangrova.worker.function.NativeFunctionConfig.DEFAULT, false, ShuffleMode.LOCAL);
    }

    static IndependentLocalProcesses remoteShuffle() throws Exception {
        return new IndependentLocalProcesses(
                org.pangrova.worker.function.NativeFunctionConfig.DEFAULT, false, ShuffleMode.REMOTE);
    }

    private IndependentLocalProcesses(org.pangrova.worker.function.NativeFunctionConfig functions,
            boolean speculation, ShuffleMode shuffleMode) throws Exception {
        this(functions, speculation, shuffleMode, null, false);
    }

    static IndependentLocalProcesses cdc(String kafkaBootstrap) throws Exception {
        return new IndependentLocalProcesses(org.pangrova.worker.function.NativeFunctionConfig.DEFAULT,
                false, ShuffleMode.NATIVE, java.util.Objects.requireNonNull(kafkaBootstrap), false);
    }

    static IndependentLocalProcesses indexed(org.pangrova.worker.function.NativeFunctionConfig functions)
            throws Exception {
        return new IndependentLocalProcesses(functions, false, ShuffleMode.NATIVE, null, true);
    }

    private IndependentLocalProcesses(org.pangrova.worker.function.NativeFunctionConfig functions,
            boolean speculation, ShuffleMode shuffleMode, String cdcKafkaBootstrap, boolean indexing) throws Exception {
        this(functions, speculation, shuffleMode, cdcKafkaBootstrap, indexing, null);
    }

    private IndependentLocalProcesses(org.pangrova.worker.function.NativeFunctionConfig functions,
            boolean speculation, ShuffleMode shuffleMode, String cdcKafkaBootstrap, boolean indexing,
            ProcessSecurityAuthority sharedSecurity) throws Exception {
        ownsSecurity = sharedSecurity == null;
        nodeName = ownsSecurity ? "local" : "peer";
        securityClosed = !ownsSecurity;
        this.cdcKafkaBootstrap = cdcKafkaBootstrap;
        this.indexing = indexing;
        this.shuffleMode = shuffleMode;
        this.speculation = speculation;
        this.functions = java.util.Objects.requireNonNull(functions, "functions");
        if (!functions.autoRegister()) {
            throw new IllegalArgumentException("Independent Workers must register their actual Native functions");
        }
        if (Runtime.version().feature() != 17) {
            throw new IllegalStateException("Independent local process test requires JDK 17");
        }
        if (!"UTF-8".equalsIgnoreCase(System.getProperty("native.encoding"))) {
            throw new IllegalStateException("Independent local process test requires a UTF-8 "
                    + "native locale so non-ASCII CLI credentials survive ProcessBuilder");
        }
        javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java");
        artifacts = LocalRuntimeArtifacts.fromRequiredProperties();
        advertisedAddress = routableHostAddress();
        workspace = Files.createTempDirectory("pangrova-local-process-",
                PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rwx------")));
        Path createdWarehouse;
        ProcessSecurityAuthority createdSecurity = null;
        ProcessStatusReportRelay createdReports = null;
        TestTlsAuthority createdTls = null;
        TestTlsAuthority.Identity createdClient;
        TestTlsAuthority.Identity createdCoordinator;
        int chosenPort;
        try {
            createdWarehouse = Files.createDirectory(workspace.resolve("warehouse"));
            if (ownsSecurity) {
                createdSecurity = new ProcessSecurityAuthority(
                        SharedTestContainers.NETWORK, shuffleMode == ShuffleMode.REMOTE);
            }
            createdReports = new ProcessStatusReportRelay();
            createdTls = new TestTlsAuthority(workspace);
            createdClient = createdTls.issue(nodeName + "-client", "localhost", false);
            String coordinatorDns = shuffleMode == ShuffleMode.REMOTE
                    ? ProcessRemoteShuffle.COORDINATOR_IDENTITY : null;
            createdCoordinator = createdTls.issue(nodeName + "-coordinator", advertisedAddress, coordinatorDns, false);
            workerIdentities.put(workerId(1),
                    createdTls.issue(workerId(1), advertisedAddress, false));
            workerIdentities.put(workerId(2),
                    createdTls.issue(workerId(2), advertisedAddress, false));
            chosenPort = availablePort();
        } catch (Exception | Error failure) {
            for (AutoCloseable owner : new AutoCloseable[] {
                    createdReports, createdSecurity, createdTls}) {
                if (owner == null) continue;
                try {
                    owner.close();
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw new IllegalStateException(
                    "Local process fixture construction failed; diagnostics retained at "
                            + workspace, failure);
        }
        warehouse = createdWarehouse;
        security = ownsSecurity ? createdSecurity : sharedSecurity;
        reports = createdReports;
        tls = createdTls;
        clientIdentity = createdClient;
        coordinatorIdentity = createdCoordinator;
        coordinatorPort = chosenPort;
    }

    void startCoordinatorAndFirstWorker() throws Exception {
        if (ownsSecurity) security.start();
        if (shuffleMode == ShuffleMode.REMOTE) {
            remoteShuffle = ProcessRemoteShuffle.start(workspace, security, tls, availablePort());
        }
        reports.connect(URI.create("http://127.0.0.1:" + coordinatorPort));
        startQueryControlPlane();
    }

    IndependentLocalProcesses startPeer() throws Exception {
        var peer = new IndependentLocalProcesses(functions, false, ShuffleMode.NATIVE, null, false, security);
        try {
            peer.startCoordinatorAndFirstWorker();
            return peer;
        } catch (Exception | Error failure) {
            failure.addSuppressed(new IllegalStateException("Peer diagnostics retained at " + peer.retainDiagnostics()));
            try {
                peer.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private String workerId(int index) {
        return nodeName + "-worker-" + index;
    }

    String coordinatorId() {
        return nodeName + "-process-coordinator";
    }

    void allowCatalog(String catalog) {
        security.allowCatalog(catalog);
    }

    private void startQueryControlPlane() throws Exception {
        launchQueryCoordinator();
        startWorker(workerId(1));
        query("SELECT 42", ResultProtocol.GRPC_TLS);
    }

    private void launchQueryCoordinator() throws Exception {
        coordinator = launch("coordinator", artifacts.coordinatorClasspath(),
                "org.pangrova.server.coordinator.bootstrap.PangrovaCoordinatorMain",
                coordinatorYaml(cdcKafkaBootstrap != null || indexing ? "COMBINED" : "QUERY"),
                "-Xms128m", "-Xmx512m",
                "-XX:MaxDirectMemorySize=128m",
                "-Dpangrova.index.basePath=" + workspace.resolve("indexes"),
                // CLI status polling and multi-file seeding exceed the public default of 100/min.
                // Rate-limit behavior is covered by dedicated HTTP integration tests.
                "-Dpangrova.http.max-requests-per-minute=100000",
                "-Dpangrova.speculation.single-task-duration-threshold-ms=15000");
        awaitCoordinatorControlPlane();
    }

    void switchToMaintenanceOnly() throws Exception {
        stopWorkers();
        stopCoordinator();
        startMaintenanceOnly();
    }

    void restartQueryCoordinator() throws Exception {
        stopWorkers();
        stopCoordinator();
        startQueryControlPlane();
    }

    @FunctionalInterface
    interface WhileCoordinatorStopped {
        void run() throws Exception;
    }

    void restartQueryCoordinatorAfterCrash(WhileCoordinatorStopped whileStopped) throws Exception {
        requireAlive(coordinator);
        coordinator.process().destroyForcibly();
        if (!coordinator.process().waitFor(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                    "Coordinator did not exit after forced termination: " + coordinator.process().pid());
        }
        whileStopped.run();
        launchQueryCoordinator();
    }

    private void stopWorkers() {
        for (var worker : workers.values()) {
            terminateGracefully(worker, Duration.ofSeconds(60));
        }
        workers.clear();
        acceptedWorkers.clear();
    }

    void stopCoordinator() throws InterruptedException {
        requireAlive(coordinator);
        coordinator.process().destroy();
        if (!coordinator.process().waitFor(60, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Coordinator did not stop after SIGTERM: " + tail(coordinator.log()));
        }
    }

    void startMaintenanceOnly() throws Exception {
        coordinator = launch("maintenance", artifacts.coordinatorClasspath(),
                "org.pangrova.server.coordinator.bootstrap.PangrovaCoordinatorMain",
                coordinatorYaml("MAINTENANCE"), "-Xms128m", "-Xmx512m", "-XX:MaxDirectMemorySize=128m");
        Awaitility.await("standalone maintenance CDC leader").atMost(STARTUP_TIMEOUT).until(() -> {
            requireAlive(coordinator);
            String log = Files.readString(coordinator.log());
            return log.contains("event=cdc_local_journaled_runtime_started")
                    && log.contains("event=maintenance_leader_elected");
        });
    }

    void startSecondWorker() throws Exception {
        startWorker(workerId(2));
        var first = acceptedWorkers.get(workerId(1)).request();
        var second = acceptedWorkers.get(workerId(2)).request();
        if (first.httpPort() == second.httpPort()
                || first.resultServerPort() == second.resultServerPort()) {
            throw new IllegalStateException("Two live Workers registered overlapping endpoints");
        }
    }

    void createSourceCatalog() throws Exception {
        String location = warehouse.toString().replace("'", "''");
        String jdbc = hostJdbcUrl().replace("'", "''");
        query("CREATE CATALOG spec_catalog WITH ('connector' = 'iceberg', "
                + "'type' = 'jdbc', 'uri' = '" + jdbc + "', "
                + "'jdbc.user' = 'pangrova', 'jdbc.password' = 'pangrova_test', "
                + "'jdbc.schema-version' = 'V1', 'warehouse' = '" + location + "')",
                ResultProtocol.GRPC_TLS);
        awaitClusterReady();
    }

    private void startWorker(String id) throws Exception {
        long after = System.nanoTime();
        Path spill = Files.createDirectories(workspace.resolve(id + "-spill"));
        Path shuffleSpill = Files.createDirectories(workspace.resolve(id + "-shuffle-spill"));
        OwnedProcess process = launch(id, artifacts.workerClasspath(),
                "org.pangrova.server.worker.bootstrap.PangrovaWorkerMain",
                workerYaml(id, spill, shuffleSpill), "-Xms128m", "-Xmx384m",
                "-XX:MaxDirectMemorySize=128m", "-Dio.netty.maxDirectMemory=0");
        workers.put(id, process);
        RegisteredWorker accepted = awaitRegistration(id, after, process);
        WorkerRegistrationRequest registration = accepted.request();
        if (!advertisedAddress.getHostAddress().equals(registration.httpHost())
                || registration.httpPort() <= 0 || registration.resultServerPort() <= 0
                || accepted.coordinatorEpoch() <= 0) {
            throw new IllegalStateException("Worker registered an unroutable endpoint: " + id);
        }
        var inventory = CapabilityInventoryCodec.decode(registration.capabilityInventory());
        if (inventory.getCapabilitiesCount() == 0) {
            throw new IllegalStateException("Worker registered no native capabilities: " + id);
        }
        awaitHealth(id, process, registration.httpPort());
        awaitWorkerCapacity(id, accepted.receivedNanos(), process);
        acceptedWorkers.put(id, accepted);
    }

    void stopAndRestartFirstWorker() throws Exception {
        stopAndRestartFirstWorker(() -> {});
    }

    @FunctionalInterface
    interface WhileFirstWorkerStopped {
        void run() throws Exception;
    }

    void stopAndRestartFirstWorker(WhileFirstWorkerStopped whileStopped) throws Exception {
        OwnedProcess old = workers.get(workerId(1));
        long oldPid = old.process().pid();
        RegisteredWorker previous = acceptedWorkers.get(workerId(1));
        terminateGracefully(old, Duration.ofSeconds(60));
        workers.remove(workerId(1));
        acceptedWorkers.remove(workerId(1));
        query("SELECT 42", ResultProtocol.GRPC_TLS); // second Worker serves a public query alone
        whileStopped.run();
        startWorker(workerId(1));
        if (workers.get(workerId(1)).process().pid() == oldPid) {
            throw new IllegalStateException("Restart reused the old Worker process");
        }
        RegisteredWorker replacement = acceptedWorkers.get(workerId(1));
        if (replacement.receivedNanos() <= previous.receivedNanos()
                || replacement.coordinatorEpoch() < previous.coordinatorEpoch()) {
            throw new IllegalStateException("Restart did not publish a fresh, fenced registration");
        }
    }

    void crashWorker(String id) throws InterruptedException {
        var worker = Objects.requireNonNull(workers.get(id), "owned Worker process");
        requireAlive(worker);
        worker.process().destroyForcibly();
        if (!worker.process().waitFor(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Worker did not exit after forced termination: " + id);
        }
    }

    long pid(String workerId) { return workers.get(workerId).process().pid(); }
    long coordinatorPid() { return coordinator.process().pid(); }

    boolean coordinatorLogged(String marker) throws IOException {
        return Files.readString(coordinator.log()).contains(marker);
    }

    boolean hasAttributedQueryAudit(String queryId) throws java.sql.SQLException {
        try (var connection = security.dataSource().getConnection();
             var statement = connection.prepareStatement(
                     "SELECT user_id, tenant_id FROM query_audit_records WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    return false;
                }
                return ProcessSecurityAuthority.USER.equals(result.getString("user_id"))
                        && ProcessSecurityAuthority.TENANT.equals(result.getString("tenant_id"));
            }
        }
    }
    Path retainDiagnostics() {
        preserveDiagnostics = true;
        return workspace;
    }
    ProcessStatusReportRelay reports() { return reports; }
    Path warehouse() { return warehouse; }
    String hostJdbcUrl() { return security.hostJdbcUrl(); }
    javax.sql.DataSource metadataDataSource() { return security.dataSource(); }
    int coordinatorPort() { return coordinatorPort; }
    String coordinatorLog() throws IOException { return Files.readString(coordinator.log()); }
    boolean remoteShuffleObjectsReleased() { return remoteShuffle.objectsReleased(); }
    HttpResponse<byte[]> replayReport(ProcessStatusReportRelay.Delivery actual) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + coordinatorPort
                        + "/v1/task/status-report"))
                .header("Authorization", "Bearer " + internalSecret)
                .header("Content-Type", org.pangrova.protocol.rest.task.TaskStatusWire.MEDIA_TYPE).timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(actual.body())).build();
        return http.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }
    void awaitClusterReady() {
        awaitHealth("Coordinator with registered Worker and catalog", coordinator, coordinatorPort);
    }
    void enableMetadataStatementCounters() throws java.sql.SQLException {
        try (var connection = security.dataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
        }
    }

    long metadataStatementCalls(String pattern) throws java.sql.SQLException {
        try (var connection = security.dataSource().getConnection();
                var statement = connection.prepareStatement("""
                        SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                          AND query LIKE ?
                        """)) {
            statement.setString(1, pattern);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) throw new java.sql.SQLException("Missing statement count");
                return rows.getLong(1);
            }
        }
    }

    void revokeTenantMembership() {
        var tenants = new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(security.dataSource());
        tenants.removeUserFromTenant(ProcessSecurityAuthority.TENANT, ProcessSecurityAuthority.USER);
    }

    int activeTenantReservations() {
        return new JdbcClusterConcurrencySlots(security.dataSource())
                .active(ProcessSecurityAuthority.TENANT);
    }

    QueryStatusResponse status(String queryId) {
        try (var client = coordinatorClientBuilder().build()) {
            return client.getQueryStatus(queryId);
        }
    }

    String submit(String sql) {
        try (var client = coordinatorClientBuilder().build()) {
            return client.submitQuery(sql, Map.of()).queryId();
        }
    }

    PangrovaRestClient.Builder coordinatorClientBuilder() {
        return PangrovaRestClient.builder()
                .coordinatorHost("127.0.0.1").coordinatorPort(coordinatorPort)
                .authentication(ClientAuthentication.basic(
                        ProcessSecurityAuthority.USER, ProcessSecurityAuthority.PASSWORD))
                .resultTransportSecurity(new org.pangrova.transport.client.security.ResultTransportSecurity(
                        true, tls.root(), clientIdentity.certificateChain(), clientIdentity.privateKey()));
    }

    QueryResult query(String sql, ResultProtocol expectedProtocol) throws Exception {
        return requireSuccessfulQuery(executeSql(sql, expectedProtocol), expectedProtocol);
    }

    QueryResult requireSuccessfulQuery(CliOutcome outcome, ResultProtocol expectedProtocol) {
        if (outcome.exitCode() != 0 || outcome.receipt() == null
                || outcome.output() == null) {
            preserveDiagnostics = true;
            throw new IllegalStateException("Public CLI query failed: exit=" + outcome.exitCode()
                    + " log=" + outcome.errors() + " workspace=" + workspace);
        }
        JsonNode record = outcome.receipt();
        if (!"FINISHED".equals(record.path("state").asText())
                || !record.path("resultConsumed").asBoolean()
                || record.path("queryId").asText().isBlank()) {
            preserveDiagnostics = true;
            throw new IllegalStateException("CLI did not prove terminal result handling: " + record);
        }
        String queryId = record.path("queryId").asText();
        QueryStatusResponse publicStatus = status(queryId);
        if (!"FINISHED".equals(publicStatus.state())
                || publicStatus.resultEndpoint() == null
                || !expectedProtocol.value().equals(publicStatus.resultEndpoint().protocol())) {
            preserveDiagnostics = true;
            throw new IllegalStateException("Public query state or typed result protocol disagrees"
                    + " with CLI receipt: " + queryId + " status=" + publicStatus);
        }
        return new QueryResult(queryId, outcome.output());
    }

    java.sql.Connection openJdbc() throws java.sql.SQLException {
        var settings = new java.util.Properties();
        settings.setProperty("user", ProcessSecurityAuthority.USER);
        settings.setProperty("password", ProcessSecurityAuthority.PASSWORD);
        settings.setProperty("resultTlsRootCa", tls.root().toString());
        settings.setProperty("resultTlsCertificateChain", clientIdentity.certificateChain().toString());
        settings.setProperty("resultTlsPrivateKey", clientIdentity.privateKey().toString());
        return java.sql.DriverManager.getConnection("jdbc:pangrova://127.0.0.1:" + coordinatorPort + "/spec_catalog", settings);
    }

    CliOutcome executeSql(String sql, ResultProtocol expectedProtocol) throws Exception {
        return executeSql(sql, expectedProtocol, Duration.ofMinutes(5));
    }

    CliOutcome executeSql(String sql, ResultProtocol expectedProtocol, Duration queryTimeout) throws Exception {
        String suffix = UUID.randomUUID().toString();
        Path source = privateFile("query-" + suffix, ".sql");
        Path output = workspace.resolve("query-" + suffix + ".out");
        Path receipt = workspace.resolve("query-" + suffix + ".receipt");
        Path log = privateFile("query-" + suffix, ".log");
        Files.writeString(source, sql, StandardCharsets.UTF_8);
        List<String> command = new ArrayList<>(List.of(javaExecutable.toString(), "-Xmx256m",
                "-XX:MaxDirectMemorySize=128m",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-Dpangrova.test.runtime-manifest=" + artifacts.manifestPath(),
                "-Dpangrova.test.runtime-manifest-sha256=" + artifacts.manifestSha256(),
                "-Dpangrova.test.native-sha256=" + artifacts.nativeSha256(),
                "-jar", artifacts.cliJar().toString(),
                "--host", "127.0.0.1", "--port", Integer.toString(coordinatorPort),
                "--user", ProcessSecurityAuthority.USER,
                "--password", ProcessSecurityAuthority.PASSWORD,
                "--query-timeout", queryTimeout.toSeconds() + "s"));
        if (expectedProtocol == ResultProtocol.GRPC_TLS) {
            command.addAll(List.of("--result-tls-root-ca", tls.root().toString(),
                    "--result-tls-certificate-chain", clientIdentity.certificateChain().toString(),
                    "--result-tls-private-key", clientIdentity.privateKey().toString()));
        } else if (expectedProtocol != ResultProtocol.GRPC) {
            throw new IllegalArgumentException("Unsupported expected result protocol: " + expectedProtocol);
        }
        command.addAll(List.of("--file", source.toString(), "--output", output.toString(),
                "--execution-receipt", receipt.toString()));
        Process client = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
        owned.add(new OwnedProcess("cli-" + suffix, client, source, log));
        LOG.info("event=local_process_spawn role=cli pid={} manifest={} manifestSha256={} "
                        + "nativeSha256={} advertisedAddress={}",
                client.pid(), artifacts.manifestPath(), artifacts.manifestSha256(),
                artifacts.nativeSha256(), advertisedAddress.getHostAddress());
        try {
            if (!client.waitFor(6, TimeUnit.MINUTES)) {
                stopClient(client, log);
                throw new IllegalStateException("CLI timed out: " + tail(log));
            }
        } catch (InterruptedException interrupted) {
            try {
                stopClient(client, log);
            } catch (Exception cleanup) {
                interrupted.addSuppressed(cleanup);
            }
            Thread.currentThread().interrupt();
            preserveDiagnostics = true;
            throw interrupted;
        }
        JsonNode record = Files.isRegularFile(receipt)
                ? JsonMapperFactory.shared().readTree(Files.readString(receipt)) : null;
        return new CliOutcome(client.exitValue(), Files.isRegularFile(output) ? Files.readString(output) : null,
                Files.readString(log), record);
    }

    private RegisteredWorker awaitRegistration(
            String workerId, long afterNanos, OwnedProcess process) {
        Awaitility.await("real registration " + workerId)
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(process);
                    return reports.registrations().stream()
                            .anyMatch(reg -> reg.receivedNanos() >= afterNanos
                                    && workerId.equals(reg.request().workerId())
                                    && reg.response().get() != null
                                    && reg.response().get().statusCode() == 200);
                });
        var delivery = reports.registrations().stream()
                .filter(reg -> reg.receivedNanos() >= afterNanos
                        && workerId.equals(reg.request().workerId())
                        && reg.response().get() != null
                        && reg.response().get().statusCode() == 200)
                .reduce((left, right) -> right).orElseThrow();
        try {
            WorkerControlResponse response = JsonMapperFactory.shared().readValue(
                    delivery.response().get().body(), WorkerControlResponse.class);
            response.requireStatus("registered");
            return new RegisteredWorker(delivery.request(),
                    response.requireCoordinatorEpoch(), delivery.receivedNanos());
        } catch (IOException failure) {
            throw new IllegalStateException("Accepted Worker registration lacks canonical ACK", failure);
        }
    }

    private void awaitCoordinatorControlPlane() {
        Awaitility.await("Coordinator control plane before Worker/catalog bootstrap")
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(coordinator);
                    try {
                        var request = HttpRequest.newBuilder(URI.create(
                                "http://127.0.0.1:" + coordinatorPort + "/v1/health"))
                                .timeout(Duration.ofSeconds(2)).GET().build();
                        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() != 200 && response.statusCode() != 503) return false;
                        JsonNode health = JsonMapperFactory.shared().readTree(response.body());
                        return "COORDINATOR".equals(health.path("nodeType").asText())
                                && health.path("checks").path("http").asBoolean()
                                && health.path("checks").path("transport").asBoolean();
                    } catch (IOException ignored) {
                        return false;
                    }
                });
    }

    private void awaitWorkerCapacity(String id, long registrationNanos, OwnedProcess process) {
        Awaitility.await("accepted actual resource heartbeat for " + id)
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(process);
                    return reports.heartbeats().stream().anyMatch(heartbeat ->
                            heartbeat.receivedNanos() >= registrationNanos
                                    && id.equals(heartbeat.request().workerId())
                                    && heartbeat.request().availableMemoryBytes() > 0
                                    && "NONE".equals(heartbeat.request().memoryPressure())
                                    && heartbeat.response().get() != null
                                    && heartbeat.response().get().statusCode() == 200);
                });
    }

    private void awaitHealth(String role, OwnedProcess process, int port) {
        Awaitility.await(role + " health")
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(200))
                .atMost(STARTUP_TIMEOUT).until(() -> {
                    requireAlive(process);
                    try {
                        var request = HttpRequest.newBuilder(URI.create(
                                "http://127.0.0.1:" + port + "/v1/health"))
                                .timeout(Duration.ofSeconds(2)).GET().build();
                        return http.send(request, HttpResponse.BodyHandlers.discarding())
                                .statusCode() == 200;
                    } catch (IOException ignored) {
                        return false;
                    }
                });
    }

    private static void requireAlive(OwnedProcess process) {
        if (!process.process().isAlive()) {
            throw new IllegalStateException(process.name() + " exited: " + tail(process.log()));
        }
    }

    private OwnedProcess launch(String name, String classpath, String mainClass, String yaml, String... jvmArgs)
            throws IOException {
        Path config = privateFile(name, ".yml");
        Path log = privateFile(name, ".log");
        Files.writeString(config, yaml, StandardCharsets.UTF_8);
        List<String> command = new ArrayList<>();
        command.add(javaExecutable.toString());
        command.addAll(Arrays.asList(jvmArgs));
        Path loggingConfig = TestProjectPaths.findProjectRoot()
                .resolve("etc/logback.xml").toRealPath();
        Path logDirectory = Files.createDirectories(workspace.resolve("logs"));
        command.add("-Dlogback.configurationFile=" + loggingConfig);
        command.add("-DPANGROVA_LOG_DIR=" + logDirectory);
        command.add("-DPANGROVA_ROLE=" + name);
        command.add("-Dpangrova.test.runtime-manifest=" + artifacts.manifestPath());
        command.add("-Dpangrova.test.runtime-manifest-sha256=" + artifacts.manifestSha256());
        command.add("-Dpangrova.test.native-sha256=" + artifacts.nativeSha256());
        command.addAll(List.of("--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", classpath, mainClass, "--config", config.toString()));
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        OwnedProcess owner = new OwnedProcess(name, process, config, log);
        owned.add(owner);
        LOG.info("event=local_process_spawn role={} pid={} manifest={} manifestSha256={} "
                        + "nativeSha256={} advertisedAddress={}",
                name, process.pid(), artifacts.manifestPath(), artifacts.manifestSha256(),
                artifacts.nativeSha256(), advertisedAddress.getHostAddress());
        if (process.pid() == ProcessHandle.current().pid()) {
            throw new IllegalStateException("Production process was not isolated: " + name);
        }
        return owner;
    }

    private String coordinatorYaml(String role) {
        return """
                coordinator:
                  id: "%s"
                  host: 127.0.0.1
                  port: %d
                  role: %s
                  internal-auth-secret: "%s"
                  speculation:
                    enabled: %s
                  ha:
                    enabled: false
                  transport:
                    advertised-host: "%s"
                    allocator-limit-bytes: 16777216
                    tls:
                      enabled: true
                      certificate-chain-path: "%s"
                      private-key-path: "%s"
                      root-ca-path: "%s"
                  shuffle:
                    mode: %s
                    tls:
                      server-identity: "%s"
                metadata:
                  enabled: true
                  jdbc-url: "%s"
                  jdbc-username: pangrova
                  jdbc-password: pangrova_test
                security:
                  enabled: true
                  auth:
                    type: LDAP
                    properties:
                      ldap-url: "%s"
                      base-dn: dc=pangrova,dc=org
                      user-dn-pattern: uid={0},dc=pangrova,dc=org
                tenant:
                  enabled: true
                indexing:
                  enabled: %s
                """.formatted(coordinatorId(), coordinatorPort, role,
                internalSecret, speculation, advertisedAddress.getHostAddress(),
                coordinatorIdentity.certificateChain(), coordinatorIdentity.privateKey(),
                tls.root(), shuffleMode, remoteShuffle == null ? advertisedAddress.getHostAddress() : "localhost",
                security.hostJdbcUrl(), security.hostLdapUrl(), indexing)
                + (remoteShuffle == null ? "" : "coordinator.shuffle.endpoints: " + remoteShuffle.endpoint() + "\n");
    }

    private String workerYaml(String id, Path spill, Path shuffleSpill) throws IOException {
        var identity = workerIdentities.get(id);
        return """
                worker:
                  admission:
                    state-directory: "%s"
                  id: "%s"
                  host: 0.0.0.0
                  advertised-host: "%s"
                  port: 0
                  coordinator-url: "http://127.0.0.1:%d"
                  internal-auth-secret: "%s"
                  transport:
                    port: 0
                    tls:
                      enabled: true
                      certificate-chain-path: "%s"
                      private-key-path: "%s"
                      root-ca-path: "%s"
                      identity: "%s"
                  memory:
                    budget-bytes: 536870912
                    system-reserved-bytes: 67108864
                    node-bytes: 1073741824
                  threads:
                    query: 2
                    io: 2
                    scheduler: 1
                  spill:
                    directory: "%s"
                  shuffle:
                    mode: %s
                    service:
                      retention-bytes: 16777216
                      transit-bytes: 16777216
                      spill-bytes: 268435456
                      spill-files: 64
                      spill-directory: "%s"
                  native:
                    cache:
                      enabled: %s
                      memory-bytes: 67108864
                      artifact-path: "%s"
                      artifact-bytes: 268435456
                      artifact-index-memory-bytes: 67108864
                  cdc:
                    enabled: %s
                """.formatted(workspace.resolve("admission-" + id), id, advertisedAddress.getHostAddress(), reports.port(), internalSecret,
                identity.certificateChain(), identity.privateKey(), tls.root(),
                advertisedAddress.getHostAddress(),
                spill, shuffleMode, shuffleSpill, indexing,
                workspace.resolve(id + "-artifact-cache"), cdcKafkaBootstrap != null)
                + aiWorkerYaml() + (cdcKafkaBootstrap == null ? "" : "worker.cdc.kafka.bootstrap-servers: "
                        + JsonMapperFactory.shared().writeValueAsString(cdcKafkaBootstrap) + "\n");
    }

    private String aiWorkerYaml() throws IOException {
        var yaml = new StringBuilder();
        for (var setting : functions.configForPlugin("ai-inference").entrySet()) {
            if (!setting.getKey().matches("[a-z0-9.-]+")) {
                throw new IllegalArgumentException("Invalid AI deployment configuration key");
            }
            yaml.append("worker.ai.").append(setting.getKey()).append(": ")
                    .append(JsonMapperFactory.shared().writeValueAsString(setting.getValue())).append('\n');
        }
        return yaml.toString();
    }

    private Path privateFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(workspace, prefix + "-", suffix,
                PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rw-------")));
    }

    private static int availablePort() throws IOException {
        try (var socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            return socket.getLocalPort();
        }
    }

    private static Inet4Address routableHostAddress() throws IOException {
        return org.pangrova.testing.testkit.network.TestNetworkAddresses.routableIpv4();
    }

    private static String tail(Path path) {
        try (var channel = Files.newByteChannel(path)) {
            long size = channel.size();
            channel.position(Math.max(0, size - 20_000));
            var bytes = java.nio.ByteBuffer.allocate((int) Math.min(size, 20_000));
            while (bytes.hasRemaining() && channel.read(bytes) != -1) {
                // Drain the bounded log tail until the buffer is full or EOF.
            }
            bytes.flip();
            return StandardCharsets.UTF_8.decode(bytes).toString();
        } catch (IOException failure) {
            return failure.toString();
        }
    }

    private static void terminateGracefully(OwnedProcess owner, Duration timeout) {
        boolean[] interrupted = {Thread.interrupted()};
        try {
            if (!owner.process().isAlive()) {
                throw new IllegalStateException(owner.name() + " exited before SIGTERM: "
                        + tail(owner.log()));
            }
            owner.process().destroy();
            if (!awaitExit(owner.process(), timeout, interrupted)) {
                throw new IllegalStateException(owner.name() + " did not drain after SIGTERM: "
                        + tail(owner.log()));
            }
            String log = tail(owner.log());
            if (!log.contains("event=worker_shutdown_completed")
                    || !log.contains("event=worker_deregistration_ack workerId="
                            + owner.name())) {
                throw new IllegalStateException(owner.name()
                        + " exited without drain and deregistration receipts: " + log);
            }
        } finally {
            if (interrupted[0]) Thread.currentThread().interrupt();
        }
    }

    private static void stopClient(Process client, Path log) {
        boolean[] interrupted = {Thread.interrupted()};
        try {
            if (!client.isAlive()) return;
            client.destroy();
            if (awaitExit(client, Duration.ofSeconds(3), interrupted)) return;
            client.destroyForcibly();
            LOG.warn("event=local_cli_force_kill pid={} log={}", client.pid(), log);
            if (!awaitExit(client, Duration.ofSeconds(10), interrupted)) {
                throw new IllegalStateException("CLI child remains alive after bounded force-kill: "
                        + tail(log));
            }
        } finally {
            if (interrupted[0]) Thread.currentThread().interrupt();
        }
    }

    private static boolean awaitExit(Process process, Duration timeout, boolean[] interrupted) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (process.isAlive()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return false;
            try {
                process.waitFor(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException ignored) {
                interrupted[0] = true;
            }
        }
        return true;
    }

    private Exception stopAllOwned(boolean[] interrupted) {
        Exception failure = null;
        for (int index = owned.size() - 1; index >= 0; index--) {
            OwnedProcess owner = owned.get(index);
            try {
                if (owner.process().isAlive()) {
                    owner.process().destroy();
                    if (!awaitExit(owner.process(), Duration.ofSeconds(30), interrupted)) {
                        owner.process().destroyForcibly();
                        LOG.warn("event=local_process_force_kill role={} pid={} log={}",
                                owner.name(), owner.process().pid(), owner.log());
                        if (!awaitExit(owner.process(), Duration.ofSeconds(10), interrupted)) {
                            throw new IllegalStateException(owner.name()
                                    + " is still live after bounded force-kill");
                        }
                        throw new IllegalStateException(owner.name() + " required SIGKILL");
                    }
                }
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        return failure;
    }

    @Override public void close() throws Exception {
        if (closed) return;
        boolean[] interrupted = {Thread.interrupted()};
        try {
            closeOwnersAndAuthorities(interrupted);
        } finally {
            if (interrupted[0]) Thread.currentThread().interrupt();
        }
    }

    private void closeOwnersAndAuthorities(boolean[] interrupted) throws Exception {
        Exception failure = stopAllOwned(interrupted);
        if (owned.stream().anyMatch(owner -> owner.process().isAlive())) {
            preserveDiagnostics = true;
            throw new IllegalStateException(
                    "Owned child JVM is still live; authority and relay retained for retry; "
                            + "diagnostics=" + workspace,
                    failure);
        }
        if (!reportsClosed) {
            try {
                reports.close();
                reportsClosed = true;
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
                interrupted[0] |= Thread.interrupted();
            }
        }
        if (reportsClosed && remoteShuffle != null) {
            try {
                remoteShuffle.close();
                remoteShuffle = null;
            } catch (Exception cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
                interrupted[0] |= Thread.interrupted();
            }
        }
        if (reportsClosed && remoteShuffle == null && !securityClosed) {
            try {
                security.close();
                securityClosed = true;
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (reportsClosed && securityClosed && !tlsClosed) {
            try {
                tls.close();
                tlsClosed = true;
            } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure == null && !preserveDiagnostics) {
            try {
                deleteWorkspace(workspace);
            } catch (IOException cleanup) {
                failure = cleanup;
            }
        }
        if (failure != null) preserveDiagnostics = true;
        else closed = true;
        if (failure != null) {
            failure.addSuppressed(new IllegalStateException(
                    "Local process diagnostics retained at " + workspace));
            throw failure;
        }
    }

    private static void deleteWorkspace(Path root) throws IOException {
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    record QueryResult(String queryId, String output) { }

    record CliOutcome(int exitCode, String output, String errors, JsonNode receipt) { }

    private record OwnedProcess(String name, Process process, Path config, Path log) { }

    private record RegisteredWorker(
            WorkerRegistrationRequest request, long coordinatorEpoch, long receivedNanos) { }
}
