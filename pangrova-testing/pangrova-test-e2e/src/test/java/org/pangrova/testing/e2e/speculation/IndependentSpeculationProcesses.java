package org.pangrova.testing.e2e.speculation;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.testing.PangrovaClusterContainer;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.net.URI;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Sole formal process entry points; every advertised Worker owns an independent JNI runtime. */
final class IndependentSpeculationProcesses implements AutoCloseable {
    private static final String ROLE_TLS_DIRECTORY = "/opt/pangrova/config/process-tls";
    private final PangrovaClusterContainer cluster;
    private final ProcessSecurityAuthority security;
    private final ProcessStatusReportRelay reports;
    private final ExecutorService clientCommands = ManagedExecutorFactory.newSingleThreadExecutor("process-sql-client");
    private final List<WorkerNetwork> networks = new ArrayList<>();
    private final List<AutoCloseable> owners = new ArrayList<>();
    private final java.util.Set<String> processIds = new java.util.LinkedHashSet<>();
    private final Path workspace;
    private final TestTlsAuthority tlsAuthority;
    private final TestTlsAuthority.Identity clientIdentity;
    private final List<TestTlsAuthority.Identity> workerIdentities = new ArrayList<>();
    private ProcessInputSnapshot input;
    private final String expectedImageId;

    IndependentSpeculationProcesses() throws Exception {
        String image = requiredProperty("pangrova.e2e.image");
        if (image.endsWith(":latest")) throw new IllegalArgumentException("An explicit same-source image tag is required");
        expectedImageId = requiredProperty("pangrova.e2e.image-id");
        workspace = Files.createTempDirectory("pangrova-independent-spec-",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        cluster = new PangrovaClusterContainer(DockerImageName.parse(image), 2);
        owners.add(cluster);
        try {
            security = new ProcessSecurityAuthority(cluster.getNetwork());
            owners.add(security);
            reports = new ProcessStatusReportRelay();
            owners.add(reports);
            tlsAuthority = new TestTlsAuthority(workspace);
            owners.add(tlsAuthority);
            clientIdentity = tlsAuthority.issue("result-client", "localhost", false);
            for (int number = 1; number <= 2; number++) {
                workerIdentities.add(tlsAuthority.issue("worker-" + number, "host.testcontainers.internal", false));
            }
            createWorkerNetworks();
            configureProcesses();
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    void start() throws Exception {
        try {
            security.start();
            input = new ProcessInputSnapshot(workspace.resolve("warehouse"), security.hostJdbcUrl());
            owners.add(input);
            var ports = new ArrayList<Integer>();
            ports.add(reports.port());
            ports.add(security.postgresPort());
            networks.forEach(network -> { ports.add(network.http().port()); ports.add(network.result().port()); });
            Testcontainers.exposeHostPorts(ports.stream().mapToInt(Integer::intValue).toArray());
            cluster.start();
            verifyImageIdentities();
            requireSuccessfulSql("CREATE CATALOG spec_catalog WITH ('connector' = 'iceberg', 'type' = 'jdbc', "
                    + "'uri' = '" + security.processJdbcUrl() + "', 'jdbc.user' = 'pangrova', 'jdbc.password' = 'pangrova_test', "
                    + "'jdbc.schema-version' = 'V1', 'warehouse' = '" + input.warehouse().toString().replace("'", "''") + "')");
            requireSuccessfulSql("SELECT 42");
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    ProcessStatusReportRelay reports() { return reports; }
    ProcessInputSnapshot input() { return Objects.requireNonNull(input, "Processes have not started"); }
    List<WorkerNetwork> workers() { return List.copyOf(networks); }
    URI coordinator() { return URI.create(cluster.getCoordinatorUrl()); }
    int activeTenantReservations() {
        return new org.pangrova.security.tenant.quota.JdbcClusterConcurrencySlots(security.dataSource())
                .active(ProcessSecurityAuthority.TENANT);
    }
    org.pangrova.protocol.rest.query.QueryStatusResponse queryStatus(String queryId) {
        try (var client = org.pangrova.client.rest.PangrovaRestClient.builder()
                .coordinatorHost(cluster.getCoordinator().getHost()).coordinatorPort(cluster.getCoordinator().getMappedPort(8080))
                .authentication(org.pangrova.client.rest.auth.ClientAuthentication.basic(
                        ProcessSecurityAuthority.USER, ProcessSecurityAuthority.PASSWORD)).build()) {
            return client.getQueryStatus(queryId);
        }
    }
    java.net.http.HttpResponse<byte[]> replayReport(ProcessStatusReportRelay.Delivery actual) throws Exception {
        var request = java.net.http.HttpRequest.newBuilder(coordinator().resolve("/v1/task/status-report"))
                .header("Authorization", "Bearer independent-process-internal-secret")
                .header("Content-Type", "application/json").timeout(Duration.ofSeconds(30))
                .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(actual.body())).build();
        return java.net.http.HttpClient.newHttpClient().send(request, java.net.http.HttpResponse.BodyHandlers.ofByteArray());
    }

    CompletableFuture<SqlOutput> query(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            try { return executeSql(sql); }
            catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CompletionException(failure);
            } catch (java.io.IOException failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
        }, clientCommands);
    }

    List<IssuedTask> issuedTasks() throws java.io.IOException {
        var tasks = new ArrayList<IssuedTask>();
        for (WorkerNetwork worker : networks) {
            for (var transfer : worker.http().transfers()) {
                if (transfer.isTaskSubmission()) tasks.add(new IssuedTask(worker.workerId(), transfer.submittedTask(), transfer));
            }
        }
        return List.copyOf(tasks);
    }

    private void createWorkerNetworks() throws Exception {
        for (int number = 1; number <= 2; number++) {
            var process = new AtomicReference<GenericContainer<?>>();
            var http = new ProcessWorkerHttpRelay(() -> endpoint(process.get(), httpPort(process.get())));
            owners.add(http);
            var result = new ProcessResultWireRelay(() -> new InetSocketAddress(
                    Objects.requireNonNull(process.get()).getHost(), process.get().getMappedPort(resultPort(process.get()))));
            owners.add(result);
            networks.add(new WorkerNetwork("e2e-worker-" + number, process, http, result));
        }
    }

    private void configureProcesses() {
        cluster.configureCoordinator(container -> container
                .withCommand("start", "coordinator", "--config", "/opt/pangrova/config/process-coordinator.yml")
                .withEnv("PANGROVA_JAVA_OPTS", "-Xms128m -Xmx512m")
                .withEnv("PANGROVA_INTERNAL_AUTH_SECRET", "independent-process-internal-secret")
                .withEnv("PANGROVA_DB_USERNAME", "pangrova").withEnv("PANGROVA_DB_PASSWORD", "pangrova_test")
                .withCopyToContainer(Transferable.of(coordinatorConfig()), "/opt/pangrova/config/process-coordinator.yml")
                .withCopyToContainer(roleMaterial(clientIdentity), ROLE_TLS_DIRECTORY)
                .withFileSystemBind(workspace.resolve("warehouse").toString(),
                        workspace.resolve("warehouse").toString(), BindMode.READ_WRITE));
        cluster.configureWorkers((number, container) -> configureWorker(networks.get(number - 1), container));
    }

    private void configureWorker(WorkerNetwork network, GenericContainer<?> container) {
        if (network == networks.get(0)) reports.connect(endpoint(cluster.getCoordinator(), 8080));
        network.process().set(container);
        container.withCommand("start", "worker", "--config", "/opt/pangrova/config/process-worker.yml")
                .withEnv("PANGROVA_JAVA_OPTS", "-Xms128m -Xmx384m -XX:MaxDirectMemorySize=128m")
                .withEnv("PANGROVA_COORDINATOR_URL", "http://host.testcontainers.internal:" + reports.port())
                .withEnv("PANGROVA_ADVERTISED_HOST", "host.testcontainers.internal")
                .withEnv("PANGROVA_INTERNAL_AUTH_SECRET", "independent-process-internal-secret")
                .withExposedPorts(network.http().port(), network.result().port())
                .withCopyToContainer(Transferable.of(workerConfig(network)), "/opt/pangrova/config/process-worker.yml")
                .withCopyToContainer(roleMaterial(workerIdentities.get(networks.indexOf(network))), ROLE_TLS_DIRECTORY)
                .withFileSystemBind(workspace.resolve("warehouse").toString(),
                        workspace.resolve("warehouse").toString(), BindMode.READ_WRITE)
                .waitingFor(Wait.forHttp("/v1/health").forPort(network.http().port()).forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(120)));
    }

    private String coordinatorConfig() {
        return """
                coordinator:
                  id: e2e-coordinator
                  host: 0.0.0.0
                  port: 8080
                  internal-auth-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"
                  role: QUERY
                  transport:
                    tls:
                      enabled: true
                      certificate-chain-path: "%s"
                      private-key-path: "%s"
                      root-ca-path: "%s"
                  shuffle:
                    tls:
                      server-identity: host.testcontainers.internal
                  speculation:
                    enabled: true
                  ha:
                    enabled: false
                state-store:
                  type: JDBC
                metadata:
                  enabled: true
                  jdbc-url: "%s"
                  jdbc-username: "${PANGROVA_DB_USERNAME}"
                  jdbc-password: "${PANGROVA_DB_PASSWORD}"
                security:
                  enabled: true
                  auth:
                    type: LDAP
                    properties:
                      ldap-url: ldap://spec-directory:389
                      base-dn: dc=pangrova,dc=org
                      user-dn-pattern: uid={0},dc=pangrova,dc=org
                tenant:
                  enabled: true
                """.formatted(ROLE_TLS_DIRECTORY + "/identity.pem",
                        ROLE_TLS_DIRECTORY + "/identity-key.pem",
                        ROLE_TLS_DIRECTORY + "/root.pem", security.processJdbcUrl());
    }

    private String workerConfig(WorkerNetwork network) {
        return """
                worker:
                  port: %d
                  coordinator-url: "${PANGROVA_COORDINATOR_URL}"
                  advertised-host: "${PANGROVA_ADVERTISED_HOST}"
                  internal-auth-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"
                  transport:
                    port: %d
                    tls:
                      enabled: true
                      certificate-chain-path: "%s"
                      private-key-path: "%s"
                      root-ca-path: "%s"
                      identity: host.testcontainers.internal
                  memory:
                    budget-bytes: 536870912
                    node-bytes: 1073741824
                  shuffle:
                    service:
                      spill-directory: "/opt/pangrova/data"
                  native:
                    cache:
                      enabled: false
                """.formatted(network.http().port(), network.result().port(), ROLE_TLS_DIRECTORY + "/identity.pem",
                        ROLE_TLS_DIRECTORY + "/identity-key.pem", ROLE_TLS_DIRECTORY + "/root.pem");
    }

    private Transferable roleMaterial(TestTlsAuthority.Identity identity) {
        List<Path> files = List.of(tlsAuthority.root(), identity.certificateChain(), identity.privateKey());
        List<String> names = List.of("root.pem", "identity.pem", "identity-key.pem");
        return new Transferable() {
            @Override
            public long getSize() {
                long size = 0;
                try {
                    for (Path file : files) size = Math.addExact(size, Files.size(file));
                } catch (IOException failure) {
                    throw new UncheckedIOException("Role TLS material is unavailable", failure);
                }
                return size;
            }

            @Override
            public void transferTo(TarArchiveOutputStream output, String destination) {
                if (!destination.equals(ROLE_TLS_DIRECTORY)) {
                    throw new IllegalArgumentException("Role TLS material has an unexpected destination");
                }
                try {
                    var directory = new TarArchiveEntry(destination + "/");
                    directory.setMode(0700);
                    output.putArchiveEntry(directory);
                    output.closeArchiveEntry();
                    for (int index = 0; index < files.size(); index++) {
                        var entry = new TarArchiveEntry(destination + "/" + names.get(index));
                        entry.setMode(index == 2 ? 0600 : 0444);
                        entry.setSize(Files.size(files.get(index)));
                        output.putArchiveEntry(entry);
                        Files.copy(files.get(index), output);
                        output.closeArchiveEntry();
                    }
                } catch (IOException failure) {
                    throw new UncheckedIOException("Role TLS material could not be staged", failure);
                }
            }
        };
    }

    private SqlOutput executeSql(String sql) throws java.io.IOException, InterruptedException {
        var result = cluster.getCoordinator().execInContainer("bash", "-c",
                "printf '%s;\\n' \"$1\" | /opt/pangrova/bin/pangrova-cli.sh --host localhost --port 8080 "
                        + "--catalog spec_catalog --user \"$2\" --password \"$3\" --query-timeout 5m "
                        + "--result-tls-root-ca \"$4\" --result-tls-certificate-chain \"$5\" --result-tls-private-key \"$6\"",
                "process-sql", sql, ProcessSecurityAuthority.USER, ProcessSecurityAuthority.PASSWORD,
                ROLE_TLS_DIRECTORY + "/root.pem", ROLE_TLS_DIRECTORY + "/identity.pem",
                ROLE_TLS_DIRECTORY + "/identity-key.pem");
        return new SqlOutput(result.getExitCode(), result.getStdout(), result.getStderr());
    }

    private void requireSuccessfulSql(String sql) throws java.io.IOException, InterruptedException {
        SqlOutput result = executeSql(sql);
        if (!result.succeeded()) throw new IllegalStateException("Actual SQL setup failed: " + result.output());
    }

    private void verifyImageIdentities() {
        var processes = new ArrayList<GenericContainer<?>>(cluster.getWorkers());
        processes.add(cluster.getCoordinator());
        for (var process : processes) {
            if (!expectedImageId.equals(process.getContainerInfo().getImageId())) {
                throw new IllegalStateException("Process image differs from the recorded same-source build image");
            }
        }
    }

    private static URI endpoint(GenericContainer<?> container, int port) {
        return URI.create("http://" + Objects.requireNonNull(container).getHost() + ":" + container.getMappedPort(port));
    }
    private int httpPort(GenericContainer<?> container) {
        return networks.stream().filter(network -> network.process().get() == container).findFirst().orElseThrow().http().port();
    }
    private int resultPort(GenericContainer<?> container) {
        return networks.stream().filter(network -> network.process().get() == container).findFirst().orElseThrow().result().port();
    }
    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) throw new IllegalStateException("Required acceptance input: " + name);
        return value;
    }

    @Override public void close() throws Exception {
        if (reports != null) reports.releaseReports();
        retainProcessIdentities();
        Exception failure = null;
        try {
            cluster.close();
            requireProcessExit();
        } catch (RuntimeException cleanupFailure) { failure = cleanupFailure; }
        clientCommands.shutdown();
        try {
            if (!clientCommands.awaitTermination(40, TimeUnit.SECONDS)) throw new IllegalStateException("Actual SQL client is still live");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            if (failure != null) interrupted.addSuppressed(failure);
            failure = interrupted;
        } catch (RuntimeException cleanupFailure) {
            if (failure == null) failure = cleanupFailure;
            else failure.addSuppressed(cleanupFailure);
        }
        if (failure != null) throw failure;
        retireIndependentResources();
        retireAuthorities();
    }

    private void retainProcessIdentities() {
        var processes = new ArrayList<GenericContainer<?>>(cluster.getWorkers());
        processes.add(cluster.getCoordinator());
        for (var process : processes) {
            if (process != null && process.getContainerId() != null) processIds.add(process.getContainerId());
        }
    }

    private void requireProcessExit() {
        if (processIds.isEmpty()) return;
        var docker = org.testcontainers.DockerClientFactory.instance().client();
        for (String id : processIds) {
            try {
                var state = docker.inspectContainerCmd(id).exec().getState();
                if (state == null || !Boolean.FALSE.equals(state.getRunning())) {
                    throw new IllegalStateException("Actual Pangrova process has not exited: " + id);
                }
            } catch (com.github.dockerjava.api.exception.NotFoundException removed) {
                // Docker confirms this exact owned process no longer exists.
            }
        }
    }

    private void retireIndependentResources() throws Exception {
        Exception failure = null;
        for (int index = owners.size() - 1; index >= 1; index--) {
            AutoCloseable owner = owners.get(index);
            if (owner == security || owner == tlsAuthority) continue;
            try {
                owner.close();
                owners.remove(index);
            } catch (Exception cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) throw failure;
    }

    private void retireAuthorities() throws Exception {
        Exception failure = null;
        for (int index = owners.size() - 1; index >= 1; index--) {
            try {
                owners.get(index).close();
                owners.remove(index);
            } catch (Exception cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) throw failure;
    }

    record WorkerNetwork(String workerId, AtomicReference<GenericContainer<?>> process,
            ProcessWorkerHttpRelay http, ProcessResultWireRelay result) { }
    record IssuedTask(String workerId, SubmitTaskRequest request, ProcessWorkerHttpRelay.Transfer transfer) { }
    record SqlOutput(int exitCode, String output, String errors) {
        boolean succeeded() { return exitCode == 0 && !output.contains("Error") && !output.contains("Query failed"); }
    }
}
