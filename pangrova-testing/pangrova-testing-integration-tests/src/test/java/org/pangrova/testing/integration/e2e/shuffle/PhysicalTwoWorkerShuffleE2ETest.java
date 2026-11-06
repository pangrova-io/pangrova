package org.pangrova.testing.integration.e2e.shuffle;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL-to-Native shuffle across two distinct Worker JVMs and result servers. */
class PhysicalTwoWorkerShuffleE2ETest extends SqlE2ETestBase {

    private static final String SECOND_WORKER_ID = "e2e-worker-2-process";
    private static final long MIN_TASK_MEMORY_BYTES = 64L * 1024 * 1024;
    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private static Process secondWorker;
    private static Path workerConfig;
    private static Path workerLog;
    private static Path workerSpillDirectory;
    private static String orders;
    private static String customers;
    private static String regions;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void startPhysicalSecondWorker() throws Exception {
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString())));
        String suffix = Long.toHexString(System.nanoTime());
        orders = CATALOG + "." + SCHEMA + ".two_worker_orders_" + suffix;
        customers = CATALOG + "." + SCHEMA + ".two_worker_customers_" + suffix;
        regions = CATALOG + "." + SCHEMA + ".two_worker_regions_" + suffix;
        seedTablesBeforeSecondWorkerJoins();

        workerConfig = Files.createTempFile("pangrova-physical-worker-", ".yml");
        workerLog = Files.createTempFile("pangrova-physical-worker-", ".log");
        workerSpillDirectory = Files.createTempDirectory("pangrova-physical-shuffle-spill-");
        String routableHost = workerAdvertisedHost();
        assertThat(InetAddress.getByName(routableHost).isLoopbackAddress())
                .as("both physical Workers must advertise a peer-reachable host")
                .isFalse();
        Files.writeString(workerConfig, """
                worker:
                  id: "%s"
                  host: "%s"
                  advertised-host: "%s"
                  port: 0
                  coordinator-url: "http://127.0.0.1:%d"
                  internal-auth-secret: "%s"
                  transport:
                    port: 0
                  shuffle:
                    service:
                      spill-directory: "%s"
                  cdc:
                    enabled: false
                """.formatted(SECOND_WORKER_ID, routableHost, routableHost,
                coordinatorHttpPort(),
                E2E_INTERNAL_AUTH_SECRET, workerSpillDirectory.toAbsolutePath()));
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        // Surefire may expose only its booter JAR as java.class.path. The
        // forked Worker needs the resolved test/dependency classpath instead.
        String childClasspath = System.getProperty(
                "surefire.test.class.path", System.getProperty("java.class.path"));
        secondWorker = new ProcessBuilder(
                java.toString(),
                "-Xmx768m", "-XX:MaxDirectMemorySize=768m",
                "-Dio.netty.maxDirectMemory=0",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", childClasspath,
                "org.pangrova.worker.bootstrap.PangrovaWorkerMain",
                "--config", workerConfig.toString())
                .redirectErrorStream(true)
                .redirectOutput(workerLog.toFile())
                .start();
        Awaitility.await("second physical Worker resource heartbeat")
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    if (!secondWorker.isAlive()) {
                        throw new IllegalStateException("Second Worker exited: " + workerLogTail());
                    }
                    WorkerInfo worker = workerManager.getWorker(SECOND_WORKER_ID);
                    return worker != null
                            && worker.healthState() == HealthState.ACTIVE
                            && worker.availableMemoryBytes() >= MIN_TASK_MEMORY_BYTES;
                });
        WorkerInfo first = workerManager.getWorker(WORKER_ID);
        WorkerInfo second = workerManager.getWorker(SECOND_WORKER_ID);
        assertThat(secondWorker.pid()).isNotEqualTo(ProcessHandle.current().pid());
        assertThat(first.httpPort()).isNotEqualTo(second.httpPort());
        assertThat(first.resultServerPort()).isNotEqualTo(second.resultServerPort());
        assertThat(first.resultServerPort()).isPositive();
        assertThat(second.resultServerPort()).isPositive();
        assertThat(second.availableMemoryBytes()).isGreaterThanOrEqualTo(
                MIN_TASK_MEMORY_BYTES);
    }

    @AfterAll
    static void stopPhysicalSecondWorker() throws Exception {
        if (secondWorker != null && secondWorker.isAlive()) {
            secondWorker.destroy();
            if (!secondWorker.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
                secondWorker.destroyForcibly();
                secondWorker.waitFor();
            }
        }
        if (workerConfig != null) {
            Files.deleteIfExists(workerConfig);
        }
        if (workerLog != null) {
            Files.deleteIfExists(workerLog);
        }
        if (workerSpillDirectory != null) {
            Files.deleteIfExists(workerSpillDirectory);
        }
    }

    @Test
    void joinAndGlobalTopKUseBothPhysicalWorkers() {
        Set<String> before = queryManager.getStateMachines().keySet();
        String sql = "SELECT o.id, MAX(o.score) AS score, c.label, r.region FROM "
                        + orders + " o JOIN " + customers + " c ON o.id = c.id "
                        + "JOIN " + regions + " r ON o.id = r.id "
                        + "GROUP BY o.id, c.label, r.region "
                        + "ORDER BY score DESC LIMIT 10";
        List<TypedRow> rows;
        try {
            rows = sqlRunner.execute(sql);
        } catch (RuntimeException failure) {
            StringBuilder tasks = new StringBuilder();
            Set<String> failedQueries = new HashSet<>(queryManager.getStateMachines().keySet());
            failedQueries.removeAll(before);
            for (String queryId : failedQueries) {
                tasks.append("query=").append(queryId).append(" stages=")
                        .append(queryManager.getStateMachines().get(queryId).getStageStates());
                for (String taskId : taskTracker.getTaskIds(queryId)) {
                    tasks.append(" task=").append(taskId)
                            .append(" worker=").append(taskTracker.getWorkerForTask(taskId))
                            .append(" state=").append(taskTracker.getTaskState(queryId, taskId));
                }
            }
            throw new AssertionError("Physical Worker query failed; second Worker log tail:\n"
                    + workerLogTail() + "\nCoordinator task snapshot: " + tasks, failure);
        }
        assertThat(rows).hasSize(10);
        for (int index = 0; index < 10; index++) {
            assertThat(rows.get(index).getLong("id")).isEqualTo(79L - index);
            assertThat(rows.get(index).getLong("score")).isEqualTo(79L - index);
            assertThat(rows.get(index).getString("region"))
                    .isEqualTo("region-" + ((79 - index) % 4));
        }
        assertThat(secondWorker.isAlive())
                .as("second physical Worker must remain alive through the query")
                .isTrue();

        Set<String> newQueries = new HashSet<>(queryManager.getStateMachines().keySet());
        newQueries.removeAll(before);
        assertThat(newQueries).hasSize(1);
        String queryId = newQueries.iterator().next();
        assertThat(queryManager.getStateMachines().get(queryId).getStageStates())
                .as("public SQL must plan multiple distributed stages")
                .hasSizeGreaterThanOrEqualTo(2);
        Set<String> taskWorkers = new HashSet<>();
        for (String taskId : taskTracker.getTaskIds(queryId)) {
            taskWorkers.add(taskTracker.getWorkerForTask(taskId));
        }
        assertThat(taskWorkers).contains(WORKER_ID, SECOND_WORKER_ID);
        Awaitility.await("both physical Workers release every issued task")
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(100))
                .ignoreExceptionsInstanceOf(IllegalStateException.class)
                .untilAsserted(() -> taskTracker.requireQueryExecutionDrained(queryId));
    }

    private static void seedTablesBeforeSecondWorkerJoins() {
        sqlRunner.executeDdl("CREATE TABLE " + orders + " (id BIGINT, score BIGINT)");
        sqlRunner.executeDdl("CREATE TABLE " + customers + " (id BIGINT, label VARCHAR)");
        sqlRunner.executeDdl("CREATE TABLE " + regions + " (id BIGINT, region VARCHAR)");
        for (int file = 0; file < 8; file++) {
            StringBuilder ordersInsert = new StringBuilder("INSERT INTO " + orders + " VALUES ");
            StringBuilder customersInsert = new StringBuilder("INSERT INTO " + customers + " VALUES ");
            StringBuilder regionsInsert = new StringBuilder("INSERT INTO " + regions + " VALUES ");
            for (int offset = 0; offset < 10; offset++) {
                int id = file * 10 + offset;
                if (offset > 0) {
                    ordersInsert.append(',');
                    customersInsert.append(',');
                    regionsInsert.append(',');
                }
                ordersInsert.append('(').append(id).append(',').append(id).append(')');
                customersInsert.append('(').append(id).append(", 'label-")
                        .append(id).append("')");
                regionsInsert.append('(').append(id).append(", 'region-")
                        .append(id % 4).append("')");
            }
            sqlRunner.execute(ordersInsert.toString());
            sqlRunner.execute(customersInsert.toString());
            sqlRunner.execute(regionsInsert.toString());
        }
    }

    private static String workerLogTail() {
        try {
            String contents = Files.readString(workerLog);
            return contents.substring(Math.max(0, contents.length() - 20000));
        } catch (IOException failure) {
            return failure.toString();
        }
    }

}
