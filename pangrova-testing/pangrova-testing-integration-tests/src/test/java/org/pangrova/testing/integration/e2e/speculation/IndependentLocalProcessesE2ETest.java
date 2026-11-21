package org.pangrova.testing.integration.e2e.speculation;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Public SQL/result transport against one Coordinator JVM and two independent Native Worker JVMs. */
@E2ETest
class IndependentLocalProcessesE2ETest extends ContainerTestBase {
    private static IndependentLocalProcesses processes;
    private static String orders;
    private static String customers;
    private static String regions;

    @BeforeAll
    static void start() throws Exception {
        processes = new IndependentLocalProcesses();
        try {
            processes.startCoordinatorAndFirstWorker();
            String suffix = UUID.randomUUID().toString().replace("-", "");
            orders = "spec_catalog.default.local_orders_" + suffix;
            customers = "spec_catalog.default.local_customers_" + suffix;
            regions = "spec_catalog.default.local_regions_" + suffix;
            processes.createSourceCatalog();
            seedTables();
            processes.startSecondWorker();
        } catch (Exception | Error failure) {
            failure.addSuppressed(new IllegalStateException(
                    "Local process diagnostics retained at " + processes.retainDiagnostics()));
            throw failure;
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (processes != null) processes.close();
    }

    @Test
    void twoWorkerShuffleAndGracefulWorkerRestartUseRealProcesses() throws Exception {
        try {
            exerciseTwoWorkerAndRestart();
        } catch (Exception | Error failure) {
            failure.addSuppressed(new IllegalStateException(
                    "Local process diagnostics retained at " + processes.retainDiagnostics()));
            throw failure;
        }
    }

    private static void exerciseTwoWorkerAndRestart() throws Exception {
        long coordinatorPid = processes.coordinatorPid();
        long firstPid = processes.pid("local-worker-1");
        long secondPid = processes.pid("local-worker-2");
        assertThat(Set.of(coordinatorPid, firstPid, secondPid, ProcessHandle.current().pid()))
                .hasSize(4);

        var baseline = processes.query(joinSql(), ResultProtocol.GRPC_TLS);
        assertExactRows(baseline.output());
        assertBothWorkersFinishedAndDrained(baseline.queryId());

        processes.stopAndRestartFirstWorker();
        assertThat(processes.coordinatorPid()).isEqualTo(coordinatorPid);
        assertThat(processes.pid("local-worker-1")).isNotEqualTo(firstPid);
        assertThat(processes.pid("local-worker-2")).isEqualTo(secondPid);
        var afterRestart = processes.query(joinSql(), ResultProtocol.GRPC_TLS);
        assertExactRows(afterRestart.output());
        assertBothWorkersFinishedAndDrained(afterRestart.queryId());
        Awaitility.await("persisted tenant query owner release")
                .atMost(Duration.ofSeconds(60))
                .until(() -> processes.activeTenantReservations() == 0);
        assertThat(processes.reports().forwardingFailures()).isEmpty();
    }

    private static void assertBothWorkersFinishedAndDrained(String queryId) {
        Awaitility.await("two real Native Worker terminal receipts for " + queryId)
                .pollInterval(Duration.ofMillis(200)).atMost(Duration.ofSeconds(60))
                .until(() -> {
                    Set<String> workers = new HashSet<>();
                    Set<Integer> stages = new HashSet<>();
                    for (var delivery : processes.reports().deliveries()) {
                        var report = delivery.report();
                        if (!queryId.equals(report.queryId())
                                || !"FINISHED".equals(report.state())
                                || report.canonicalTaskReport() == null
                                || delivery.response().get() == null
                                || delivery.response().get().statusCode() != 200) continue;
                        TaskReport canonical = TaskReport.parseFrom(report.canonicalTaskReport());
                        if (!canonical.getAttempt().getTaskId().equals(report.taskId())
                                || !canonical.getAttempt().getQueryOrJobId().equals(queryId)
                                || canonical.getAttempt().getStageId() != report.stageId()
                                || canonical.getAttempt().getPartitionId() != report.partitionId()
                                || canonical.getState() != TaskState.TASK_STATE_FINISHED
                                || canonical.getExecutionDrainState()
                                        != ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
                            continue;
                        }
                        workers.add(report.workerId());
                        stages.add(report.stageId());
                    }
                    return workers.containsAll(Set.of("local-worker-1", "local-worker-2"))
                            && stages.size() >= 2;
                });
        assertThat(processes.status(queryId).state()).isEqualTo("FINISHED");
    }

    private static void assertExactRows(String output) {
        List<List<String>> rows = new ArrayList<>();
        boolean headerSeen = false;
        for (String line : output.split("\\R")) {
            if (!line.startsWith("|")) continue;
            if (!headerSeen) {
                headerSeen = true;
                continue;
            }
            String[] cells = line.split("\\|", -1);
            List<String> values = Arrays.stream(cells, 1, cells.length - 1)
                    .map(String::trim).toList();
            rows.add(values);
        }
        assertThat(output).contains("10 rows");
        assertThat(rows).hasSize(10);
        for (int index = 0; index < 10; index++) {
            int id = 79 - index;
            assertThat(rows.get(index)).containsExactly(Integer.toString(id),
                    Integer.toString(id), "label-" + id, "region-" + (id % 4));
        }
    }

    private static String joinSql() {
        return "SELECT o.id, MAX(o.score) AS score, c.label, r.region FROM "
                + orders + " o JOIN " + customers + " c ON o.id = c.id "
                + "JOIN " + regions + " r ON o.id = r.id "
                + "GROUP BY o.id, c.label, r.region ORDER BY score DESC LIMIT 10";
    }

    private static void seedTables() throws Exception {
        processes.query("CREATE TABLE " + orders + " (id BIGINT, score BIGINT)", ResultProtocol.GRPC_TLS);
        processes.query("CREATE TABLE " + customers + " (id BIGINT, label VARCHAR)", ResultProtocol.GRPC_TLS);
        processes.query("CREATE TABLE " + regions + " (id BIGINT, region VARCHAR)", ResultProtocol.GRPC_TLS);
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
            processes.query(ordersInsert.toString(), ResultProtocol.GRPC_TLS);
            processes.query(customersInsert.toString(), ResultProtocol.GRPC_TLS);
            processes.query(regionsInsert.toString(), ResultProtocol.GRPC_TLS);
        }
    }
}
