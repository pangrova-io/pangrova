package org.pangrova.testing.e2e.speculation;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.pangrova.protocol.rest.query.ResultProtocol;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.ai.SharedAiProvider;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Real model I/O from two isolated production Worker JVMs, including joined restart. */
@E2ETest
@Tag("ai-provider")
class IndependentAiProcessesE2ETest extends ContainerTestBase {

    @Test
    void bothNativeWorkersInvokeTheRealModelBeforeAndAfterWorkerRestart() throws Exception {
        try (var provider = new SharedAiProvider()) {
            var deployment = provider.prepare();
            try (var processes = new IndependentLocalProcesses(deployment)) {
                try {
                    processes.startCoordinatorAndFirstWorker();
                    processes.createSourceCatalog();
                    String table = "spec_catalog.default.ai_process_"
                            + UUID.randomUUID().toString().replace("-", "");
                    processes.query("CREATE TABLE " + table + " (id BIGINT, prompt VARCHAR)",
                            ResultProtocol.GRPC_TLS);
                    for (int id = 0; id < 4; id++) {
                        processes.query("INSERT INTO " + table + " VALUES (" + id
                                + ", 'Reply with one word for greeting " + id + ".')", ResultProtocol.GRPC_TLS);
                    }
                    processes.startSecondWorker();
                    assertThat(Set.of(processes.coordinatorPid(), processes.pid("local-worker-1"),
                            processes.pid("local-worker-2"), ProcessHandle.current().pid())).hasSize(4);
                    verifyArithmetic(processes, table);
                    executeAndVerify(processes, provider, table);
                    long previous = processes.pid("local-worker-1");
                    processes.stopAndRestartFirstWorker();
                    assertThat(processes.pid("local-worker-1")).isNotEqualTo(previous);
                    executeAndVerify(processes, provider, table);
                    Awaitility.await("AI task owners released their tenant reservation")
                            .atMost(Duration.ofSeconds(60))
                            .until(() -> processes.activeTenantReservations() == 0);
                    assertThat(processes.reports().forwardingFailures()).isEmpty();
                } catch (Exception | Error failure) {
                    failure.addSuppressed(new IllegalStateException(
                            "AI process diagnostics retained at " + processes.retainDiagnostics()));
                    throw failure;
                }
            }
        }
    }

    private static void verifyArithmetic(IndependentLocalProcesses processes, String table) throws Exception {
        var result = processes.query("SELECT id, id + 2147483647 AS wide_sum, "
                + "2147483647 + id AS reverse_sum, id - 1 AS difference, id * 2 AS product, "
                + "id / 2 AS quotient, id % 2 AS remainder, "
                + "CAST(NULL AS BIGINT) + id AS null_sum, "
                + "CAST(id AS FLOAT) + CAST(16777216 AS DOUBLE) AS float_wide_sum, "
                + "CAST(16777216 AS DOUBLE) + CAST(id AS FLOAT) AS float_reverse_sum "
                + "FROM " + table + " ORDER BY id",
                ResultProtocol.GRPC_TLS);
        var actual = rows(result.output());
        assertThat(actual).hasSize(4);
        for (int id = 0; id < 4; id++) {
            assertThat(actual.get(id)).hasSize(10);
            assertThat(actual.get(id).subList(0, 8)).containsExactly(Integer.toString(id),
                    Long.toString(2147483647L + id), Long.toString(2147483647L + id),
                    Integer.toString(id - 1), Integer.toString(id * 2),
                    Integer.toString(id / 2), Integer.toString(id % 2), "NULL");
            assertThat(Double.parseDouble(actual.get(id).get(8))).isEqualTo(16777216d + id);
            assertThat(Double.parseDouble(actual.get(id).get(9))).isEqualTo(16777216d + id);
        }
        assertThat(processes.status(result.queryId()).state()).isEqualTo("FINISHED");
    }

    private static void executeAndVerify(
            IndependentLocalProcesses processes, SharedAiProvider provider, String table) throws Exception {
        long before = provider.requestCount("/v1/chat/completions");
        var result = processes.query("SELECT id, AI_COMPLETE('" + SharedAiProvider.LOGICAL_MODEL
                + "', prompt) AS response FROM " + table, ResultProtocol.GRPC_TLS);
        var rows = rows(result.output());
        assertThat(rows).hasSize(4);
        assertThat(rows).extracting(row -> row.get(0)).containsExactlyInAnyOrder("0", "1", "2", "3");
        rows.forEach(row -> assertThat(row.get(1)).isNotBlank().isNotEqualTo("NULL"));
        assertProviderRequests(provider, before, 4);
        assertThat(processes.status(result.queryId()).state()).isEqualTo("FINISHED");
        assertAuditAttribution(processes, result.queryId());

        // Tiny scans correctly use one task. Two aggregate inputs require real
        // exchange boundaries without overriding cardinalities or the scheduler.
        before = provider.requestCount("/v1/chat/completions");
        var distributed = processes.query(distributedSql(table), ResultProtocol.GRPC_TLS);
        var pairs = rows(distributed.output());
        assertThat(pairs).hasSize(2);
        assertThat(pairs).extracting(row -> List.of(row.get(0), row.get(2)))
                .containsExactlyInAnyOrder(List.of("0", "2"), List.of("1", "3"));
        pairs.forEach(row -> {
            assertThat(row.get(1)).isNotBlank().isNotEqualTo("NULL");
            assertThat(row.get(3)).isNotBlank().isNotEqualTo("NULL");
            assertThat(row.get(4)).isNotBlank().isNotEqualTo("NULL");
        });
        assertProviderRequests(provider, before, 6);
        Awaitility.await("both independent Native AI owners returned their physical-call evidence")
                .pollInterval(Duration.ofMillis(200)).atMost(Duration.ofSeconds(60))
                .until(() -> aiWorkers(processes, distributed.queryId())
                        .equals(Set.of("local-worker-1", "local-worker-2")));
        assertThat(processes.status(distributed.queryId()).state()).isEqualTo("FINISHED");
        assertAuditAttribution(processes, distributed.queryId());
    }

    private static void assertAuditAttribution(IndependentLocalProcesses processes, String queryId) {
        Awaitility.await("production terminal audit keeps its authenticated query subject")
                .atMost(Duration.ofSeconds(5)).until(() -> processes.hasAttributedQueryAudit(queryId));
    }

    private static String distributedSql(String table) {
        String projection = "SELECT id, MAX(AI_COMPLETE('" + SharedAiProvider.LOGICAL_MODEL
                + "', prompt)) AS response FROM " + table;
        return "SELECT a.id, a.response, b.id AS paired_id, b.response AS paired_response, AI_COMPLETE('"
                + SharedAiProvider.LOGICAL_MODEL + "', a.response) AS chained_response FROM ("
                + projection + " WHERE id < 2 GROUP BY id) a JOIN ("
                + projection + " WHERE id >= 2 GROUP BY id) b ON a.id + 2 = b.id";
    }

    private static List<List<String>> rows(String output) {
        return output.lines().filter(line -> line.startsWith("|")).skip(1)
                .map(line -> line.split("\\|", -1))
                .map(cells -> Arrays.stream(cells, 1, cells.length - 1).map(String::trim).toList())
                .toList();
    }

    private static void assertProviderRequests(SharedAiProvider provider, long before, int expected) {
        Awaitility.await("real model completed exactly the query's physical calls")
                .atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(provider.requestCount("/v1/chat/completions")).isEqualTo(before + expected));
    }

    private static Set<String> aiWorkers(IndependentLocalProcesses processes, String queryId) throws Exception {
        Set<String> workers = new HashSet<>();
        for (var delivery : processes.reports().deliveries()) {
            var report = delivery.report();
            if (!queryId.equals(report.queryId()) || report.canonicalTaskReport() == null
                    || delivery.response().get() == null || delivery.response().get().statusCode() != 200) continue;
            TaskReport canonical = TaskReport.parseFrom(report.canonicalTaskReport());
            if (canonical.getState() == TaskState.TASK_STATE_FINISHED
                    && canonical.getExecutionDrainState()
                            == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED
                    && canonical.getAttempt().getTaskId().equals(report.taskId())
                    && canonical.getAttempt().getQueryOrJobId().equals(queryId)
                    && canonical.getAttempt().getStageId() == report.stageId()
                    && canonical.getAttempt().getPartitionId() == report.partitionId()
                    && canonical.getUsage().getProviderAttempts() > 0
                    && canonical.getUsage().getProviderItems() > 0
                    && canonical.getUsage().getProviderRequestBytes() > 0
                    && canonical.getUsage().getProviderResponseBytes() > 0) {
                workers.add(report.workerId());
            }
        }
        return workers;
    }
}
