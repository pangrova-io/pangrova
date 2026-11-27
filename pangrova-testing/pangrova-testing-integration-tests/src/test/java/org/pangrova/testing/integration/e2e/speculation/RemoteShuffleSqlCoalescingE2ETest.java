package org.pangrova.testing.integration.e2e.speculation;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs a public SQL aggregate through exact committed REMOTE partitions. */
@E2ETest
@ResourceLock("pangrova-shuffle-acceptance-aws-properties")
class RemoteShuffleSqlCoalescingE2ETest extends ContainerTestBase {
    @Test
    void groupedSqlCoalescesFourCommittedPartitionsAndDrainsTheQuery() throws Exception {
        try (var processes = IndependentLocalProcesses.remoteShuffle()) {
            try {
                runQuery(processes);
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Local process diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    private static void runQuery(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        String table = "spec_catalog.default.remote_coalescing";
        processes.executeWithoutRows("CREATE TABLE " + table + " (grp BIGINT, id BIGINT)", ResultProtocol.GRPC_TLS);
        for (int partition = 0; partition < 4; partition++) {
            int key = partition * 10;
            processes.executeWithoutRows("INSERT INTO " + table + " VALUES (0," + key + "),(1," + (key + 1)
                    + "),(1," + (key + 2) + "),(2," + (key + 3) + "),(3," + (key + 4) + ")",
                    ResultProtocol.GRPC_TLS);
        }
        processes.startSecondWorker();
        var result = processes.query("SELECT grp, COUNT(*) AS n FROM " + table + " GROUP BY grp ORDER BY grp",
                ResultProtocol.GRPC_TLS);
        assertThat(result.output().lines().filter(line -> line.startsWith("|")).skip(1)
                .map(line -> Arrays.stream(line.split("\\|", -1)).skip(1).limit(2).map(String::trim).toList())
                .toList()).containsExactly(List.of("0", "4"), List.of("1", "8"), List.of("2", "4"), List.of("3", "4"));

        Awaitility.await("committed ranges and real query drain").atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(processes.coordinatorLog().lines()
                    .filter(line -> line.contains("event=shuffle_input_coalesced queryId=" + result.queryId())))
                    .anySatisfy(line -> assertThat(line).contains("sourcePartitions=4 consumers=1"));
            assertThat(releasedWorkers(processes, result.queryId()))
                    .contains("local-worker-1", "local-worker-2");
            assertThat(processes.status(result.queryId()).drained()).isTrue();
            assertThat(processes.activeTenantReservations()).isZero();
            try (var connection = processes.metadataDataSource().getConnection();
                    var statement = connection.prepareStatement(
                            "SELECT 1 FROM shuffle.shuffle_allocation WHERE query_id = ?")) {
                statement.setString(1, result.queryId());
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isFalse();
                }
            }
            assertThat(processes.remoteShuffleObjectsReleased()).isTrue();
        });
        assertThat(processes.reports().forwardingFailures()).isEmpty();
    }

    private static Set<String> releasedWorkers(IndependentLocalProcesses processes, String queryId)
            throws com.google.protobuf.InvalidProtocolBufferException {
        Set<String> workers = new HashSet<>();
        for (var delivery : processes.reports().deliveries()) {
            var report = delivery.report();
            var response = delivery.response().get();
            if (!queryId.equals(report.queryId()) || report.canonicalTaskReport() == null
                    || response == null || response.statusCode() != 200) continue;
            var canonical = TaskReport.parseFrom(report.canonicalTaskReport());
            if (canonical.getState() == TaskState.TASK_STATE_FINISHED
                    && canonical.getExecutionDrainState() == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
                workers.add(report.workerId());
            }
        }
        return workers;
    }
}
