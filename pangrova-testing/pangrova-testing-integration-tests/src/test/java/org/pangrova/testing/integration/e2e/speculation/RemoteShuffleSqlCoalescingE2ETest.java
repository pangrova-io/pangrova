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
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs public SQL through exact committed REMOTE shuffle ranges. */
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

    @Test
    void fullHashJoinCoalescesBothInputsWithoutLosingDuplicatesOrUnmatchedRows() throws Exception {
        try (var processes = IndependentLocalProcesses.remoteShuffle()) {
            try {
                verifyFullJoin(processes);
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Local process diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    private static void verifyFullJoin(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        String left = "spec_catalog.default.coalesced_join_left";
        String right = "spec_catalog.default.coalesced_join_right";
        seedJoinInput(processes, left, 100, 2);
        seedJoinInput(processes, right, 200, 3);
        processes.startSecondWorker();

        var result = processes.query("SELECT COALESCE(l.payload, 0) AS lp, COALESCE(r.payload, 0) AS rp "
                + "FROM " + left + " l FULL OUTER JOIN " + right + " r ON l.id = r.id ORDER BY lp, rp",
                ResultProtocol.GRPC_TLS);
        assertThat(resultRows(result.output())).containsExactly(
                List.of("0", "203"), List.of("0", "204"), List.of("101", "201"), List.of("101", "202"),
                List.of("102", "201"), List.of("102", "202"), List.of("103", "0"), List.of("104", "0"));

        var decision = Pattern.compile("event=shuffle_input_coalesced queryId=" + Pattern.quote(result.queryId())
                + " stageId=(\\d+) sourcePartitions=4 consumers=1 inputs=2");
        Awaitility.await("one aligned join consumer and full drain").atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var matcher = decision.matcher(processes.coordinatorLog());
            assertThat(matcher.find()).isTrue();
            int joinStage = Integer.parseInt(matcher.group(1));
            assertThat(releasedStagePartitions(processes, result.queryId(), joinStage)).containsExactly(0);
            assertThat(releasedWorkers(processes, result.queryId())).contains("local-worker-1", "local-worker-2");
            assertThat(processes.status(result.queryId()).drained()).isTrue();
            assertThat(processes.activeTenantReservations()).isZero();
            assertThat(processes.remoteShuffleObjectsReleased()).isTrue();
        });
        assertThat(processes.reports().forwardingFailures()).isEmpty();
    }

    private static void seedJoinInput(IndependentLocalProcesses processes, String table, int base, int unmatched)
            throws Exception {
        processes.command("CREATE TABLE " + table + " (id BIGINT, payload BIGINT)", ResultProtocol.GRPC_TLS);
        var keys = List.of("1", "1", Integer.toString(unmatched), "NULL");
        for (int row = 0; row < keys.size(); row++) {
            processes.command("INSERT INTO " + table + " VALUES (" + keys.get(row) + "," + (base + row + 1) + ")",
                    ResultProtocol.GRPC_TLS);
        }
    }

    private static List<List<String>> resultRows(String output) {
        return output.lines().filter(line -> line.startsWith("|")).skip(1)
                .map(line -> Arrays.stream(line.split("\\|", -1)).skip(1).limit(2).map(String::trim).toList())
                .toList();
    }

    private static Set<Integer> releasedStagePartitions(IndependentLocalProcesses processes, String queryId, int stageId)
            throws com.google.protobuf.InvalidProtocolBufferException {
        var partitions = new HashSet<Integer>();
        for (var delivery : processes.reports().deliveries()) {
            var report = delivery.report();
            var response = delivery.response().get();
            if (!queryId.equals(report.queryId()) || report.canonicalTaskReport() == null
                    || response == null || response.statusCode() != 200) continue;
            var canonical = TaskReport.parseFrom(report.canonicalTaskReport());
            if (canonical.getAttempt().getStageId() == stageId && canonical.getState() == TaskState.TASK_STATE_FINISHED
                    && canonical.getExecutionDrainState() == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
                partitions.add(canonical.getAttempt().getPartitionId());
            }
        }
        return partitions;
    }

    private static void runQuery(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        String table = "spec_catalog.default.remote_coalescing";
        processes.command("CREATE TABLE " + table + " (grp BIGINT, id BIGINT)", ResultProtocol.GRPC_TLS);
        for (int partition = 0; partition < 4; partition++) {
            int key = partition * 10;
            processes.command("INSERT INTO " + table + " VALUES (0," + key + "),(1," + (key + 1)
                    + "),(1," + (key + 2) + "),(2," + (key + 3) + "),(3," + (key + 4) + ")",
                    ResultProtocol.GRPC_TLS);
        }
        processes.startSecondWorker();
        var result = processes.query("SELECT grp, COUNT(*) AS n FROM " + table + " GROUP BY grp ORDER BY grp",
                ResultProtocol.GRPC_TLS);
        assertThat(resultRows(result.output())).containsExactly(List.of("0", "4"), List.of("1", "8"), List.of("2", "4"), List.of("3", "4"));

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
