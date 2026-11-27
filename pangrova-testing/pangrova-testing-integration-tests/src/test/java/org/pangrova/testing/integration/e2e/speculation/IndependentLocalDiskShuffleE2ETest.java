package org.pangrova.testing.integration.e2e.speculation;

import java.time.Duration;
import java.util.HashSet;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SQL, local spill producers and consumers on a cluster with two eligible physical JVMs. */
@E2ETest
class IndependentLocalDiskShuffleE2ETest extends ContainerTestBase {
    @Test
    void diskBackedShuffleBindsEveryStageOfTheQueryToOneIncarnation() throws Exception {
        try (var cluster = IndependentLocalProcesses.diskShuffle()) {
            cluster.startCoordinatorAndFirstWorker();
            cluster.startSecondWorker();
            cluster.createSourceCatalog();
            String table = "spec_catalog.default.local_disk_" + UUID.randomUUID().toString().replace("-", "");
            cluster.executeWithoutRows("CREATE TABLE " + table + " (id BIGINT, amount BIGINT)", ResultProtocol.GRPC_TLS);
            cluster.executeWithoutRows("INSERT INTO " + table + " VALUES (1, 5), (2, 17)", ResultProtocol.GRPC_TLS);
            cluster.executeWithoutRows("INSERT INTO " + table + " VALUES (1, 9), (2, 11)", ResultProtocol.GRPC_TLS);
            var result = cluster.query("SELECT l.id, SUM(l.amount * r.amount) AS total FROM " + table
                    + " l JOIN " + table + " r ON l.id = r.id GROUP BY l.id ORDER BY l.id", ResultProtocol.GRPC_TLS);
            assertThat(result.output()).containsPattern("\\|\\s*1\\s*\\|\\s*196\\s*\\|")
                    .containsPattern("\\|\\s*2\\s*\\|\\s*784\\s*\\|");
            Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                var stages = new HashSet<Integer>();
                var incarnations = new HashSet<String>();
                for (var delivery : cluster.reports().deliveries()) {
                    var report = delivery.report();
                    if (!report.queryId().equals(result.queryId()) || report.canonicalTaskReport() == null
                            || delivery.response().get() == null || delivery.response().get().statusCode() != 200) continue;
                    var terminal = TaskReport.parseFrom(report.canonicalTaskReport());
                    if (terminal.getState() != TaskState.TASK_STATE_FINISHED) continue;
                    var attempt = terminal.getAttempt();
                    assertThat(attempt.getAttemptId()).isZero();
                    assertThat(attempt.getWorkerExecutionIdentity().getIncarnationId()).isNotBlank();
                    stages.add(attempt.getStageId());
                    incarnations.add(report.workerId() + "/" + attempt.getWorkerExecutionIdentity().getIncarnationId());
                }
                assertThat(stages.size()).isGreaterThanOrEqualTo(2);
                assertThat(incarnations).hasSize(1);
            });
        }
    }
}
