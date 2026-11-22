package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Public clients cancel Coordinator-owned queries while an authenticated Worker terminal report is held. */
@E2ETest
class IndependentClientCancellationE2ETest extends ContainerTestBase {
    @Test void jdbcCancelAndCliDeadlineCancelTheirExactQueries() throws Exception {
        try (var cluster = new IndependentLocalProcesses(false)) {
            cluster.startCoordinatorAndFirstWorker();
            cluster.startSecondWorker();
            var executor = Executors.newFixedThreadPool(2);
            try {
                try (var connection = cluster.openJdbc(); var statement = connection.createStatement()) {
                    var first = new CompletableFuture<String>();
                    try (var hold = holdNewQuery(cluster, first, false)) {
                        var running = executor.submit(() -> statement.executeQuery("SELECT 41 + 1"));
                        String id = first.get(60, TimeUnit.SECONDS);
                        assertThat(cluster.status(id).state()).isEqualTo("RUNNING");
                        assertHeldTask(cluster, id);
                        var cancel = executor.submit(() -> {
                            statement.cancel();
                            return null;
                        });
                        assertCancelled(cluster, id);
                        hold.close();
                        cancel.get(60, TimeUnit.SECONDS);
                        assertCancellationAudit(cluster, id, "SELECT 41 + 1");
                        assertThatThrownBy(() -> running.get(60, TimeUnit.SECONDS))
                                .hasRootCauseInstanceOf(SQLException.class).hasStackTraceContaining(id);
                    }
                    try (var rows = statement.executeQuery("SELECT 42")) {
                        assertThat(rows.next()).isTrue();
                        assertThat(rows.getInt(1)).isEqualTo(42);
                        assertThat(rows.next()).isFalse();
                    }
                }
                var second = new CompletableFuture<String>();
                try (var hold = holdNewQuery(cluster, second, true)) {
                    var deadline = executor.submit(() -> cluster.executeSql(
                            "SELECT 40 + 2", ResultProtocol.GRPC_TLS, Duration.ofSeconds(15)));
                    String id = second.get(60, TimeUnit.SECONDS);
                    assertHeldTask(cluster, id);
                    assertThat(cluster.activeTenantReservations()).isPositive();
                    assertCancelled(cluster, id);
                    // Hold the original FINISHED delivery while the Worker retries through the real endpoint.
                    await().atMost(Duration.ofSeconds(80)).until(() -> cluster.reports().deliveries().stream()
                            .filter(delivery -> id.equals(delivery.report().queryId())
                                    && "FINISHED".equals(delivery.report().state()))
                            .count() >= 2);
                    // Wait for the actual Worker retry, seal and physical release before forwarding the late packet.
                    await().atMost(Duration.ofSeconds(80)).until(() -> cluster.activeTenantReservations() == 0);
                    hold.close();
                    awaitRetiredResultDelivery(cluster, id);
                    var outcome = deadline.get(60, TimeUnit.SECONDS);
                    assertThat(outcome.exitCode()).isNotZero();
                    assertThat(outcome.receipt()).isNull();
                    assertCancellationAudit(cluster, id, "SELECT 40 + 2");
                    assertThat(outcome.errors()).contains(id).containsIgnoringCase("timed out");
                }
            } finally {
                executor.shutdownNow();
            }
            assertThat(cluster.query("SELECT 42", ResultProtocol.GRPC_TLS).output()).contains("42");
        }
    }

    private static ProcessStatusReportRelay.ReportHold holdNewQuery(IndependentLocalProcesses cluster,
                                                                    CompletableFuture<String> selected,
                                                                    boolean holdFirstDeliveryOnly) {
        // Previous Worker receipts may be retried while the following client submits its SQL.
        var held = new java.util.concurrent.atomic.AtomicBoolean();
        var previous = new HashSet<String>();
        cluster.reports().deliveries().forEach(delivery -> previous.add(delivery.report().queryId()));
        return cluster.reports().holdReports(report -> {
            if (!"FINISHED".equals(report.state()) || previous.contains(report.queryId())) return false;
            selected.complete(report.queryId());
            return report.queryId().equals(selected.getNow(null))
                    && (!holdFirstDeliveryOnly || held.compareAndSet(false, true));
        });
    }

    private static void awaitRetiredResultDelivery(IndependentLocalProcesses cluster, String id) {
        await().atMost(Duration.ofSeconds(65)).untilAsserted(() -> {
            var receipts = new java.util.ArrayList<String>();
            for (var observed : cluster.reports().deliveries()) {
                var report = observed.report();
                var response = observed.response().get();
                if (!id.equals(report.queryId()) || !"FINISHED".equals(report.state())
                        || response == null || response.statusCode() != 200) continue;
                var canonical = TaskReport.parseFrom(report.canonicalTaskReport());
                var ack = org.pangrova.protocol.http.TaskStatusAck.parseFrom(response.body());
                assertThat(ack.getTaskId()).isEqualTo(report.taskId());
                assertThat(ack.getSequence()).isEqualTo(report.reportSequence());
                receipts.add(canonical.getExecutionDrainState().name() + "/" + ack.getDirective().name());
            }
            assertThat(receipts).as("terminal acknowledgements for %s", id).contains(
                    "EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED/CONTINUE",
                    "EXECUTION_DRAIN_STATE_UNSPECIFIED/" + org.pangrova.protocol.http.TaskStatusAck.Directive.RETIRED_RESULT_NOTICE.name());
        });
    }

    private static void assertHeldTask(IndependentLocalProcesses cluster, String id) throws Exception {
        for (var observed : cluster.reports().deliveries()) {
            var report = observed.report();
            if (!id.equals(report.queryId()) || !"FINISHED".equals(report.state())) continue;
            var canonical = TaskReport.parseFrom(report.canonicalTaskReport());
            assertThat(canonical.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(canonical.getAttempt().getQueryOrJobId()).isEqualTo(id);
            assertThat(observed.response().get()).isNull();
            return;
        }
        throw new AssertionError("Expected a held authenticated terminal report for " + id);
    }

    private static void assertCancellationAudit(IndependentLocalProcesses cluster, String id, String sql) {
        // The query history and the asynchronous security writer have independent PostgreSQL commits.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            try (var db = cluster.metadataDataSource().getConnection();
                 var record = db.prepareStatement("""
                         SELECT user_id, tenant_id, sql_text, wall_time_ms, completed_at, success
                         FROM query_audit_records WHERE query_id = ?
                         """);
                 var events = db.prepareStatement("""
                         SELECT event_type, principal, properties::jsonb ->> 'tenant_id' AS tenant_id,
                                sql_text, success, duration_ms, event_time
                         FROM audit_logs WHERE query_id = ? AND event_type LIKE 'QUERY_%'
                         ORDER BY event_time, event_id
                         """)) {
                record.setString(1, id);
                try (var row = record.executeQuery()) {
                    assertThat(row.next()).as("durable query audit %s", id).isTrue();
                    assertThat(row.getString("user_id")).isEqualTo(ProcessSecurityAuthority.USER);
                    assertThat(row.getString("tenant_id")).isEqualTo(ProcessSecurityAuthority.TENANT);
                    assertThat(row.getString("sql_text")).isEqualTo(sql);
                    assertThat(row.getLong("wall_time_ms")).isNotNegative();
                    assertThat(row.getTimestamp("completed_at")).isNotNull();
                    assertThat(row.getObject("success")).isEqualTo(false);
                    assertThat(row.next()).isFalse();
                }
                events.setString(1, id);
                var types = new java.util.ArrayList<String>();
                try (var row = events.executeQuery()) {
                    while (row.next()) {
                        String type = row.getString("event_type");
                        types.add(type);
                        assertThat(row.getString("principal")).isEqualTo(ProcessSecurityAuthority.USER);
                        assertThat(row.getTimestamp("event_time")).isNotNull();
                        if ("QUERY_SUBMITTED".equals(type)) {
                            assertThat(row.getString("sql_text")).isEqualTo(sql);
                            assertThat(row.getObject("success")).isEqualTo(true);
                        } else {
                            assertThat(row.getString("tenant_id")).isEqualTo(ProcessSecurityAuthority.TENANT);
                            assertThat(row.getObject("success")).isEqualTo(false);
                            assertThat(row.getLong("duration_ms")).isNotNegative();
                        }
                    }
                }
                assertThat(types).as("security query events for %s", id)
                        .containsExactly("QUERY_SUBMITTED", "QUERY_CANCELLED");
            }
        });
    }

    private static void assertCancelled(IndependentLocalProcesses cluster, String id) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var status = cluster.status(id);
            assertThat(status.queryId()).isEqualTo(id);
            assertThat(status.state()).isEqualTo("CANCELLED");
        });
    }
}
