package org.pangrova.testing.integration.e2e.lifecycle;

import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class AsyncSubmitSaturationE2ETest extends SqlE2ETestBase {
    private static final int SUBMIT_THREADS = 4;
    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Test
    void fourAcknowledgedReadsWithOutstandingTerminalReportsDoNotBlockTheNextSubmission() throws Exception {
        var submitter = Executors.newSingleThreadExecutor();
        var queryIds = new HashSet<String>();
        try (var reports = new HeldTerminalReports(); var client = openClient()) {
            for (long value = 101; value <= 104; value++) {
                queryIds.add(readConstant(client, value));
            }
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(reports.queryIds()).containsExactlyInAnyOrderElementsOf(queryIds);
                assertQueriesRunning(queryIds);
            });
            String fifthId = submitter.submit(() -> {
                try (var isolated = openClient()) {
                    return readConstant(isolated, 105);
                }
            }).get(10, TimeUnit.SECONDS);
            assertQueriesRunning(queryIds);
            assertThat(queryIds.add(fifthId)).isTrue();
        } finally {
            submitter.shutdownNow();
            assertThat(submitter.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        assertPhysicalCleanup(queryIds);
    }

    @Test
    void fourWritesAwaitingFencedCommitDoNotBlockAnIndependentRead() throws Exception {
        String prefix = CATALOG + "." + SCHEMA + ".pending_write_" + Long.toHexString(System.nanoTime()) + "_";
        for (int index = 0; index < SUBMIT_THREADS; index++) {
            sqlRunner.executeDdl("CREATE TABLE " + prefix + index + " (id BIGINT)");
        }

        var submitters = Executors.newFixedThreadPool(SUBMIT_THREADS + 1);
        var writes = new ArrayList<Future<String>>();
        var queryIds = new HashSet<String>();
        try {
            try (var reports = new HeldTerminalReports()) {
                for (int index = 0; index < SUBMIT_THREADS; index++) {
                    String table = prefix + index;
                    writes.add(submitters.submit(() -> writeAndAwaitCommit(table)));
                }
                await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                    var held = reports.queryIds();
                    assertThat(held).hasSize(SUBMIT_THREADS);
                    assertQueriesRunning(held);
                });
                String read = submitters.submit(() -> {
                    try (var client = openClient()) {
                        return readConstant(client, 201);
                    }
                }).get(10, TimeUnit.SECONDS);
                assertThat(queryIds.add(read)).isTrue();
                assertQueriesRunning(reports.queryIds());
                assertThat(writes).allSatisfy(write -> assertThat(write).isNotDone());
            }
            for (var write : writes) {
                assertThat(queryIds.add(write.get(15, TimeUnit.SECONDS))).isTrue();
            }
            assertPhysicalCleanup(queryIds);
            for (int index = 0; index < SUBMIT_THREADS; index++) {
                assertThat(sqlRunner.execute("SELECT id FROM " + prefix + index))
                        .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(42L));
            }
        } finally {
            submitters.shutdownNow();
            assertThat(submitters.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static String writeAndAwaitCommit(String table) throws Exception {
        try (var client = openClient()) {
            var execution = client.beginQuery("INSERT INTO " + table + " VALUES (42)", Map.of());
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
            var terminal = execution.awaitStatus(QueryWaitTarget.TERMINAL, deadline);
            assertThat(terminal.state()).isEqualTo("FINISHED");
            assertThat(terminal.resultDisposition()).isEqualTo(
                    org.pangrova.common.enums.ResultDisposition.NONE);
            assertThat(terminal.resultEndpoint()).isNull();
            return execution.submission().queryId();
        }
    }

    private static void assertQueriesRunning(Set<String> queryIds) {
        assertThat(queryIds).allSatisfy(id -> assertThat(queryManager.getQueryState(id)).contains(QueryState.RUNNING));
    }

    private static void assertPhysicalCleanup(Set<String> queryIds) {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(queryIds).allSatisfy(id -> assertThat(
                        queryManager.getStateMachines().get(id).physicalCleanupApplied()).isTrue()));
    }

    private static final class HeldTerminalReports implements AutoCloseable {
        private final Map<String, List<TaskStatusReport>> held = new LinkedHashMap<>();
        private boolean closed;

        private HeldTerminalReports() {
            configureVirtualRoutingStatusDelivery(this::report);
        }

        private void report(TaskStatusReport report) {
            synchronized (held) {
                if (!closed && "FINISHED".equals(report.state())) {
                    if (!held.containsKey(report.queryId()) && held.size() < SUBMIT_THREADS) {
                        held.put(report.queryId(), new ArrayList<>());
                    }
                    var reports = held.get(report.queryId());
                    if (reports != null) {
                        reports.add(report);
                        return;
                    }
                }
            }
            reportToSharedCoordinator(report);
        }

        private Set<String> queryIds() {
            synchronized (held) {
                return Set.copyOf(held.keySet());
            }
        }

        @Override
        public void close() {
            synchronized (held) {
                closed = true;
                restoreSharedStatusDelivery();
                held.values().stream().flatMap(List::stream)
                        .forEach(AsyncSubmitSaturationE2ETest::reportToSharedCoordinator);
                held.clear();
            }
        }
    }

    private static PangrovaRestClient openClient() {
        return PangrovaRestClient.builder()
                .coordinatorHost("localhost")
                .coordinatorPort(coordinatorHttpPort())
                .bearerToken(E2E_USER_BEARER_TOKEN)
                .retryPolicy(RetryPolicy.NONE)
                .build();
    }

    private static String readConstant(PangrovaRestClient client, long value) throws Exception {
        var execution = client.beginQuery("SELECT CAST(" + value + " AS BIGINT) AS answer", Map.of());
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        var ready = execution.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
        while (ready.resultDisposition() == ResultDisposition.PENDING) {
            assertThat(ready.state()).isNotIn("FAILED", "CANCELLED", "FINISHED");
            ready = execution.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
        }
        assertThat(ready.resultDisposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(ready.resultEndpoint()).isNotNull();
        try (var result = client.openResult(ready.resultEndpoint(), deadline)) {
            assertThat(result.next()).isTrue();
            assertThat(((Number) result.value(0)).longValue()).isEqualTo(value);
            assertThat(result.next()).isFalse();
        }
        return execution.submission().queryId();
    }
}
