package org.pangrova.testing.integration.e2e.speculation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.protocol.http.TaskRuntimeFilterDelivery;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.task.TaskRuntimeFilterWire;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.OperatorStats;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Compares two runs of one SQL plan, holding only the actual build receipt at different scan boundaries. */
@E2ETest
@ResourceLock("pangrova-shuffle-acceptance-aws-properties")
class IndependentLateRuntimeFilterE2ETest extends ContainerTestBase {
    private static final ResultProtocol FLIGHT = ResultProtocol.GRPC_TLS;
    private static final String PROBE = "spec_catalog.default.late_probe";
    private static final String BUILD = "spec_catalog.default.late_build";
    private static final int FILES = 40;
    private static final int ROWS_PER_FILE = 32_768;
    private static final long LAST_KEY = (long) FILES * ROWS_PER_FILE - 7;
    private static final Duration EVENT_DEADLINE = Duration.ofSeconds(25);

    @Test
    void publishedBuildFiltersANativeProbeAlreadyRunningOnTwoWorkers(TestReporter numbers) throws Exception {
        var firstTarget = new AtomicReference<URI>();
        var secondTarget = new AtomicReference<URI>();
        try (var first = new ProcessWorkerHttpRelay(firstTarget::get);
                var second = new ProcessWorkerHttpRelay(secondTarget::get);
                var processes = new IndependentLocalProcesses()) {
            processes.reports().mapWorkerHttpPorts(worker -> {
                boolean initial = worker.workerId().equals("local-worker-1");
                (initial ? firstTarget : secondTarget).set(
                        URI.create("http://" + worker.httpHost() + ":" + worker.httpPort()));
                return (initial ? first : second).port();
            });
            try {
                seed(processes);
                processes.startSecondWorker();
                var relays = List.of(first, second);
                var baseline = execute(processes, relays, false);
                var late = execute(processes, relays, true);
                assertThat(late.rows()).containsExactly("17", "17", Long.toString(LAST_KEY));
                assertThat(late.rows()).isEqualTo(baseline.rows());
                assertThat(baseline.scanRows()).isEqualTo((long) FILES * ROWS_PER_FILE);
                assertThat(late.scanRows()).isPositive().isLessThan(baseline.scanRows());
                assertThat(late.readBytes()).isPositive().isLessThanOrEqualTo(baseline.readBytes());
                assertThat(first.forwardingFailures()).isEmpty();
                assertThat(second.forwardingFailures()).isEmpty();
                assertThat(processes.reports().forwardingFailures()).isEmpty();
                numbers.publishEntry(Map.of("baseline_probe_rows", Long.toString(baseline.scanRows()),
                        "late_probe_rows", Long.toString(late.scanRows()),
                        "baseline_local_read_bytes", Long.toString(baseline.readBytes()),
                        "late_local_read_bytes", Long.toString(late.readBytes())));
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Late-filter process diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    private static void seed(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        processes.command("CREATE TABLE " + PROBE + " (id BIGINT)", FLIGHT);
        processes.command("CREATE TABLE " + BUILD + " (id BIGINT)", FLIGHT);
        processes.command("INSERT INTO " + BUILD + " VALUES (8), (8), (" + (LAST_KEY - 1) / 2 + "), (NULL)", FLIGHT);
        try (var catalog = ProcessInputSnapshot.openCatalog(processes.warehouse(), processes.hostJdbcUrl())) {
            var table = catalog.loadTable(TableIdentifier.of("default", "late_probe"));
            var append = table.newAppend();
            for (int file = 0; file < FILES; file++) {
                var path = Path.of(table.location(), "data", "late-probe-" + file + ".parquet");
                var writer = Parquet.writeData(table.io().newOutputFile(path.toString()))
                        .schema(table.schema()).withSpec(table.spec())
                        .createWriterFunc(GenericParquetWriter::create).build();
                try (writer) {
                    var row = GenericRecord.create(table.schema());
                    long start = (long) file * ROWS_PER_FILE;
                    for (long id = start; id < start + ROWS_PER_FILE; id++) {
                        row.setField("id", id);
                        writer.write(row);
                    }
                }
                append.appendFile(writer.toDataFile());
            }
            append.commit();
        }
    }

    private static ReadEvidence execute(IndependentLocalProcesses processes,
            List<ProcessWorkerHttpRelay> relays, boolean deliverWhileRunning) throws Exception {
        // Only the next query's build may be held. Earlier query replays still receive their real ACKs.
        long observedAfter = System.nanoTime();
        var query = new AtomicReference<String>();
        for (var relay : relays) {
            relay.beforeDelivery(transfer -> identifyBuildQuery(transfer, query));
        }
        try (var held = processes.reports().holdReports(report ->
                report.queryId().equals(query.get()) && hasBuildSummary(report))) {
            var run = ManagedExecutorFactory.newSingleThreadExecutor("late-filter-sql-client");
            try {
                // Keep two independent scan roots. A direct selective build can fuse the probe into
                // the broadcast consumer, which cannot scan until the build edge is published.
                var future = run.submit(() -> processes.query("SELECT p.id FROM " + PROBE
                        + " p JOIN (SELECT id * 2 + 1 AS join_key FROM " + BUILD
                        + ") b ON p.id = b.join_key ORDER BY p.id", FLIGHT));

                await("the actual build submission identifies this query").atMost(EVENT_DEADLINE)
                        .untilAsserted(() -> {
                            if (future.isDone()) future.get();
                            assertThat(query.get()).isNotBlank();
                        });
                await("build receipt is held after Native built the keys").pollInterval(Duration.ofMillis(10))
                        .atMost(EVENT_DEADLINE)
                        .untilAsserted(() -> assertThat(processes.reports().deliveries()).anyMatch(delivery ->
                                query.get().equals(delivery.report().queryId())
                                        && hasBuildSummary(delivery.report()) && delivery.response().get() == null));

                var probes = new ArrayList<TaskAttemptId>();
                await("the exact probe attempts are active on both Workers").pollInterval(Duration.ofMillis(10))
                        .atMost(EVENT_DEADLINE)
                        .untilAsserted(() -> {
                            probes.clear();
                            for (var transfer : transfers(relays, observedAfter)) {
                                if (!transfer.isTaskSubmission()) continue;
                                var request = transfer.submittedTask().taskRequest();
                                if (request.getAttempt().getQueryOrJobId().equals(query.get())
                                        && request.getSplitsList().stream().anyMatch(split -> PROBE.equals(split.getSourceId()))) {
                                    probes.add(request.getAttempt());
                                }
                            }
                            assertThat(probes).doesNotHaveDuplicates();
                            assertThat(probes.stream().map(TaskAttemptId::getWorkerExecutionIdentity).distinct()).hasSize(2);
                            assertThat(probes).allMatch(attempt -> processes.reports().deliveries().stream()
                                    .anyMatch(delivery -> attempt.equals(delivery.report().reportedAttempt())
                                            && "RUNNING".equals(delivery.report().state())
                                            && delivered(delivery, 200)));
                        });

                if (!deliverWhileRunning) {
                    await("unfiltered scans finish before the build is released")
                            .atMost(EVENT_DEADLINE).untilAsserted(() ->
                                    assertThat(probes).allMatch(attempt -> processes.reports().deliveries().stream()
                                            .anyMatch(delivery -> finished(delivery, attempt))));
                }
                held.close();
                if (deliverWhileRunning) {
                    await("a live Worker natively acknowledges the exact late filter").atMost(EVENT_DEADLINE)
                            .untilAsserted(() -> {
                                List<TaskRuntimeFilterDelivery> accepted = new ArrayList<>();
                                for (var transfer : filterTransfers(relays, observedAfter)) {
                                    if (transfer.response().get() != null
                                            && transfer.response().get().statusCode() == 204) {
                                        accepted.add(TaskRuntimeFilterWire.decode(transfer.body()));
                                    }
                                }
                                assertThat(accepted).isNotEmpty().allSatisfy(delivery -> {
                                    assertThat(probes).contains(delivery.getAttempt());
                                    assertThat(delivery.getUpdate().getSoleTableScan()).isTrue();
                                    assertThat(delivery.getUpdate().getFiltersList()).singleElement().satisfies(filter ->
                                            assertThat(filter.getInList().getValuesList()).extracting(value ->
                                                    value.getInt64Value()).containsExactly(17L, LAST_KEY));
                                });
                                assertThat(accepted).extracting(TaskRuntimeFilterDelivery::getAttempt)
                                        .doesNotHaveDuplicates();
                            });
                }
                var result = future.get(90, TimeUnit.SECONDS);
                assertThat(result.queryId()).isEqualTo(query.get());
                if (!deliverWhileRunning) assertThat(filterTransfers(relays, observedAfter)).isEmpty();
                await("the actual query releases task, shuffle and tenant ownership").atMost(EVENT_DEADLINE)
                        .untilAsserted(() -> {
                            assertThat(processes.status(query.get()).drained()).isTrue();
                            assertThat(processes.activeTenantReservations()).isZero();
                            assertThat(executionReceipts(processes, probes)).hasSize(probes.size());
                        });
                var receipts = executionReceipts(processes, probes);
                long scan = receipts.stream().flatMap(receipt -> receipt.getNativeTaskStats().getOperatorStatsList().stream())
                        .filter(operator -> operator.getOperatorType().equals("TableScan"))
                        .mapToLong(OperatorStats::getOutputRows).sum();
                long bytes = receipts.stream().mapToLong(receipt -> receipt.getUsage().getObjectStoreReadBytes()).sum();
                var rows = result.output().lines().filter(line -> line.startsWith("|")).skip(1)
                        .map(line -> line.split("\\|", -1)[1].trim()).toList();
                return new ReadEvidence(rows, scan, bytes);
            } finally {
                held.close();
                run.shutdownNow();
                assertThat(run.awaitTermination(35, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static void identifyBuildQuery(ProcessWorkerHttpRelay.Transfer transfer, AtomicReference<String> query) {
        if (!transfer.isTaskSubmission() || query.get() != null) return;
        try {
            var request = transfer.submittedTask().taskRequest();
            if (request.getBuildKeyOrdinalsCount() > 0
                    && request.getSplitsList().stream().anyMatch(split -> BUILD.equals(split.getSourceId()))) {
                query.compareAndSet(null, request.getAttempt().getQueryOrJobId());
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private static List<ProcessWorkerHttpRelay.Transfer> transfers(List<ProcessWorkerHttpRelay> relays, long after) {
        return relays.stream().flatMap(relay -> relay.transfers().stream())
                .filter(value -> value.receivedNanos() >= after).toList();
    }

    private static List<ProcessWorkerHttpRelay.Transfer> filterTransfers(List<ProcessWorkerHttpRelay> relays, long after) {
        return transfers(relays, after).stream()
                .filter(value -> value.method().equals("POST") && value.path().getPath().endsWith("/dynamic-filter")).toList();
    }

    private static boolean hasBuildSummary(TaskStatusReport report) {
        var evidence = report.terminalEvidence();
        return evidence != null && evidence.report().getNativeTaskStats().getBuildKeySummariesCount() > 0;
    }

    private static boolean delivered(ProcessStatusReportRelay.Delivery delivery, int status) {
        var response = delivery.response().get();
        return response != null && response.statusCode() == status;
    }

    private static boolean finished(ProcessStatusReportRelay.Delivery delivery, TaskAttemptId attempt) {
        var evidence = delivery.report().terminalEvidence();
        return evidence != null && delivered(delivery, 200) && attempt.equals(evidence.report().getAttempt())
                && evidence.report().getState() == TaskState.TASK_STATE_FINISHED;
    }

    private static List<TaskReport> executionReceipts(IndependentLocalProcesses processes, List<TaskAttemptId> attempts) {
        var exact = new LinkedHashMap<TaskAttemptId, TaskReport>();
        for (var delivery : processes.reports().deliveries()) {
            var evidence = delivery.report().terminalEvidence();
            if (evidence == null || !delivered(delivery, 200)) continue;
            var report = evidence.report();
            if (attempts.contains(report.getAttempt()) && report.getExecutionDrainState()
                    == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
                exact.put(report.getAttempt(), report);
            }
        }
        return List.copyOf(exact.values());
    }

    private record ReadEvidence(List<String> rows, long scanRows, long readBytes) {}
}
