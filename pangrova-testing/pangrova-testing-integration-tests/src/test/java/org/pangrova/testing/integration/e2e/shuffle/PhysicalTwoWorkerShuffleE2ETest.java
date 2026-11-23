package org.pangrova.testing.integration.e2e.shuffle;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.TestReporter;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.v2.BuildKeyRange;
import org.pangrova.protocol.v2.BuildKeySummary;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.OperatorStats;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.WorkerExecutionIdentity;
import org.pangrova.scheduler.api.enums.HealthState;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

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
                      spill-bytes: 268435456
                      spill-directory: "%s"
                  cdc:
                    enabled: false
                """.formatted(SECOND_WORKER_ID, routableHost, routableHost,
                coordinatorHttpPort(),
                E2E_INTERNAL_AUTH_SECRET, workerSpillDirectory.toAbsolutePath()));
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        // Distribution verification supplies the ordered Worker-only runtime. Reactor tests
        // run before packaging and use Surefire's resolved classpath for the child JVM.
        String childClasspath = System.getProperty("pangrova.e2e.worker.classpath",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        if (childClasspath.isBlank()) {
            throw new IllegalArgumentException("The physical Worker classpath is empty");
        }
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
                "org.pangrova.server.worker.bootstrap.PangrovaWorkerMain",
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
        var expected = Set.of(workerManager.getWorker(WORKER_ID).workerExecutionIdentity().toProto(),
                workerManager.getWorker(SECOND_WORKER_ID).workerExecutionIdentity().toProto());
        var drainedWorkers = new ConcurrentHashMap<String, Set<WorkerExecutionIdentity>>();
        var capture = new AtomicReference<>(drainedWorkers);
        taskTracker.addExecutionDrainListener(receipt -> {
            var targets = capture.get();
            if (targets != null
                    && receipt.getExecutionDrainState() == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
                var attempt = receipt.getAttempt();
                targets.computeIfAbsent(attempt.getQueryOrJobId(), ignored -> ConcurrentHashMap.newKeySet())
                        .add(attempt.getWorkerExecutionIdentity());
            }
        });
        try {
            requireDistributedResultAndDrain(drainedWorkers, expected);
        } finally {
            capture.set(null);
        }
    }

    @Test
    void scalarAggregatesReduceOnEachScanWorkerBeforeGather() {
        var result = executeWithScanAggregates("SELECT COUNT(*) AS total, "
                + "COUNT(CASE WHEN score >= 40 THEN score END) AS matched, "
                + "MIN(score) FILTER (WHERE score >= 80) AS no_match, MAX(score) AS highest FROM " + orders);
        assertThat(result.rows()).singleElement().satisfies(row -> {
            assertThat(row.getLong("total")).isEqualTo(80L);
            assertThat(row.getLong("matched")).isEqualTo(40L);
            assertThat(row.getRaw("no_match")).isNull();
            assertThat(row.getLong("highest")).isEqualTo(79L);
        });
        assertThat(result.scannedRows()).isEqualTo(80);
        assertThat(result.localAggregates()).allSatisfy(stat -> assertThat(stat.getOutputRows()).isOne());
    }

    @Test
    void observedLowCardinalityGroupsReduceBeforeHashExchange(TestReporter metrics) {
        StringBuilder insert = new StringBuilder("INSERT INTO " + regions + " VALUES (1000, NULL)");
        for (int row = 0; row < 80; row++) {
            insert.append(", (").append(1001 + row).append(", 'region-").append(row % 4).append("')");
        }
        sqlRunner.execute(insert.toString());
        var observed = sqlRunner.execute("SELECT COUNT(*) AS rows, COUNT(DISTINCT region) AS ndv, "
                + "COUNT(region) AS nonnull FROM " + regions).get(0);
        long rows = observed.getLong("rows");
        var groups = new TableStatistics.ColumnStatistics(rows - observed.getLong("nonnull"),
                observed.getLong("ndv"), null, null, -1);
        var analyzed = sqlRunner.execute("ANALYZE TABLE " + regions).get(0);
        assertThat(analyzed.getLong("row_count")).isEqualTo(rows);
        metadataBootstrap.catalogRegistry().tableStatistics().record(CATALOG, SCHEMA,
                regions.substring(regions.lastIndexOf('.') + 1), new TableStatistics(rows, -1, Map.of("region", groups)),
                "ANALYZE", analyzed.getString("version_token"));

        var result = executeWithScanAggregates(
                "SELECT region, COUNT(*) AS total, MAX(id) AS highest FROM " + regions + " GROUP BY region");
        assertThat(result.rows()).hasSize(5).allSatisfy(row -> {
            String region = row.getString("region");
            if (region == null) {
                assertThat(row.getLong("total")).isEqualTo(1L);
                assertThat(row.getLong("highest")).isEqualTo(1000L);
            } else {
                assertThat(row.getLong("total")).isEqualTo(40L);
                assertThat(row.getLong("highest")).isEqualTo(1077L + Integer.parseInt(region.substring(7)));
            }
        });
        long shuffledRows = result.localAggregates().stream().mapToLong(OperatorStats::getOutputRows).sum();
        assertThat(result.scannedRows()).isEqualTo(161);
        assertThat(shuffledRows).isBetween(5L, 5L * result.localAggregates().size()).isLessThan(result.scannedRows());
        metrics.publishEntry(Map.of("scan_rows", Long.toString(result.scannedRows()),
                "partial_shuffle_rows", Long.toString(shuffledRows)));
    }

    @Test
    void observedLowCardinalityCompositeGroupsReduceBeforeHashExchange(TestReporter metrics) {
        String table = orders + "_joint_groups";
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, region VARCHAR, segment VARCHAR)");
        var regionLabels = List.of("'east'", "'east'", "'west'", "NULL");
        var segmentLabels = List.of("'retail'", "NULL", "'retail'", "'wholesale'");
        for (int first = 0; first < 192; first += 48) {
            String values = java.util.stream.IntStream.range(first, first + 48)
                    .mapToObj(id -> "(" + id + ", " + regionLabels.get(id % 4) + ", " + segmentLabels.get(id % 4) + ")")
                    .collect(java.util.stream.Collectors.joining(", "));
            sqlRunner.execute("INSERT INTO " + table + " VALUES " + values);
        }
        String sql = "SELECT region, segment, COUNT(*) AS total, MIN(id) AS lowest, MAX(id) AS highest FROM "
                + table + " GROUP BY region, segment";
        var raw = executeWithScanAggregates(sql, false);
        var observed = sqlRunner.execute("SELECT COUNT(*) AS rows, COUNT(DISTINCT region) AS r_ndv, "
                + "COUNT(region) AS r_nonnull, COUNT(DISTINCT segment) AS s_ndv, COUNT(segment) AS s_nonnull FROM "
                + table).get(0);
        long rows = observed.getLong("rows");
        Map<String, TableStatistics.ColumnStatistics> columns = Map.of(
                "region", new TableStatistics.ColumnStatistics(rows - observed.getLong("r_nonnull"),
                        observed.getLong("r_ndv"), null, null, -1),
                "segment", new TableStatistics.ColumnStatistics(rows - observed.getLong("s_nonnull"),
                        observed.getLong("s_ndv"), null, null, -1));
        var analyzed = sqlRunner.execute("ANALYZE TABLE " + table).get(0);
        assertThat(analyzed.getLong("row_count")).isEqualTo(rows);
        metadataBootstrap.catalogRegistry().tableStatistics().record(CATALOG, SCHEMA,
                table.substring(table.lastIndexOf('.') + 1), new TableStatistics(rows, -1, columns),
                "ANALYZE", analyzed.getString("version_token"));

        var planned = executeWithScanAggregates(sql);
        assertThat(planned.rows()).hasSize(4).allSatisfy(row -> {
            int group = Math.toIntExact(row.getLong("lowest"));
            assertThat(group).isBetween(0, 3);
            assertThat(row.getLong("total")).isEqualTo(48L);
            assertThat(row.getLong("highest")).isEqualTo(group + 188L);
            assertThat(row.getString("region")).isEqualTo(group == 3 ? null : regionLabels.get(group).replace("'", ""));
            assertThat(row.getString("segment")).isEqualTo(group == 1 ? null : segmentLabels.get(group).replace("'", ""));
        });
        assertThat(planned.rows()).containsExactlyInAnyOrderElementsOf(raw.rows());
        assertThat(raw.scannedRows()).isEqualTo(192);
        assertThat(planned.scannedRows()).isEqualTo(raw.scannedRows());
        assertThat(raw.exchangeRows()).isEqualTo(192);
        long aggregateRows = planned.localAggregates().stream().mapToLong(OperatorStats::getOutputRows).sum();
        assertThat(planned.exchangeRows()).isEqualTo(aggregateRows).isBetween(4L, 16L);
        metrics.publishEntry(Map.of("scan_rows", Long.toString(planned.scannedRows()),
                "raw_shuffle_rows", Long.toString(raw.exchangeRows()),
                "planned_shuffle_rows", Long.toString(planned.exchangeRows())));
        org.slf4j.LoggerFactory.getLogger(PhysicalTwoWorkerShuffleE2ETest.class).info(
                "event=composite_group_shuffle_rows raw={} stat_bound={} scan={}",
                raw.exchangeRows(), planned.exchangeRows(), planned.scannedRows());
    }

    @Test
    void completedBuildPrunesPinnedIcebergPartitionsBeforeWorkerDispatch(TestReporter metrics) {
        String prefix = orders + "_filter";
        String probe = prefix + "_probe";
        String build = prefix + "_build";
        sqlRunner.executeDdl("CREATE TABLE " + probe + " (id BIGINT, payload BIGINT) PARTITIONED BY (id)");
        sqlRunner.executeDdl("CREATE TABLE " + build + " (id BIGINT)");
        String values = java.util.stream.IntStream.range(0, 40)
                .mapToObj(row -> "(" + row + ", " + (row + 100) + ")")
                .collect(java.util.stream.Collectors.joining(", "));
        sqlRunner.execute("INSERT INTO " + probe + " VALUES " + values);
        sqlRunner.execute("INSERT INTO " + build + " VALUES (13), (13), (14), (NULL)");

        String projected = "SELECT p.id, p.payload FROM " + probe + " p";
        String join = " JOIN " + build + " b ON ";
        String condition = "p.id = b.id ORDER BY p.id";
        var unpruned = executeWithDrainedReports(projected + join + "ABS(p.id) = b.id ORDER BY p.id");
        var pruned = executeWithDrainedReports(projected + join + condition);
        var outer = executeWithDrainedReports(projected + " LEFT" + join + condition);
        assertThat(pruned.rows()).extracting(row -> row.getLong("id")).containsExactly(13L, 13L, 14L);
        assertThat(pruned.rows()).extracting(row -> row.getLong("payload")).containsExactly(113L, 113L, 114L);
        assertThat(unpruned.rows()).usingRecursiveComparison().isEqualTo(pruned.rows());
        assertThat(outer.rows()).hasSize(41);
        assertThat(outer.buildSummaries()).isEmpty();
        assertThat(outer.sourceSplits(probe)).isEqualTo(40L);
        assertThat(unpruned.buildSummaries()).isEmpty();
        assertThat(pruned.buildSummaries()).singleElement().satisfies(summary ->
                assertThat(summary.getRangesList()).containsExactly(buildKeyRange(13, 14)));
        assertThat(unpruned.sourceSplits(probe)).isEqualTo(40L);
        assertThat(pruned.sourceSplits(probe)).isEqualTo(2L);
        org.slf4j.LoggerFactory.getLogger(PhysicalTwoWorkerShuffleE2ETest.class).info(
                "event=runtime_filter_probe_comparison fullSplits={} filteredSplits={} fullBytes={} filteredBytes={}",
                unpruned.sourceSplits(probe), pruned.sourceSplits(probe),
                unpruned.sourceReadBytes(probe), pruned.sourceReadBytes(probe));
        assertThat(unpruned.sourceReadBytes(probe)).isGreaterThan(pruned.sourceReadBytes(probe));
        assertThat(pruned.sourceReadBytes(probe)).isPositive();
        metrics.publishEntry(Map.of("full_probe_splits", Long.toString(unpruned.sourceSplits(probe)),
                "filtered_probe_splits", Long.toString(pruned.sourceSplits(probe)),
                "full_probe_read_bytes", Long.toString(unpruned.sourceReadBytes(probe)),
                "filtered_probe_read_bytes", Long.toString(pruned.sourceReadBytes(probe))));

        String allNull = prefix + "_null_build";
        sqlRunner.executeDdl("CREATE TABLE " + allNull + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + allNull + " VALUES (NULL)");
        var empty = executeWithDrainedReports(projected + " JOIN " + allNull + " b ON " + condition);
        assertThat(empty.rows()).isEmpty();
        assertThat(empty.sourceSplits(probe)).isZero();
        assertThat(empty.buildSummaries()).singleElement().satisfies(summary ->
                assertThat(summary.getRangesList()).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void separatedBuildRangesPruneOnlyMatchingProbePartitions(boolean oneBuildTask, TestReporter metrics) {
        String suffix = oneBuildTask ? "_single_sparse" : "_multi_sparse";
        String probe = orders + suffix + "_probe";
        String build = orders + suffix + "_build";
        sqlRunner.executeDdl("CREATE TABLE " + probe + " (id BIGINT, payload BIGINT) PARTITIONED BY (id)");
        sqlRunner.executeDdl("CREATE TABLE " + build + " (id BIGINT)");
        String values = java.util.stream.IntStream.range(0, 40)
                .mapToObj(row -> "(" + row + ", " + (row + 100) + ")")
                .collect(java.util.stream.Collectors.joining(", "));
        sqlRunner.execute("INSERT INTO " + probe + " VALUES " + values);
        String valuesBeforeGap = "(2), (2), (3)";
        String valuesAfterGap = "(35), (36), (NULL)";
        if (oneBuildTask) {
            sqlRunner.execute("INSERT INTO " + build + " VALUES " + valuesBeforeGap + ", " + valuesAfterGap);
        } else {
            sqlRunner.execute("INSERT INTO " + build + " VALUES " + valuesBeforeGap);
            sqlRunner.execute("INSERT INTO " + build + " VALUES " + valuesAfterGap);
        }

        String join = "SELECT p.id, p.payload FROM " + probe + " p JOIN " + build + " b ON ";
        // Measure the pruned read before the full scan can warm its file ranges.
        var pruned = executeWithDrainedReports(join + "p.id = b.id ORDER BY p.id");
        var unpruned = executeWithDrainedReports(join + "ABS(p.id) = b.id ORDER BY p.id");
        assertThat(pruned.rows()).extracting(row -> row.getLong("id")).containsExactly(2L, 2L, 3L, 35L, 36L);
        assertThat(pruned.rows()).extracting(row -> row.getLong("payload"))
                .containsExactly(102L, 102L, 103L, 135L, 136L);
        assertThat(unpruned.rows()).usingRecursiveComparison().isEqualTo(pruned.rows());
        assertThat(unpruned.buildSummaries()).isEmpty();
        var lower = buildKeyRange(2, 3);
        var upper = buildKeyRange(35, 36);
        if (oneBuildTask) {
            assertThat(pruned.buildSummaries()).singleElement().satisfies(summary ->
                    assertThat(summary.getRangesList()).containsExactly(lower, upper));
        } else {
            assertThat(pruned.buildSummaries()).extracting(BuildKeySummary::getRangesList)
                    .containsExactlyInAnyOrder(List.of(lower), List.of(upper));
        }
        long fullSplits = unpruned.sourceSplits(probe);
        long filteredSplits = pruned.sourceSplits(probe);
        long fullBytes = unpruned.sourceReadBytes(probe);
        long filteredBytes = pruned.sourceReadBytes(probe);
        org.slf4j.LoggerFactory.getLogger(PhysicalTwoWorkerShuffleE2ETest.class).info(
                "event=sparse_build_probe oneBuildTask={} fullSplits={} filteredSplits={} fullBytes={} filteredBytes={}",
                oneBuildTask, fullSplits, filteredSplits, fullBytes, filteredBytes);
        metrics.publishEntry(Map.of("full_probe_splits", Long.toString(fullSplits),
                "filtered_probe_splits", Long.toString(filteredSplits), "full_probe_read_bytes", Long.toString(fullBytes),
                "filtered_probe_read_bytes", Long.toString(filteredBytes)));
        assertThat(fullSplits).isEqualTo(40);
        assertThat(filteredSplits).isEqualTo(4);
        assertThat(filteredBytes).isPositive().isLessThan(fullBytes);
    }

    private static BuildKeyRange buildKeyRange(long minimum, long maximum) {
        return BuildKeyRange.newBuilder().setMinimum(minimum).setMaximum(maximum).build();
    }

    private static DrainedQuery executeWithDrainedReports(String sql) {
        var capture = new AtomicReference<>(new ConcurrentHashMap<String, TaskReport>());
        taskTracker.addExecutionDrainListener(report -> {
            var reports = capture.get();
            if (reports != null && report.hasNativeTaskStats()) {
                reports.put(report.getAttempt().getTaskId(), report);
            }
        });
        var before = new HashSet<>(queryManager.getStateMachines().keySet());
        try {
            var rows = sqlRunner.execute(sql);
            var issued = new HashSet<>(queryManager.getStateMachines().keySet());
            issued.removeAll(before);
            assertThat(issued).hasSize(1);
            String queryId = issued.iterator().next();
            Awaitility.await("Native execution drained").atMost(Duration.ofSeconds(30))
                    .ignoreExceptionsInstanceOf(IllegalStateException.class).untilAsserted(() -> {
                        taskTracker.requireQueryExecutionDrained(queryId);
                        assertThat(queryManager.getStateMachines().get(queryId).physicalCleanupApplied()).isTrue();
                    });
            return new DrainedQuery(rows, capture.get().values().stream()
                    .filter(report -> report.getAttempt().getQueryOrJobId().equals(queryId)).toList());
        } finally {
            capture.set(null);
        }
    }

    private record DrainedQuery(List<TypedRow> rows, List<TaskReport> reports) {
        List<BuildKeySummary> buildSummaries() {
            return reports.stream().map(TaskReport::getNativeTaskStats)
                    .filter(org.pangrova.protocol.v2.NativeTaskStats::hasBuildKeys)
                    .map(org.pangrova.protocol.v2.NativeTaskStats::getBuildKeys).toList();
        }

        long sourceSplits(String source) {
            return reports.stream().flatMap(report -> report.getCoverage().getSourceSnapshotsList().stream())
                    .filter(coverage -> coverage.getSourceId().equals(source))
                    .mapToLong(org.pangrova.protocol.v2.SourceSnapshotReport::getCompletedWorkUnits).sum();
        }

        long sourceReadBytes(String source) {
            return reports.stream().filter(report -> report.getCoverage().getSourceSnapshotsList().stream()
                            .anyMatch(coverage -> coverage.getSourceId().equals(source)))
                    .mapToLong(report -> report.getUsage().getObjectStoreReadBytes()).sum();
        }
    }

    private static ScanAggregates executeWithScanAggregates(String sql) {
        return executeWithScanAggregates(sql, true);
    }

    private static ScanAggregates executeWithScanAggregates(String sql, boolean reduce) {
        var capture = new AtomicReference<>(new ConcurrentHashMap<String, TaskReport>());
        taskTracker.addExecutionDrainListener(report -> {
            var reports = capture.get();
            if (reports != null && report.hasNativeTaskStats()) {
                reports.put(report.getAttempt().getTaskId(), report);
            }
        });
        var before = new HashSet<>(queryManager.getStateMachines().keySet());
        try {
            var rows = sqlRunner.execute(sql);
            var issued = new HashSet<>(queryManager.getStateMachines().keySet());
            issued.removeAll(before);
            assertThat(issued).hasSize(1);
            String queryId = issued.iterator().next();
            var aggregateResult = new AtomicReference<ScanAggregates>();
            Awaitility.await("Native scan and aggregation receipts").atMost(Duration.ofSeconds(30))
                    .ignoreExceptionsInstanceOf(IllegalStateException.class)
                    .untilAsserted(() -> {
                        taskTracker.requireQueryExecutionDrained(queryId);
                        var scanWorkers = capture.get().values().stream()
                                .filter(report -> report.getAttempt().getQueryOrJobId().equals(queryId))
                                .filter(report -> report.getNativeTaskStats().getOperatorStatsList().stream()
                                        .anyMatch(stat -> stat.getOperatorType().equals("TableScan")))
                                .toList();
                        assertThat(scanWorkers).extracting(report -> report.getAttempt().getWorkerExecutionIdentity())
                                .contains(workerManager.getWorker(WORKER_ID).workerExecutionIdentity().toProto(),
                                        workerManager.getWorker(SECOND_WORKER_ID).workerExecutionIdentity().toProto());
                        var scanStats = scanWorkers.stream().flatMap(report ->
                                report.getNativeTaskStats().getOperatorStatsList().stream()).toList();
                        long scannedRows = scanStats.stream().filter(stat -> stat.getOperatorType().equals("TableScan"))
                                .mapToLong(OperatorStats::getOutputRows).sum();
                        var localReductions = scanStats.stream()
                                .filter(stat -> stat.getOperatorType().equals("Aggregation"))
                                .toList();
                        assertThat(localReductions).hasSize(reduce ? scanWorkers.size() : 0);
                        long exchangeRows = scanStats.stream().filter(stat -> stat.getOperatorType().equals(
                                "CallbackSink")).mapToLong(OperatorStats::getInputRows).sum();
                        aggregateResult.set(new ScanAggregates(rows, scannedRows, localReductions, exchangeRows));
                    });
            return aggregateResult.get();
        } finally {
            capture.set(null);
        }
    }

    private record ScanAggregates(
            List<TypedRow> rows, long scannedRows, List<OperatorStats> localAggregates, long exchangeRows) {}

    private void requireDistributedResultAndDrain(Map<String, Set<WorkerExecutionIdentity>> drainedWorkers,
            Set<WorkerExecutionIdentity> expected) {
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
        Awaitility.await("both physical Workers release every issued task")
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(100))
                .ignoreExceptionsInstanceOf(IllegalStateException.class)
                .untilAsserted(() -> {
                    taskTracker.requireQueryExecutionDrained(queryId);
                    assertThat(drainedWorkers.getOrDefault(queryId, Set.of())).containsAll(expected);
                    assertThat(queryManager.getStateMachines().get(queryId).physicalCleanupApplied()).isTrue();
                    var profile = queryProfileService().getProfile(queryId, E2E_USER_PRINCIPAL);
                    assertThat(profile).isPresent();
                    var graph = profile.orElseThrow().executionGraph();
                    assertThat(graph).isNotNull();
                    assertThat(graph.exchanges()).isNotEmpty().allSatisfy(edge -> {
                        assertThat(graph.stages()).extracting(stage -> stage.stageId())
                                .contains(edge.producerStageId(), edge.consumerStageId());
                        assertThat(edge.plannedPartitions()).isPositive();
                    });
                    assertThat(graph.transitions().stream().filter(event -> "FINISHED".equals(event.state())))
                            .extracting(event -> event.stageId())
                            .containsExactlyInAnyOrderElementsOf(graph.stages().stream()
                                    .map(stage -> stage.stageId()).toList());
                    assertThat(graph.transitions()).allSatisfy(event -> {
                        assertThat(event.elapsedNanos()).isNotNegative();
                        assertThat(event.generation()).isNotNegative();
                    });
                    assertThat(graph.downstreamPreparations()).isNotEmpty().allSatisfy(preparation -> {
                        assertThat(preparation.startedNanos()).isGreaterThanOrEqualTo(preparation.submittedNanos());
                        assertThat(preparation.finishedNanos()).isGreaterThanOrEqualTo(preparation.startedNanos());
                    });
                    assertThat(graph.downstreamPreparations()).anySatisfy(preparation ->
                            assertThat(preparation.outcome()).isEqualTo(
                                    org.pangrova.query.profiling.OperatorStatsSource.PreparationOutcome.COMPLETED));
                    assertDispatchWork(graph);
                    var attempts = profile.orElseThrow().attempts();
                    assertThat(graph.selectedAttempts()).isNotEmpty().allSatisfy(selection ->
                            assertThat(attempts).anySatisfy(attempt -> {
                                assertThat(attempt.stageId()).isEqualTo(selection.stageId());
                                assertThat(attempt.partitionId()).isEqualTo(selection.partitionId());
                                assertThat(attempt.planEpoch()).isEqualTo(selection.planEpoch());
                                assertThat(attempt.attemptId()).isEqualTo(selection.attemptId());
                                assertThat(attempt.taskId()).isEqualTo(selection.taskId());
                            }));
                    assertThat(attempts.stream().mapToInt(attempt -> attempt.stageId()).distinct().count())
                            .isGreaterThanOrEqualTo(2);
                    assertThat(attempts).extracting(attempt -> attempt.workerId())
                            .contains(WORKER_ID, SECOND_WORKER_ID);
                    var timings = new java.util.TreeMap<String, String>();
                    for (var interval : graph.stageDispatchWork()) {
                        String key = "stage." + interval.stageId() + "." + interval.phase();
                        timings.put(key + ".nanos", Long.toString(interval.finishedNanos() - interval.startedNanos()));
                        if (interval.itemCount() != null) {
                            timings.put(key + ".items", Integer.toString(interval.itemCount()));
                        }
                    }
                    System.out.println("[E2E-DIAG] Observed stage dispatch work: " + timings);
                });
    }

    private static void assertDispatchWork(org.pangrova.query.profiling.OperatorStatsSource.ExecutionGraph graph) {
        var work = graph.stageDispatchWork();
        assertThat(work).isNotEmpty().allSatisfy(interval -> {
            assertThat(interval.startedNanos()).isNotNegative();
            assertThat(interval.finishedNanos()).isGreaterThanOrEqualTo(interval.startedNanos());
            assertThat(interval.successful()).isTrue();
        });
        for (var stage : graph.stages()) {
            var intervals = work.stream().filter(interval -> interval.stageId() == stage.stageId()).toList();
            assertThat(intervals).extracting(interval -> interval.phase().name()).containsExactly(
                    "SERIALIZE_PLAN", "ENUMERATE_SPLITS", "VALIDATE_AND_BIND_PLAN", "SUBMIT_TASKS",
                    "OBSERVE_COHORT_ACK");
            for (int index = 1; index < intervals.size(); index++) {
                assertThat(intervals.get(index).startedNanos()).isGreaterThanOrEqualTo(
                        intervals.get(index - 1).finishedNanos());
                assertThat(intervals.get(index).generation()).isEqualTo(intervals.get(0).generation());
            }
            assertThat(intervals.get(3).itemCount()).isPositive();
            assertThat(intervals.get(4).itemCount()).isEqualTo(intervals.get(3).itemCount());
            var running = graph.transitions().stream().filter(transition ->
                    transition.stageId() == stage.stageId() && "RUNNING".equals(transition.state()))
                    .findFirst().orElseThrow();
            assertThat(intervals.get(4).finishedNanos()).isLessThanOrEqualTo(running.elapsedNanos());
        }
        assertThat(work).anySatisfy(interval -> {
            assertThat(interval.phase().name()).isEqualTo("ENUMERATE_SPLITS");
            assertThat(interval.itemCount()).isPositive();
        });
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
