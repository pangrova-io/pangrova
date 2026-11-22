package org.pangrova.testing.integration.e2e.result;

import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.transport.client.FlightResultFetcher;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E: Backpressure under slow consumer.
 * Proves FlightResultServer handles large result sets without OOM
 * via Velox OutputBufferManager backpressure.
 *
 * <p>Validates: Requirements 4.1, 4.2, 4.3, 4.4</p>
 */
@DisplayName("Backpressure E2E — Large Result Sets")
class BackpressureE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int ROWS = 8_192;
    private static final String SHORT_TEXT = "a".repeat(2_048);
    private static final String LONG_TEXT = "b".repeat(16_384);
    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BackpressureE2ETest.class);

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                TestConnectorFactory.createIcebergConnector(icebergWarehouse()), 1);
    }

    @Test
    @DisplayName("Scenario 1 — Large result set streams without OOM")
    void largeResultSetStreamsCorrectly() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice "
                + "FROM lineitem ORDER BY l_orderkey, l_partkey");
        assertThat(rows).hasSizeGreaterThan(10);
        for (int i = 1; i < rows.size(); i++) {
            long prevKey = rows.get(i - 1).getLong("l_orderkey");
            long currKey = rows.get(i).getLong("l_orderkey");
            assertThat(currKey).isGreaterThanOrEqualTo(prevKey);
        }
    }

    @Test
    void nativeFlightHandlesWideVariableRowsWithASlowReaderAndAnEarlyClose() throws Exception {
        String table = CATALOG + "." + SCHEMA + ".flight_width_" + Long.toHexString(System.nanoTime());
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");
        for (int first = 0; first < ROWS; first += 1_024) {
            String rows = java.util.stream.IntStream.range(first, first + 1_024)
                    .mapToObj(value -> "(" + value + ")").collect(java.util.stream.Collectors.joining(","));
            sqlRunner.executeDdl("INSERT INTO " + table + " VALUES " + rows);
        }
        String wide = "SELECT id, CASE WHEN id < " + ROWS / 2 + " THEN CAST('" + SHORT_TEXT
                + "' AS VARCHAR) ELSE CAST('" + LONG_TEXT + "' AS VARCHAR) END AS payload FROM " + table;
        try (var client = PangrovaRestClient.builder().coordinatorHost("localhost")
                .coordinatorPort(coordinatorHttpPort()).bearerToken(E2E_USER_BEARER_TOKEN)
                .retryPolicy(RetryPolicy.NONE).build()) {
            readEveryRow(client, "SELECT id, CAST(id AS VARCHAR) AS payload FROM " + table, false);
            readEveryRow(client, wide, true);
            long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
            var query = client.beginQuery(wide, Map.of());
            CancelOutcome cancelled;
            try (var cursor = client.openResult(endpoint(query, deadline), deadline)) {
                assertThat(cursor.next()).isTrue();
                cancelled = query.cancel();
            }
            assertThat(cancelled).isIn(CancelOutcome.CANCEL_ACCEPTED, CancelOutcome.ALREADY_TERMINAL);
            String finalState = awaitPhysicalDrain(query, deadline);
            assertThat(finalState).isEqualTo(cancelled == CancelOutcome.CANCEL_ACCEPTED ? "CANCELLED" : "FINISHED");
            LOG.info("event=flight_width_reader_closed queryId={} outcome={} finalState={}",
                    query.submission().queryId(), cancelled, finalState);
            verifyAbruptFlightDisconnect(client, wide);
        } finally {
            sqlRunner.executeDdl("DROP TABLE IF EXISTS " + table);
        }
    }

    private static void verifyAbruptFlightDisconnect(PangrovaRestClient client, String sql) throws Exception {
        Path firstRow = Files.createTempFile("pangrova-flight-first-row-", ".signal");
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        String childClasspath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = null;
        try {
            child = new ProcessBuilder(java.toString(), "-Xmx256m", "-Dio.netty.maxDirectMemory=0",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", childClasspath,
                    InterruptedFlightClient.class.getName(), firstRow.toString())
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
            var query = client.beginQuery(sql, Map.of());
            var route = endpoint(query, deadline);
            try (var command = new DataOutputStream(child.getOutputStream())) {
                command.writeUTF(route.host());
                command.writeInt(route.port());
                command.writeUTF(route.ticket());
                command.writeUTF(route.protocol());
                command.writeLong(deadline);
            }
            Process reader = child;
            org.awaitility.Awaitility.await("independent client received its first Flight row")
                    .pollInterval(Duration.ofMillis(25)).atMost(Duration.ofSeconds(20))
                    .until(() -> {
                        assertThat(reader.isAlive()).isTrue();
                        return Files.size(firstRow) > 0;
                    });

            String queryId = query.submission().queryId();
            assertThat(queryManager.getStateMachine(queryId).physicalCleanupApplied()).isFalse();
            long disconnectedAt = System.nanoTime();
            child.destroyForcibly();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            long drainDeadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
            var status = query.awaitStatus(QueryWaitTarget.DRAINED, drainDeadline);
            while (!Boolean.TRUE.equals(status.drained())) {
                status = query.awaitStatus(QueryWaitTarget.DRAINED, drainDeadline);
            }
            assertThat(status.state()).isEqualTo("FAILED");
            assertThat(status.error()).isNotNull();
            assertThat(status.error().message()).contains("closed before committed EOF");
            taskTracker.requireQueryExecutionDrained(queryId);
            assertThat(queryManager.getStateMachine(queryId).physicalCleanupApplied()).isTrue();
            LOG.info("event=flight_client_process_exit queryId={} terminalState={} drainMs={}",
                    queryId, status.state(), millis(System.nanoTime() - disconnectedAt));
        } finally {
            if (child != null && child.isAlive()) {
                child.destroyForcibly();
                assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
            Files.deleteIfExists(firstRow);
        }
    }

    /** The parent kills this process after the first row, leaving the server to observe the socket closing. */
    public static final class InterruptedFlightClient {
        public static void main(String[] args) throws Exception {
            var command = new DataInputStream(System.in);
            var route = new ResultEndpoint(command.readUTF(), command.readInt(), command.readUTF(), command.readUTF());
            long deadline = command.readLong();
            try (var fetcher = new FlightResultFetcher(); var cursor = fetcher.open(route, deadline)) {
                if (!cursor.next()) throw new IllegalStateException("Flight returned no first row");
                Files.writeString(Path.of(args[0]), "read");
                new java.util.concurrent.CountDownLatch(1).await();
            }
        }
    }

    private static void readEveryRow(PangrovaRestClient client, String sql, boolean wide) throws Exception {
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        long startedAt = System.nanoTime();
        var query = client.beginQuery(sql, Map.of());
        var seen = new BitSet(ROWS);
        long payloadBytes = 0;
        long firstRow = 0;
        try (var cursor = client.openResult(endpoint(query, deadline), deadline)) {
            while (cursor.next()) {
                if (firstRow == 0) {
                    firstRow = System.nanoTime();
                    if (wide) Thread.sleep(250);
                }
                int id = ((Number) cursor.value(0)).intValue();
                assertThat(id).isBetween(0, ROWS - 1);
                assertThat(seen.get(id)).isFalse();
                seen.set(id);
                String text = cursor.value(1).toString();
                if (wide) {
                    assertThat(text).isEqualTo(id < ROWS / 2 ? SHORT_TEXT : LONG_TEXT);
                } else {
                    assertThat(text).isEqualTo(Integer.toString(id));
                }
                payloadBytes += text.length();
            }
        }
        long eof = System.nanoTime();
        assertThat(seen.cardinality()).isEqualTo(ROWS);
        if (wide) assertThat(payloadBytes).isEqualTo((long) (SHORT_TEXT.length() + LONG_TEXT.length()) * ROWS / 2);
        assertThat(awaitPhysicalDrain(query, deadline)).isEqualTo("FINISHED");
        LOG.info("event=flight_width_sample queryId={} wide={} rows={} payloadBytes={} firstMs={} eofMs={} drainMs={}",
                query.submission().queryId(), wide, seen.cardinality(), payloadBytes,
                millis(firstRow - startedAt), millis(eof - startedAt), millis(System.nanoTime() - startedAt));
    }

    private static ResultEndpoint endpoint(QueryExecution query, long deadline) {
        ResultEndpoint endpoint = query.submission().resultEndpoint();
        while (endpoint == null) {
            var status = query.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
            assertThat(status.state()).isNotIn("FAILED", "CANCELLED");
            endpoint = status.resultEndpoint();
            if ("FINISHED".equals(status.state())) assertThat(endpoint).isNotNull();
        }
        return endpoint;
    }

    private static String awaitPhysicalDrain(QueryExecution query, long deadline) {
        var status = query.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        while (!Boolean.TRUE.equals(status.drained())) {
            assertThat(status.state()).isNotEqualTo("FAILED");
            status = query.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        }
        String queryId = query.submission().queryId();
        taskTracker.requireQueryExecutionDrained(queryId);
        assertThat(queryManager.getStateMachine(queryId).physicalCleanupApplied()).isTrue();
        return status.state();
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    @Test
    @DisplayName("Scenario 2 — Wide result set with many columns")
    void wideResultSet() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM lineitem ORDER BY l_orderkey LIMIT 50");
        assertThat(rows).hasSize(50);
        TypedRow first = rows.get(0);
        assertThat(first.getLong("l_orderkey")).isNotNull();
        assertThat(first.getLong("l_quantity")).isNotNull();
        assertThat(first.getDouble("l_extendedprice")).isNotNull();
    }
}
