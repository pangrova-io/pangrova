package org.pangrova.testing.integration.e2e.performance;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Measures an authenticated SDK client across HTTP, Native Flight and query ownership release. */
@Tag("performance")
class PersistentQueryLatencyTest extends SqlE2ETestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentQueryLatencyTest.class);
    private static final String CATALOG = "iceberg_catalog";
    private static final String TABLE = CATALOG + ".default.persistent_latency";
    private static final Duration QUERY_BUDGET = Duration.ofSeconds(30);
    private static final long ARRIVAL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(125);
    private static final Workload CONSTANT = new Workload("constant", "SELECT 42", List.of(42L));
    private static final Workload POINT = new Workload("point",
            "SELECT score FROM " + TABLE + " WHERE shard = 0 AND id = 49", List.of(98L));
    private static final Workload SELECTIVE = new Workload("selective",
            "SELECT id FROM " + TABLE + " WHERE id BETWEEN 96 AND 99 ORDER BY id", List.of(96L, 97L, 98L, 99L));
    private static final Workload AGGREGATE = new Workload("count",
            "SELECT COUNT(*) AS row_count FROM " + TABLE, List.of(512L));
    private static final Workload GROUPED = new Workload("grouped",
            "SELECT grp, COUNT(*) AS row_count FROM " + TABLE + " GROUP BY grp ORDER BY grp",
            LongStream.range(0, 16).boxed().toList());
    private static final List<Workload> WORKLOADS = List.of(CONSTANT, POINT, SELECTIVE, AGGREGATE, GROUPED);

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, "default", icebergWarehouse().toString()));
    }

    @BeforeAll
    static void seed() {
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, "default", icebergWarehouse().toString())));
        sqlRunner.executeDdl("CREATE TABLE " + TABLE
                + " (id BIGINT, score BIGINT, grp BIGINT, shard BIGINT) PARTITIONED BY (shard)");
        for (int shard = 0; shard < 4; shard++) {
            var insert = new StringBuilder("INSERT INTO ").append(TABLE).append(" VALUES ");
            for (int index = 0; index < 128; index++) {
                long id = shard * 128L + index;
                if (index != 0) insert.append(", ");
                insert.append('(').append(id).append(", ").append(2 * id)
                        .append(", ").append(id % 16).append(", ").append(shard).append(')');
            }
            sqlRunner.execute(insert.toString());
        }
    }

    @Test
    void reportsSingleClientAndFixedArrivalLatency() throws Exception {
        try (var client = PangrovaRestClient.builder().coordinatorHost("localhost")
                .coordinatorPort(coordinatorHttpPort()).bearerToken(E2E_USER_BEARER_TOKEN)
                .retryPolicy(RetryPolicy.NONE).build()) {
            for (int round = 0; round < 4; round++) {
                for (var workload : WORKLOADS) measure(client, workload, System.nanoTime(), "warmup");
            }
            for (int round = 0; round < 30; round++) {
                for (var workload : WORKLOADS) measure(client, workload, System.nanoTime(), "serial");
            }
            measureFixedArrivals(client);
        }
    }

    private static void measureFixedArrivals(PangrovaRestClient client) throws Exception {
        var arrivals = ManagedExecutorFactory.newScheduledThreadPool("query-latency-arrivals", 8);
        List<Future<?>> ready = new ArrayList<>();
        long firstArrival = System.nanoTime() + ARRIVAL_INTERVAL_NANOS;
        try {
            for (int index = 0; index < 80; index++) {
                long scheduled = firstArrival + index * ARRIVAL_INTERVAL_NANOS;
                // Each cycle offers one grouped query alongside the four interactive workloads.
                var workload = WORKLOADS.get(index % WORKLOADS.size());
                ready.add(arrivals.schedule(() -> measure(client, workload, scheduled, "fixed"),
                        Math.max(0, scheduled - System.nanoTime()), TimeUnit.NANOSECONDS));
            }
            for (var query : ready) query.get(QUERY_BUDGET.toSeconds(), TimeUnit.SECONDS);
        } finally {
            ExecutorDrain.shutdownAndAwait(arrivals, QUERY_BUDGET.toMillis());
        }
    }

    private static void measure(PangrovaRestClient client, Workload workload, long scheduled, String mode) {
        long started = System.nanoTime();
        long deadline = System.currentTimeMillis() + QUERY_BUDGET.toMillis();
        var execution = client.beginQuery(workload.sql(), Map.of());
        long submitted = System.nanoTime();
        int statusCalls = 0;
        var endpoint = execution.submission().resultEndpoint();
        QueryStatusResponse status = null;
        while (endpoint == null) {
            status = execution.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
            statusCalls++;
            requireSuccessOrActive(status);
            endpoint = status.resultEndpoint();
            if ("FINISHED".equals(status.state()) && endpoint == null) {
                throw new AssertionError("Finished query did not publish a result endpoint");
            }
        }
        long ready = System.nanoTime();
        long firstRow;
        var values = new ArrayList<Long>();
        try (var cursor = client.openResult(endpoint, deadline)) {
            assertThat(cursor.next()).isTrue();
            firstRow = System.nanoTime();
            do {
                values.add(((Number) cursor.value(0)).longValue());
                if (workload == GROUPED) assertThat(((Number) cursor.value(1)).longValue()).isEqualTo(32L);
            } while (cursor.next());
        }
        long eof = System.nanoTime();
        while (status == null || !"FINISHED".equals(status.state())) {
            status = execution.awaitStatus(QueryWaitTarget.TERMINAL, deadline);
            statusCalls++;
            requireSuccessOrActive(status);
        }
        long terminal = System.nanoTime();
        while (!Boolean.TRUE.equals(status.drained())) {
            status = execution.awaitStatus(QueryWaitTarget.DRAINED, deadline);
            statusCalls++;
            requireSuccessOrActive(status);
        }
        long drained = System.nanoTime();
        assertThat(values).containsExactlyElementsOf(workload.expected());
        if (!"warmup".equals(mode)) {
            LOG.info("event=persistent_query_sample mode={} workload={} queryId={} queueUs={} submitUs={} "
                            + "readyUs={} firstRowUs={} eofUs={} terminalUs={} drainedUs={} statusCalls={}",
                    mode, workload.name(), execution.submission().queryId(),
                    micros(started, scheduled), micros(submitted, scheduled), micros(ready, scheduled),
                    micros(firstRow, scheduled), micros(eof, scheduled), micros(terminal, scheduled),
                    micros(drained, scheduled), statusCalls);
        }
    }

    private static long micros(long timestamp, long origin) {
        return TimeUnit.NANOSECONDS.toMicros(timestamp - origin);
    }

    private static void requireSuccessOrActive(QueryStatusResponse status) {
        assertThat(status.state()).as("query %s status %s", status.queryId(), status.error())
                .isNotIn("FAILED", "CANCELLED");
    }

    private record Workload(String name, String sql, List<Long> expected) { }
}
