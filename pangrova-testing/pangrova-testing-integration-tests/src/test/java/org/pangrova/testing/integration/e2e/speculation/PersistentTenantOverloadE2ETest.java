package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.exception.PangrovaServerException;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** Offers fixed arrivals to a real tenant while its Native output owners hold admission slots. */
@E2ETest
@Tag("performance")
class PersistentTenantOverloadE2ETest extends ContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentTenantOverloadE2ETest.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String TABLE = "spec_catalog.default.fixed_arrivals";
    private static final List<Workload> WORKLOADS = List.of(
            new Workload("constant", "SELECT CAST(42 AS BIGINT)", List.of(42L)),
            new Workload("point", "SELECT id FROM " + TABLE + " WHERE id = 5", List.of(5L)),
            new Workload("scan", "SELECT id FROM " + TABLE + " WHERE id >= 5 ORDER BY id", List.of(5L, 6L, 7L)),
            new Workload("count", "SELECT COUNT(*) FROM " + TABLE, List.of(8L)),
            new Workload("group", "SELECT COUNT(*) FROM " + TABLE + " GROUP BY shard ORDER BY shard",
                    List.of(4L, 4L)));

    @Test
    void observesQuotaRejectionThenReadsAndPhysicallyReleasesFixedArrivals() throws Exception {
        try (var processes = new IndependentLocalProcesses()) {
            try {
                runArrivals(processes);
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Process diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    @Test
    void resolvesTenantLimitsOnceAndChecksMembershipAgainOnTheNextSubmission() throws Exception {
        String membershipLookup = "SELECT t.*, m.role AS membership_role FROM tenants t "
                + "JOIN tenant_user_memberships m ON m.tenant_id=t.tenant_id WHERE m.user_principal=$1";
        String tenantLookup = "SELECT * FROM tenants WHERE tenant_id = $1";
        String placementLookup = "SELECT status, resource_pool FROM tenants WHERE tenant_id = $1";
        try (var processes = new IndependentLocalProcesses()) {
            processes.startCoordinatorAndFirstWorker();
            processes.enableMetadataStatementCounters();
            try (var client = processes.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
                long initialMembership = processes.metadataStatementCalls(membershipLookup);
                long initialTenant = processes.metadataStatementCalls(tenantLookup);
                long initialPlacement = processes.metadataStatementCalls(placementLookup);
                long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
                var query = client.beginQuery("SELECT CAST(42 AS BIGINT)", Map.of());
                finish(client, query, awaitResult(query, deadline), List.of(42L), deadline);
                assertThat(processes.activeTenantReservations()).isZero();
                long membershipReads = processes.metadataStatementCalls(membershipLookup) - initialMembership;
                long tenantReads = processes.metadataStatementCalls(tenantLookup) - initialTenant;
                long placementReads = processes.metadataStatementCalls(placementLookup) - initialPlacement;
                LOG.info("event=tenant_admission_reads membership={} tenantById={} placement={}",
                        membershipReads, tenantReads, placementReads);
                assertThat(membershipReads).as("authorization membership lookups").isOne();
                assertThat(tenantReads).as("full tenant reads for placement").isZero();
                assertThat(placementReads).as("current placement read for one cohort").isOne();

                processes.revokeTenantMembership();
                var revoked = catchThrowableOfType(() -> client.beginQuery("SELECT CAST(43 AS BIGINT)", Map.of()),
                        PangrovaClientException.class);
                assertThat(revoked).isNotNull().isNotInstanceOf(PangrovaServerException.class);
                assertThat(revoked.errorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED.getCode());
                assertThat(processes.metadataStatementCalls(membershipLookup) - initialMembership).isEqualTo(2L);
                assertThat(processes.metadataStatementCalls(tenantLookup) - initialTenant).isZero();
                assertThat(processes.metadataStatementCalls(placementLookup) - initialPlacement).isOne();
                assertThat(processes.activeTenantReservations()).isZero();
            }
        }
    }

    @Test
    void occupiedSharedSlotsReturnTheTenantQuotaResponseBeforeAnyLocalQueryRuns() throws Exception {
        try (var processes = new IndependentLocalProcesses()) {
            processes.startCoordinatorAndFirstWorker();
            String first = "peer-query-1";
            String second = "peer-query-2";
            assertThat(processes.reservePeerTenantSlot(first)).isTrue();
            try (var client = processes.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
                assertThat(processes.reservePeerTenantSlot(second)).isTrue();
                assertThat(processes.activeTenantReservations()).isEqualTo(2);
                var denied = catchThrowableOfType(() -> client.beginQuery("SELECT CAST(42 AS BIGINT)", Map.of()),
                        PangrovaClientException.class);
                assertThat(denied).isNotNull();
                LOG.info("event=peer_slot_admission errorCode={} serverError={}",
                        denied.errorCode(), denied instanceof PangrovaServerException);
                assertThat(denied).isNotInstanceOf(PangrovaServerException.class);
                assertThat(denied.errorCode()).isEqualTo(PangrovaErrorCode.TENANT_QUOTA_EXCEEDED.getCode());
                assertThat(processes.activeTenantReservations()).isEqualTo(2);
            } finally {
                processes.releasePeerTenantSlot(second);
                processes.releasePeerTenantSlot(first);
            }
            assertThat(processes.activeTenantReservations()).isZero();
            try (var client = processes.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
                long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
                var accepted = client.beginQuery("SELECT CAST(42 AS BIGINT)", Map.of());
                finish(client, accepted, awaitResult(accepted, deadline), List.of(42L), deadline);
                assertThat(processes.activeTenantReservations()).isZero();
            }
        }
    }

    private static void runArrivals(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        processes.executeWithoutRows("CREATE TABLE " + TABLE + " (id BIGINT, shard BIGINT) PARTITIONED BY (shard)",
                ResultProtocol.GRPC_TLS);
        processes.executeWithoutRows("INSERT INTO " + TABLE + " VALUES (0,0),(1,0),(2,0),(3,0)", ResultProtocol.GRPC_TLS);
        processes.executeWithoutRows("INSERT INTO " + TABLE + " VALUES (4,1),(5,1),(6,1),(7,1)", ResultProtocol.GRPC_TLS);
        processes.startSecondWorker();

        try (var client = processes.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
            for (var workload : WORKLOADS) {
                long warmupDeadline = System.currentTimeMillis() + TIMEOUT.toMillis();
                var warmup = client.beginQuery(workload.sql(), Map.of());
                finish(client, warmup, awaitResult(warmup, warmupDeadline), workload.expected(), warmupDeadline);
            }
            assertThat(processes.activeTenantReservations()).isZero();

            var first = client.beginQuery("SELECT CAST(81 AS BIGINT)", Map.of());
            var second = client.beginQuery("SELECT CAST(82 AS BIGINT)", Map.of());
            long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
            var firstEndpoint = awaitResult(first, deadline);
            var secondEndpoint = awaitResult(second, deadline);
            assertThat(processes.activeTenantReservations()).isEqualTo(2);

            offer(45, Duration.ofMillis(50), 8, (workload, scheduled) -> reject(client, workload, scheduled));
            assertThat(processes.activeTenantReservations()).isEqualTo(2);
            finish(client, first, firstEndpoint, List.of(81L), deadline);
            finish(client, second, secondEndpoint, List.of(82L), deadline);
            assertThat(processes.activeTenantReservations()).isZero();
            offer(40, Duration.ofMillis(200), 1, (workload, scheduled) -> complete(client, workload, scheduled));
            assertThat(processes.activeTenantReservations()).isZero();
            assertThat(processes.reports().forwardingFailures()).isEmpty();
        }
    }

    private static void offer(int count, Duration interval, int concurrency,
                              BiConsumer<Workload, Long> operation) throws Exception {
        var pool = ManagedExecutorFactory.newScheduledThreadPool("tenant-overload-arrivals", concurrency);
        var jobs = new ArrayList<Future<?>>(count);
        long first = System.nanoTime() + interval.toNanos();
        try {
            for (int index = 0; index < count; index++) {
                long scheduled = first + index * interval.toNanos();
                var workload = WORKLOADS.get(index % WORKLOADS.size());
                jobs.add(pool.schedule(() -> operation.accept(workload, scheduled),
                        Math.max(0, scheduled - System.nanoTime()), TimeUnit.NANOSECONDS));
            }
            for (var job : jobs) job.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } finally {
            ExecutorDrain.shutdownAndAwait(pool, TIMEOUT.toMillis());
        }
    }

    private static void reject(PangrovaRestClient client, Workload workload, long scheduled) {
        long started = System.nanoTime();
        var failure = catchThrowableOfType(() -> client.beginQuery(workload.sql(), Map.of()),
                PangrovaClientException.class);
        long rejected = System.nanoTime();
        assertThat(failure).isNotNull().isNotInstanceOf(PangrovaServerException.class);
        assertThat(failure.errorCode()).isEqualTo(PangrovaErrorCode.TENANT_QUOTA_EXCEEDED.getCode());
        LOG.info("event=tenant_arrival result=quota_rejection workload={} submitUs={} queueUs={}",
                workload.name(), micros(rejected, scheduled), micros(started, scheduled));
    }

    private static void complete(PangrovaRestClient client, Workload workload, long scheduled) {
        long started = System.nanoTime();
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        var query = client.beginQuery(workload.sql(), Map.of());
        long submitted = System.nanoTime();
        var endpoint = awaitResult(query, deadline);
        long ready = System.nanoTime();
        long eof = finish(client, query, endpoint, workload.expected(), deadline);
        LOG.info("event=tenant_arrival result=drained workload={} submitUs={} queueUs={} readyUs={} "
                        + "eofUs={} drainUs={}",
                workload.name(), micros(submitted, scheduled), micros(started, scheduled),
                micros(ready, scheduled), micros(eof, scheduled), micros(System.nanoTime(), scheduled));
    }

    private static ResultEndpoint awaitResult(QueryExecution query, long deadline) {
        var endpoint = query.submission().resultEndpoint();
        while (endpoint == null) {
            var status = query.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
            assertThat(status.state()).as("Query %s error %s", status.queryId(), status.error())
                    .isNotIn("FAILED", "CANCELLED");
            assertThat(status.resultDisposition())
                    .isEqualTo(org.pangrova.common.enums.ResultDisposition.STREAM);
            endpoint = status.resultEndpoint();
        }
        assertThat(endpoint.protocol()).isEqualTo(ResultProtocol.GRPC_TLS.value());
        return endpoint;
    }

    private static long finish(PangrovaRestClient client, QueryExecution query, ResultEndpoint endpoint,
                               List<Long> expected, long deadline) {
        var values = new ArrayList<Long>();
        try (var cursor = client.openResult(endpoint, deadline)) {
            while (cursor.next()) {
                values.add(((Number) cursor.value(0)).longValue());
            }
        }
        long eof = System.nanoTime();
        assertThat(values).containsExactlyElementsOf(expected);
        var status = query.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        while (!Boolean.TRUE.equals(status.drained())) {
            assertThat(status.state()).as("Query %s error %s", status.queryId(), status.error())
                    .isNotIn("FAILED", "CANCELLED");
            status = query.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        }
        assertThat(status.state()).isEqualTo("FINISHED");
        return eof;
    }

    private static long micros(long timestamp, long origin) {
        return TimeUnit.NANOSECONDS.toMicros(timestamp - origin);
    }

    private record Workload(String name, String sql, List<Long> expected) { }
}
