package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.exception.PangrovaServerException;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.pangrova.testing.integration.e2e.speculation.ProcessQueryResults.awaitResult;
import static org.pangrova.testing.integration.e2e.speculation.ProcessQueryResults.consumeAndDrain;

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
                consumeAndDrain(client, query, awaitResult(query, deadline), List.of(42L), deadline);
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
    void independentCoordinatorsShareTenantQuotaAndDrainMixedSql() throws Exception {
        try (var primary = new IndependentLocalProcesses()) {
            primary.startCoordinatorAndFirstWorker();
            seedArrivalTable(primary);
            try (var peer = primary.startPeer()) {
                try {
                    verifySharedQuota(primary, peer);
                } catch (Exception | Error failure) {
                    failure.addSuppressed(new IllegalStateException(
                            "Coordinator diagnostics retained at " + primary.retainDiagnostics()
                                    + " and " + peer.retainDiagnostics()));
                    throw failure;
                }
            }
        }
    }

    private static void verifySharedQuota(IndependentLocalProcesses primary, IndependentLocalProcesses peer)
            throws Exception {
        assertThat(peer.coordinatorPid()).isNotEqualTo(primary.coordinatorPid());
        assertThat(peer.coordinatorPort()).isNotEqualTo(primary.coordinatorPort());
        assertThat(peer.hostJdbcUrl()).isEqualTo(primary.hostJdbcUrl());
        primary.enableMetadataStatementCounters();
        try (var first = primary.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build();
             var second = peer.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
            var clients = List.of(first, second);
            for (var client : clients) complete(client, WORKLOADS.get(1), System.nanoTime());
            assertThat(primary.activeTenantReservations()).isZero();

            long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
            var firstQuery = first.beginQuery(WORKLOADS.get(1).sql(), Map.of());
            var secondQuery = second.beginQuery(WORKLOADS.get(3).sql(), Map.of());
            var firstEndpoint = awaitResult(firstQuery, deadline);
            var secondEndpoint = awaitResult(secondQuery, deadline);
            assertThat(peer.activeTenantReservations()).isEqualTo(2);

            String journal = "SELECT%FROM query_state_journal%";
            long snapshot = primary.metadataStatementCalls(journal);
            verifyReconnectedStatus(primary, firstQuery);
            verifyReconnectedStatus(peer, secondQuery);
            assertThat(primary.metadataStatementCalls(journal) - snapshot)
                    .as("journal SELECTs for authenticated status reads on live owners").isZero();

            offer(clients, 40, Duration.ofMillis(25), 8, PersistentTenantOverloadE2ETest::reject);
            assertThat(primary.activeTenantReservations()).isEqualTo(2);
            consumeAndDrain(first, firstQuery, firstEndpoint, WORKLOADS.get(1).expected(), deadline);
            assertThat(peer.activeTenantReservations()).isOne();
            consumeAndDrain(second, secondQuery, secondEndpoint, WORKLOADS.get(3).expected(), deadline);
            assertThat(primary.activeTenantReservations()).isZero();

            offer(clients, 80, Duration.ofMillis(125), 2, PersistentTenantOverloadE2ETest::complete);
            assertThat(primary.activeTenantReservations()).isZero();
            assertThat(primary.reports().forwardingFailures()).isEmpty();
            assertThat(peer.reports().forwardingFailures()).isEmpty();
            requireReportsFromOwner(primary, firstQuery);
            requireReportsFromOwner(peer, secondQuery);
        }
    }

    private static void verifyReconnectedStatus(IndependentLocalProcesses process, QueryExecution active) {
        try (var observer = process.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
            for (int read = 0; read < 12; read++) {
                var status = observer.getQueryStatus(active.submission().queryId());
                assertThat(status.state()).isNotIn("FAILED", "CANCELLED");
                assertThat(status.resultEndpoint()).isNotNull();
            }
        }
    }

    private static void requireReportsFromOwner(IndependentLocalProcesses process, QueryExecution query) {
        var observed = process.reports().deliveries().stream().map(ProcessStatusReportRelay.Delivery::report)
                .filter(report -> query.submission().queryId().equals(report.queryId())).toList();
        assertThat(observed).isNotEmpty().allSatisfy(report ->
                assertThat(report.reportedAttempt().getExecutionOwner().getOwnerId())
                        .isEqualTo(process.coordinatorId()));
    }

    private static void seedArrivalTable(IndependentLocalProcesses processes) throws Exception {
        processes.createSourceCatalog();
        processes.query("CREATE TABLE " + TABLE + " (id BIGINT, shard BIGINT) PARTITIONED BY (shard)",
                ResultProtocol.GRPC_TLS);
        processes.query("INSERT INTO " + TABLE + " VALUES (0,0),(1,0),(2,0),(3,0)", ResultProtocol.GRPC_TLS);
        processes.query("INSERT INTO " + TABLE + " VALUES (4,1),(5,1),(6,1),(7,1)", ResultProtocol.GRPC_TLS);
    }

    private static void runArrivals(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        seedArrivalTable(processes);
        processes.startSecondWorker();

        try (var client = processes.coordinatorClientBuilder().retryPolicy(RetryPolicy.NONE).build()) {
            for (var workload : WORKLOADS) {
                long warmupDeadline = System.currentTimeMillis() + TIMEOUT.toMillis();
                var warmup = client.beginQuery(workload.sql(), Map.of());
                consumeAndDrain(client, warmup, awaitResult(warmup, warmupDeadline), workload.expected(), warmupDeadline);
            }
            assertThat(processes.activeTenantReservations()).isZero();

            var first = client.beginQuery("SELECT CAST(81 AS BIGINT)", Map.of());
            var second = client.beginQuery("SELECT CAST(82 AS BIGINT)", Map.of());
            long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
            var firstEndpoint = awaitResult(first, deadline);
            var secondEndpoint = awaitResult(second, deadline);
            assertThat(processes.activeTenantReservations()).isEqualTo(2);

            offer(List.of(client), 45, Duration.ofMillis(50), 8, PersistentTenantOverloadE2ETest::reject);
            assertThat(processes.activeTenantReservations()).isEqualTo(2);
            consumeAndDrain(client, first, firstEndpoint, List.of(81L), deadline);
            consumeAndDrain(client, second, secondEndpoint, List.of(82L), deadline);
            assertThat(processes.activeTenantReservations()).isZero();
            offer(List.of(client), 40, Duration.ofMillis(200), 1, PersistentTenantOverloadE2ETest::complete);
            assertThat(processes.activeTenantReservations()).isZero();
            assertThat(processes.reports().forwardingFailures()).isEmpty();
        }
    }

    @FunctionalInterface
    private interface Arrival {
        void run(PangrovaRestClient client, Workload workload, long scheduled);
    }

    private static void offer(List<PangrovaRestClient> clients, int count, Duration interval, int concurrency,
                              Arrival operation) throws Exception {
        var pool = ManagedExecutorFactory.newScheduledThreadPool("tenant-overload-arrivals", concurrency);
        var jobs = new ArrayList<Future<?>>(count);
        long first = System.nanoTime() + interval.toNanos();
        try {
            for (int index = 0; index < count; index++) {
                long scheduled = first + index * interval.toNanos();
                var workload = WORKLOADS.get(index % WORKLOADS.size());
                var client = clients.get(index % clients.size());
                jobs.add(pool.schedule(() -> operation.run(client, workload, scheduled),
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
        var timing = consumeAndDrain(client, query, endpoint, workload.expected(), deadline);
        LOG.info("event=tenant_arrival result=drained workload={} submitUs={} queueUs={} readyUs={} "
                        + "firstRowUs={} eofUs={} drainUs={}",
                workload.name(), micros(submitted, scheduled), micros(started, scheduled),
                micros(ready, scheduled), micros(timing.firstRow(), scheduled),
                micros(timing.eof(), scheduled), micros(System.nanoTime(), scheduled));
    }

    private static long micros(long timestamp, long origin) {
        return TimeUnit.NANOSECONDS.toMicros(timestamp - origin);
    }

    private record Workload(String name, String sql, List<Long> expected) { }
}
