package org.pangrova.coordinator.integration.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.http.CoordinatorHttpServerContext;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.scoring.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

/** Actual authenticated Worker control endpoints for lifecycle integration, not SQL E2E. */
final class WorkerBootstrapControlFixture implements AutoCloseable {
    private final String workerHost = actualWorkerHost();
    private final String secret = UUID.randomUUID().toString();
    private final AuthenticationManager authentication = new AuthenticationManager(List.of());
    private final RemoteTaskDispatcher transport = new RemoteTaskDispatcher(
            CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
    private final QueryManager queries;
    private final CoordinatorHttpServer server;
    private final List<WorkerBootstrap> workers = new ArrayList<>();
    private final WorkerManager registrations =
            new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());

    WorkerBootstrapControlFixture() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        var context = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, transport,
                new TaskScheduler(new WorkerPerformanceScorer()), registrations, tracker, null, null, null, 30L);
        var dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(context), null, null);
        queries = new QueryManager(QueryManagerContext.minimal(dispatcher, TestObservability.TRACING));
        var receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(queries, tracker, registrations));
        server = new CoordinatorHttpServer(0, new CoordinatorHttpServerContext(receiver, queries,
                null, authentication, null, registrations, null, null, null, null, null, secret, () -> 1L));
        try { server.start(); }
        catch (Exception | Error failure) {
            try { close(); }
            catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    WorkerBootstrapConfig config(String workerId) {
        var original = TestShuffleServiceConfig.config(WorkerBootstrapConfig.withDefaults(workerId));
        return new WorkerBootstrapConfig(original.workerId(), 0, 0, "http://127.0.0.1:" + server.getPort(),
                original.heartbeatIntervalMs(), original.observabilityEnabled(),
                original.observabilityBootstrapConfig(), original.resourceConfig(), original.resultRetentionSeconds(),
                original.cdcEnabled(), original.registrationRetryConfig(), workerHost, secret,
                original.shuffleMode(), original.cacheConfig(), workerHost, null, original.nativeFunctionConfig(),
                original.transportSecurityConfig(), original.admissionStateDirectory());
    }

    WorkerBootstrap bootstrap(String workerId) {
        var worker = new WorkerBootstrap(config(workerId));
        workers.add(worker);
        worker.installProductionCatalogRuntime();
        return worker;
    }

    org.pangrova.coordinator.worker.WorkerInfo registration(String workerId) {
        return registrations.getWorker(workerId);
    }

    private static String actualWorkerHost() throws IOException {
        for (var network : NetworkInterface.networkInterfaces().toList()) {
            if (!network.isUp() || network.isLoopback()) {
                continue;
            }
            for (var address : network.inetAddresses().toList()) {
                if (address instanceof Inet4Address && !address.isLoopbackAddress()
                        && !address.isLinkLocalAddress() && !address.isAnyLocalAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        throw new IOException("Actual Worker registration requires an active non-loopback IPv4 interface");
    }

    @Override public void close() throws Exception {
        Throwable failure = null;
        for (int index = workers.size() - 1; index >= 0; index--) {
            try { workers.get(index).shutdown(); }
            catch (Exception | Error cleanup) {
                if (failure == null) failure = cleanup;
                else if (failure != cleanup) failure.addSuppressed(cleanup);
            }
        }
        for (AutoCloseable owner : new AutoCloseable[]{server::stop, queries::stopPeriodicCleanup,
                transport, authentication, queries.getStateJournal()}) {
            try { owner.close(); }
            catch (Exception | Error cleanup) {
                if (failure == null) failure = cleanup;
                else if (failure != cleanup) failure.addSuppressed(cleanup);
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof Exception exception) throw exception;
    }
}
