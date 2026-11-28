package org.pangrova.worker.bootstrap.phase;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.common.exception.BootstrapFailedException;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.registration.WorkerRegistrationClient;
import org.pangrova.worker.bootstrap.lifecycle.WorkerShutdownSequence;
import org.pangrova.worker.heartbeat.HeartbeatConfig;
import org.pangrova.protocol.rest.worker.HeartbeatRequest;
import org.pangrova.protocol.rest.worker.TenantUsageSnapshot;
import org.pangrova.worker.heartbeat.HeartbeatSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Worker heartbeat lifecycle: periodic heartbeat sending,
 * Coordinator registration, and deregistration.
 */
public final class WorkerHeartbeatBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerHeartbeatBootstrap.class);

    private WorkerHeartbeatBootstrap() {}

    /**
     * Starts periodic heartbeat sending to the Coordinator.
     */
    public static void startHeartbeat(WorkerBootstrapConfig config, WorkerComponentRefs refs) {
        String coordinatorUrl = config.coordinatorUrl();
        if (coordinatorUrl == null || coordinatorUrl.isBlank()) {
            LOG.info("event=phase_skipped phase=heartbeat reason=no_coordinator_url");
            return;
        }
        ScheduledExecutorService executor = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                "worker-heartbeat");
        refs.setHeartbeatExecutor(executor);
        HeartbeatSender sender = createHeartbeatSender(config, refs, executor);
        sender.start();
        refs.setHeartbeatSender(sender);
        LOG.info("event=phase_started phase=heartbeat workerId={}", config.workerId());
    }

    private static HeartbeatSender createHeartbeatSender(
            WorkerBootstrapConfig config, WorkerComponentRefs refs,
            ScheduledExecutorService executor) {
        HeartbeatConfig hbConfig = new HeartbeatConfig(
                config.coordinatorUrl(), config.workerId(), config.heartbeatIntervalMs());
        HeartbeatSender sender = new HeartbeatSender(
                hbConfig, () -> collectHeartbeatRequest(config, refs), executor,
                config.internalAuthSecret());
        sender.setCoordinatorEpochObserver(epoch -> {
            if (refs.taskManager() != null) {
                refs.taskManager().observeControlEpoch(epoch);
            }
        });
        sender.setFencedCallback(() -> refs.taskManager().fenceAdmission());
        sender.setReRegistrationCallback(() -> {
            try {
                register(config, refs);
                LOG.info("event=heartbeat_triggered_reregistration workerId={}", config.workerId());
            } catch (Exception e) {
                LOG.warn("event=heartbeat_reregistration_failed workerId={} error={}",
                        config.workerId(), e.getMessage());
            }
        });
        return sender;
    }

    /**
     * Registers this Worker with the Coordinator via HTTP.
     */
    public static void register(WorkerBootstrapConfig config, WorkerComponentRefs refs) {
        String coordinatorUrl = config.coordinatorUrl();
        if (coordinatorUrl == null || coordinatorUrl.isBlank()) {
            LOG.info("event=phase_skipped phase=registration reason=no_coordinator_url");
            return;
        }
        WorkerRegistrationClient client = refs.registrationClient();
        boolean ownsNewClient = client == null;
        if (ownsNewClient) {
            client = new WorkerRegistrationClient(
                    coordinatorUrl, config.internalAuthSecret());
        }
        String httpHost = WorkerHttpBootstrap.resolveAdvertisedHost(config);
        int httpPort = refs.httpServer() != null ? refs.httpServer().getPort() : 0;
        int resultPort = Math.max(0, refs.resultServerPort());
        int transportPort = 0;
        org.pangrova.protocol.rest.worker.WorkerRegistrationRequest request =
                new org.pangrova.protocol.rest.worker.WorkerRegistrationRequest(
                        config.workerId(), httpHost,
                        httpPort, transportPort, resultPort,
                        CanonicalProtocolIdentity.PROTOCOL_MAJOR,
                        null, null, config.shuffleMode().name(),
                        refs.nativeCapabilityInventory(),
                        refs.httpServer() != null ? refs.httpServer().getCopyTaskTlsPort() : 0,
                        refs.taskManager().workerExecutionIdentity(),
                        config.shuffleMode() == org.pangrova.config.enums.ShuffleMode.REMOTE
                                ? config.resourceConfig().shuffleProvider().id() : null,
                        refs.taskManager().cdcIngestSupported());
        long coordinatorEpoch;
        try {
            coordinatorEpoch = attemptRegistration(client, request, config);
        } catch (RuntimeException e) {
            if (ownsNewClient) {
                client.close();
            }
            throw e;
        }
        if (refs.taskManager() != null) {
            refs.taskManager().observeControlEpoch(coordinatorEpoch);
        }
        refs.setRegistrationClient(client);
        LOG.info("event=registration_sent coordinatorUrl={} coordinatorEpoch={}",
                coordinatorUrl, coordinatorEpoch);
    }

    /**
     * Deregisters this Worker from the Coordinator (delegated to WorkerShutdownSequence).
     */
    public static void deregister(WorkerComponentRefs refs, String workerId) {
        WorkerShutdownSequence.deregister(refs.registrationClient(), workerId);
    }

    /**
     * Stops the heartbeat sender and executor (delegated to WorkerShutdownSequence).
     */
    public static void stopHeartbeat(WorkerComponentRefs refs) {
        WorkerShutdownSequence.stopHeartbeat(refs.heartbeatSender(), refs.heartbeatExecutor());
    }

    static HeartbeatRequest collectHeartbeatRequest(WorkerBootstrapConfig config,
                                                     WorkerComponentRefs refs) {
        int inFlightTasks = refs.taskManager().getInFlightTaskCount();
        long availableMemory = refs.resourceManager() != null
                ? refs.resourceManager().getMemoryArbitrator().getAvailableMemory() : 0;
        if (refs.nodeMemoryPool() != null) {
            // Native allocations and serialized buffers are charged to both
            // ledgers. Advertise the tighter budget, especially when the
            // configured node cap is below the global arbitrator budget.
            availableMemory = Math.min(availableMemory, refs.nodeMemoryPool().getFreeBytes());
        }
        String pressure = refs.resourceManager() != null
                ? refs.resourceManager().getOverallPressure().name() : "NONE";

        List<String> cachedFilePaths = refs.recentFileAccessTracker() == null
                ? List.of() : refs.recentFileAccessTracker().heartbeatSample();
        List<TenantUsageSnapshot> usage = refs.tenantResourceTracker() == null
                ? List.of() : List.copyOf(refs.tenantResourceTracker().snapshotUsage().values());
        return new HeartbeatRequest(config.workerId(), pressure, 0.0, inFlightTasks, availableMemory,
                cachedFilePaths, usage, refs.taskManager().workerExecutionIdentity());
    }

    private static long attemptRegistration(WorkerRegistrationClient client,
                                            org.pangrova.protocol.rest.worker.WorkerRegistrationRequest request,
                                            WorkerBootstrapConfig config) {
        WorkerBootstrapConfig.RegistrationRetryConfig retry = config.registrationRetryConfig();
        BackoffStrategy backoff = BackoffStrategy.retry(retry.initialBackoffMs(), retry.maxBackoffMs());
        for (int attempt = 1; attempt <= retry.maxAttempts(); attempt++) {
            try {
                return client.register(request).requireCoordinatorEpoch();
            } catch (Exception e) {
                handleRegistrationFailure(e, attempt, retry, backoff, config);
            }
        }
        throw new IllegalStateException("Registration retry loop exited unexpectedly");
    }

    private static void handleRegistrationFailure(Exception e, int attempt,
                                                   WorkerBootstrapConfig.RegistrationRetryConfig retry,
                                                   BackoffStrategy backoff,
                                                   WorkerBootstrapConfig config) {
        if (attempt == retry.maxAttempts()) {
            LOG.error("event=registration_exhausted workerId={} attempts={} maxAttempts={} errorType={} error={}",
                    config.workerId(), attempt, retry.maxAttempts(),
                    e.getClass().getSimpleName(), e.getMessage());
            throw new BootstrapFailedException("registration", e);
        }
        long backoffMs = backoff.next();
        LOG.warn("event=registration_retry workerId={} attempt={} maxAttempts={} backoffMs={} errorType={} error={}",
                config.workerId(), attempt, retry.maxAttempts(), backoffMs,
                e.getClass().getSimpleName(), e.getMessage());
        sleepOrThrow(backoffMs);
    }

    private static void sleepOrThrow(long backoffMs) {
        ThreadUtils.sleepQuietly(backoffMs);
        if (Thread.currentThread().isInterrupted()) {
            throw new BootstrapFailedException("registration",
                    new RuntimeException("Interrupted during registration backoff"));
        }
    }
}
