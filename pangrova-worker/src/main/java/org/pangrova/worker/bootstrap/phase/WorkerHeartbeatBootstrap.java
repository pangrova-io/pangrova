package org.pangrova.worker.bootstrap.phase;

import java.util.concurrent.ScheduledExecutorService;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.operations.bootstrap.BootstrapFailedException;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.lifecycle.WorkerRegistrationClient;
import org.pangrova.worker.bootstrap.lifecycle.WorkerShutdownSequence;
import org.pangrova.worker.heartbeat.HeartbeatConfig;
import org.pangrova.worker.heartbeat.HeartbeatPayload;
import org.pangrova.worker.heartbeat.HeartbeatSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Worker heartbeat lifecycle: periodic heartbeat sending,
 * Coordinator registration, and deregistration.
 */
public final class WorkerHeartbeatBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerHeartbeatBootstrap.class);

    /** Mask extracting the 7 value bits of a varint continuation byte. */
    private static final int VARINT_VALUE_MASK = 0x7F;

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
                hbConfig, () -> collectHeartbeatPayload(config, refs), executor,
                config.internalAuthSecret());
        sender.setCoordinatorEpochObserver(epoch -> {
            if (refs.taskManager() != null) {
                refs.taskManager().observeControlEpoch(epoch);
            }
        });
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
                        org.pangrova.protocol.CanonicalTaskRequests.PROTOCOL_MAJOR,
                        null, null, null,
                        refs.nativeCapabilityInventory());
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

    static HeartbeatPayload collectHeartbeatPayload(WorkerBootstrapConfig config,
                                                     WorkerComponentRefs refs) {
        int runningTasks = refs.taskManager() != null ? refs.taskManager().getRunningTaskCount() : 0;
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

        NativeMemorySnapshot nativeSnapshot = collectNativeMemorySnapshot();
        java.util.List<String> cachedFilePaths = refs.recentFileAccessTracker() != null
                ? refs.recentFileAccessTracker().heartbeatSample()
                : java.util.List.of();

        // F-WORKER-MEM-02: cache hit/miss counters and used/max bytes now live
        // in CacheMetricsPublisher (sourced from native AsyncDataCache via
        // GET_CACHE_STATS). The heartbeat carries only the cached-paths sample
        // for cache-locality scheduling — not duplicate observability counters.
        // P1-5: report this node's per-tenant usage so the coordinator can
        // aggregate a cluster-wide view and detect aggregate oversell.
        java.util.Map<String, Long> tenantMemoryBytes = new java.util.HashMap<>();
        java.util.Map<String, Integer> tenantActiveQueries = new java.util.HashMap<>();
        if (refs.tenantResourceTracker() != null) {
            for (org.pangrova.spi.resource.tenant.TenantUsageSnapshot usage
                    : refs.tenantResourceTracker().snapshotUsage().values()) {
                tenantMemoryBytes.put(usage.tenantId(), usage.memoryBytes());
                tenantActiveQueries.put(usage.tenantId(), usage.activeQueries());
            }
        }
        return new HeartbeatPayload(config.workerId(), pressure, 0.0,
                runningTasks, availableMemory, nativeSnapshot.usedBytes, nativeSnapshot.capacityBytes,
                cachedFilePaths, 0L, 0L, 0L, 0L, tenantMemoryBytes, tenantActiveQueries);
    }

    private static NativeMemorySnapshot collectNativeMemorySnapshot() {
        // The native memory snapshot is optional. Probe the loader without
        // triggering NativeRuntime class initialization — when the native
        // library is not on the classpath, accessing NativeRuntime would throw
        // an UnsatisfiedLinkError / NoClassDefFoundError and abort the whole
        // heartbeat payload (losing the Java-side cached-path sample).
        if (!org.pangrova.worker.jni.NativeLibraryLoader.isFunctional()) {
            return new NativeMemorySnapshot(0, 0);
        }
        try {
            byte[] snapshotBytes = org.pangrova.worker.jni.NativeRuntime.nativeGetMemorySnapshot();
            if (snapshotBytes != null && snapshotBytes.length > 0) {
                return new NativeMemorySnapshot(
                        parseVarintField(snapshotBytes, 1),
                        parseVarintField(snapshotBytes, 6));
            }
        } catch (Exception e) {
            LOG.debug("event=native_memory_snapshot_failed error={}", e.getMessage());
        }
        return new NativeMemorySnapshot(0, 0);
    }

    private record NativeMemorySnapshot(long usedBytes, long capacityBytes) {}

    /**
     * Lightweight proto varint field parser for MemorySnapshot.
     * Avoids pulling proto-java dependency into bootstrap path.
     * Supports int64 fields encoded as varint (wire type 0).
     */
    private static long parseVarintField(byte[] data, int fieldNumber) {
        int[] pos = {0};
        while (pos[0] < data.length) {
            int tag = readVarint32(data, pos);
            int wireType = tag & 0x07;
            int field = tag >>> 3;
            long value = consumeField(data, pos, wireType);
            if (wireType == 0 && field == fieldNumber) {
                return value;
            }
        }
        return 0;
    }

    /** Reads a varint-encoded int32 from data at pos, advancing pos. */
    private static int readVarint32(byte[] data, int[] pos) {
        int result = 0;
        int shift = 0;
        while (pos[0] < data.length && (data[pos[0]] & 0x80) != 0) {
            result |= (data[pos[0]] & VARINT_VALUE_MASK) << shift;
            shift += 7;
            pos[0]++;
        }
        if (pos[0] < data.length) {
            result |= (data[pos[0]] & VARINT_VALUE_MASK) << shift;
            pos[0]++;
        }
        return result;
    }

    /** Reads a varint-encoded int64 from data at pos, advancing pos. */
    private static long readVarint64(byte[] data, int[] pos) {
        long result = 0;
        int shift = 0;
        while (pos[0] < data.length && (data[pos[0]] & 0x80) != 0) {
            result |= (long) (data[pos[0]] & VARINT_VALUE_MASK) << shift;
            shift += 7;
            pos[0]++;
        }
        if (pos[0] < data.length) {
            result |= (long) (data[pos[0]] & VARINT_VALUE_MASK) << shift;
            pos[0]++;
        }
        return result;
    }

    /**
     * Consumes one proto field value based on wire type.
     * Returns the varint value for wire type 0, or 0 for skipped types.
     * Sets pos past the end of the data for unsupported wire types.
     */
    private static long consumeField(byte[] data, int[] pos, int wireType) {
        if (wireType == 0) {
            return readVarint64(data, pos);
        }
        if (wireType == 2) {
            int len = readVarint32(data, pos);
            pos[0] += len;
            return 0;
        }
        pos[0] = data.length; // unsupported wire type — stop parsing
        return 0;
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
