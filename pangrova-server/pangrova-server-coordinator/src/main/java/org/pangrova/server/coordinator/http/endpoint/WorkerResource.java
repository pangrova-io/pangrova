package org.pangrova.server.coordinator.http.endpoint;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.container.ContainerRequestContext;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordination.api.membership.WorkerShuffleDeployment;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerManager.HeartbeatData;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.protocol.rest.worker.HeartbeatRequest;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.enums.MemoryPressure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * JAX-RS resource for worker heartbeat and registration.
 * Delegates to {@link WorkerManager}.
 *
 * <p>Worker registration accepts only the exact canonical v2 protocol identity.
 * Incompatible workers are rejected with HTTP 409.</p>
 */
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class WorkerResource {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerResource.class);

    private static final int HTTP_STATUS_BAD_REQUEST = 400;
    private static final int HTTP_STATUS_CONFLICT = 409;
    /** Maximum allowed length of a worker hostname (RFC 1035 limit). */
    private static final int MAX_WORKER_HOST_LENGTH = 253;

    static final int CANONICAL_PROTOCOL_VERSION = CanonicalProtocolIdentity.PROTOCOL_MAJOR;

    private final WorkerManager workerManager;
    private final LongSupplier coordinatorEpochSupplier;

    public WorkerResource(WorkerManager workerManager) {
        this(workerManager, () -> 1L);
    }

    /** Creates the authenticated registration resource with the current Coordinator epoch. */
    public WorkerResource(WorkerManager workerManager, LongSupplier coordinatorEpochSupplier) {
        this.workerManager = checkNotNull(workerManager, "workerManager must not be null");
        this.coordinatorEpochSupplier = checkNotNull(
                coordinatorEpochSupplier, "coordinatorEpochSupplier must not be null");
    }

    /** Processes a worker heartbeat, recovering unregistered workers from the registry when possible. */
    @POST
    @Path("/worker/heartbeat")
    public Response heartbeat(HeartbeatRequest request, @Context ContainerRequestContext context) {
        AuthenticationFilter.requireInternalServiceIdentity(context);
        var identity = request.workerExecutionIdentity();
        var result = workerManager.onHeartbeat(
                new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity(
                        request.workerId(), identity.admissionDomainId(), identity.incarnationId()),
                toHeartbeatData(request));
        if (result != org.pangrova.coordination.api.enums.WorkerHeartbeatStatus.ACCEPTED) {
            String action = result == org.pangrova.coordination.api.enums.WorkerHeartbeatStatus.FENCED
                    ? "fenced" : "re-register";
            return Response.status(HTTP_STATUS_CONFLICT)
                    .entity(new org.pangrova.protocol.rest.worker.WorkerControlResponse(
                            "error", action,
                                    "Worker registration is not owned by this process: " + request.workerId(),
                                            null)).build();
        }
        return Response.ok(controlResponse("ok")).build();
    }

    /**
     * Registers a worker. WorkerId is taken from the request body.
     * This is the canonical registration endpoint matching
     * {@code WorkerRegistrationClient}.
     */
    @POST
    @Path("/workers/register")
    public Response register(WorkerRegistrationRequest request, @Context ContainerRequestContext context) {
        AuthenticationFilter.requireInternalServiceIdentity(context);
        return doRegister(request, false);
    }

    /** Conditional repair of a previously attempted registration. */
    @POST
    @Path("/workers/refresh")
    public Response refresh(WorkerRegistrationRequest request, @Context ContainerRequestContext context) {
        AuthenticationFilter.requireInternalServiceIdentity(context);
        return doRegister(request, true);
    }

    /**
     * Withdraws the exact process identified by the DELETE JSON body; logical ID reuse is safe.
     * Matches {@code DELETE /v1/workers/{workerId}} sent by
     * {@code WorkerRegistrationClient.deregister}.
     */
    @DELETE
    @Path("/workers/{workerId}")
    public Response deregisterWorker(@PathParam("workerId") String workerId,
                                     org.pangrova.protocol.execution.identity.WorkerExecutionIdentity identity,
                                     @Context ContainerRequestContext context) {
        AuthenticationFilter.requireInternalServiceIdentity(context);
        if (identity == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("message", "Worker withdrawal requires execution identity")).build();
        }
        workerManager.removeWorker(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity(
                workerId, identity.admissionDomainId(), identity.incarnationId()));
        LOG.info("event=worker_deregistered workerId={}", workerId);
        return Response.ok(Map.of("status", "deregistered")).build();
    }

    private Response doRegister(WorkerRegistrationRequest request, boolean refresh) {
        try {
            request.requireBoundedMetadata(JsonMapperFactory.shared());
        } catch (IllegalArgumentException failure) {
            LOG.warn("event=worker_registration_rejected workerId={} reason=oversized_metadata",
                    request.workerId());
            return Response.status(HTTP_STATUS_BAD_REQUEST)
                    .entity(Map.of("status", "error", "message", failure.getMessage()))
                    .build();
        }
        if (!isCanonicalProtocolVersion(request.protocolVersion())) {
            return rejectIncompatibleProtocol(request);
        }
        final org.pangrova.protocol.v2.CapabilityInventory capabilityInventory;
        try {
            capabilityInventory = CapabilityInventoryCodec.decode(
                    request.capabilityInventory());
        } catch (IllegalArgumentException failure) {
            LOG.warn("event=worker_registration_rejected workerId={} reason=invalid_capability_inventory error={}",
                    request.workerId(), failure.getMessage());
            return Response.status(HTTP_STATUS_BAD_REQUEST)
                    .entity(Map.of(
                            "status", "error",
                            "message", "Invalid canonical capability inventory: "
                                    + failure.getMessage()))
                    .build();
        }
        String host = request.httpHost();
        if (!isValidWorkerHost(host)) {
            LOG.warn("event=worker_registration_rejected workerId={} host={} reason=invalid_host",
                    request.workerId(), host);
            return Response.status(HTTP_STATUS_BAD_REQUEST)
                    .entity(Map.of("status", "error", "message", "Invalid worker host: " + host))
                    .build();
        }
        var deployment = reportedShuffle(request);
        if (!workerManager.supportsShuffle(deployment) || (request.shuffleMode() != null && deployment == null)) {
            return Response.status(HTTP_STATUS_CONFLICT)
                    .entity(Map.of("status", "error", "message",
                            "Worker shuffle deployment does not match Coordinator"))
                    .build();
        }
        WorkerInfo info = toWorkerInfo(request, capabilityInventory, deployment);
        if (refresh) {
            if (!workerManager.refreshWorker(info)) {
                return Response.status(HTTP_STATUS_CONFLICT)
                        .entity(new org.pangrova.protocol.rest.worker.WorkerControlResponse(
                                "error", "fenced", "Another process owns the Worker registration", null)).build();
            }
        } else {
            workerManager.registerWorker(info);
        }
        LOG.info("event=worker_registered workerId={} host={}:{} transportPort={} resultServerPort={} "
                        + "protocolVersion={} capabilityCount={} modelCount={}",
                request.workerId(), request.httpHost(),
                request.httpPort(), request.transportPort(),
                request.resultServerPort(),
                request.protocolVersion(), capabilityInventory.getCapabilitiesCount(),
                capabilityInventory.getModelsCount());
        return Response.ok(controlResponse("registered")).build();
    }

    private org.pangrova.protocol.rest.worker.WorkerControlResponse controlResponse(
            String status) {
        long epoch = coordinatorEpochSupplier.getAsLong();
        if (epoch <= 0) {
            throw new IllegalStateException(
                    "Coordinator HA epoch is not initialized: " + epoch);
        }
        return new org.pangrova.protocol.rest.worker.WorkerControlResponse(
                status, null, null, epoch);
    }

    private static Response rejectIncompatibleProtocol(WorkerRegistrationRequest request) {
        LOG.warn("event=worker_registration_rejected workerId={} protocolVersion={} "
                + "requiredVersion={} reason=incompatible_protocol_version",
                request.workerId(), request.protocolVersion(),
                CANONICAL_PROTOCOL_VERSION);
        return Response.status(HTTP_STATUS_CONFLICT)
                .entity(Map.of(
                        "status", "error",
                        "message", "Incompatible protocol version: " + request.protocolVersion()
                                + ". Required version: " + CANONICAL_PROTOCOL_VERSION,
                        "requiredProtocolVersion", CANONICAL_PROTOCOL_VERSION))
                .build();
    }

    /** Returns true only for the exact canonical Worker protocol major. */
    static boolean isCanonicalProtocolVersion(int protocolVersion) {
        return protocolVersion == CANONICAL_PROTOCOL_VERSION;
    }

    private static HeartbeatData toHeartbeatData(HeartbeatRequest request) {
        MemoryPressure pressure = parseMemoryPressure(request.memoryPressure());
        // F-CHAIN-CACHE-02: lift cached file paths from heartbeat into a
        // CacheCatalogSummary, which the WorkerManager forwards into the
        // worker registry. Replaces dead bloom-filter approach.
        boolean hasCacheReport = (request.cachedFilePaths() != null
                && !request.cachedFilePaths().isEmpty())
                || request.cacheMaxBytes() > 0
                || request.cacheHitCount() > 0
                || request.cacheMissCount() > 0;
        org.pangrova.coordination.api.heartbeat.CacheCatalogSummary cacheSummary =
                hasCacheReport
                        ? new org.pangrova.coordination.api.heartbeat.CacheCatalogSummary(
                                request.cachedFilePaths(),
                                request.cacheUsedBytes(),
                                request.cacheMaxBytes(),
                                request.cacheHitCount(),
                                request.cacheMissCount())
                        : null;
        return new HeartbeatData(
                pressure,
                request.cpuUtilization(),
                request.runningTaskCount(),
                request.availableMemoryBytes(),
                cacheSummary,
                buildTenantUsage(request));
    }

    /**
     * P1-5: reconstructs per-tenant usage snapshots from the worker heartbeat so
     * the coordinator can aggregate a cluster-wide view of tenant consumption.
     */
    private static java.util.Map<String, org.pangrova.protocol.rest.worker.TenantUsageSnapshot>
            buildTenantUsage(HeartbeatRequest request) {
        java.util.Map<String, Long> mem = request.tenantMemoryBytes();
        java.util.Map<String, Integer> queries = request.tenantActiveQueries();
        if (isEmptyOrNull(mem) && isEmptyOrNull(queries)) {
            return java.util.Map.of();
        }
        java.util.Set<String> tenantIds = collectTenantIds(mem, queries);
        java.util.Map<String, org.pangrova.protocol.rest.worker.TenantUsageSnapshot> usage =
                new java.util.HashMap<>(tenantIds.size());
        for (String tenantId : tenantIds) {
            long m = memoryBytesFor(mem, tenantId);
            int q = activeQueriesFor(queries, tenantId);
            usage.put(tenantId, new org.pangrova.protocol.rest.worker.TenantUsageSnapshot(tenantId, m, q));
        }
        return usage;
    }

    private static boolean isEmptyOrNull(java.util.Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

    private static java.util.Set<String> collectTenantIds(
            java.util.Map<String, Long> mem, java.util.Map<String, Integer> queries) {
        java.util.Set<String> tenantIds = new java.util.HashSet<>();
        if (mem != null) {
            tenantIds.addAll(mem.keySet());
        }
        if (queries != null) {
            tenantIds.addAll(queries.keySet());
        }
        return tenantIds;
    }

    private static long memoryBytesFor(java.util.Map<String, Long> mem, String tenantId) {
        return mem != null ? mem.getOrDefault(tenantId, 0L) : 0L;
    }

    private static int activeQueriesFor(java.util.Map<String, Integer> queries, String tenantId) {
        return queries != null ? queries.getOrDefault(tenantId, 0) : 0;
    }

    private static MemoryPressure parseMemoryPressure(String value) {
        try {
            return MemoryPressure.valueOf(value);
        } catch (IllegalArgumentException e) {
            LOG.debug("Unknown memoryPressure '{}', defaulting to NONE", value);
            return MemoryPressure.NONE;
        }
    }

    private static WorkerShuffleDeployment reportedShuffle(WorkerRegistrationRequest request) {
        if (request.shuffleMode() == null || ("REMOTE".equals(request.shuffleMode())
                && request.shuffleProvider() == null)) {
            return null;
        }
        return new WorkerShuffleDeployment(request.shuffleMode(), request.shuffleProvider());
    }

    private static WorkerInfo toWorkerInfo(
            WorkerRegistrationRequest req, org.pangrova.protocol.v2.CapabilityInventory capabilityInventory,
            WorkerShuffleDeployment deployment) {
        return new WorkerInfo(
                req.workerId(), req.httpHost(), req.httpPort(), req.transportPort(),
                req.resultServerPort(),
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, 0L, Instant.now(), 0.0, null,
                req.protocolVersion(),
                req.rackId(), req.dataLocality(), null, capabilityInventory,
                req.copyTaskTlsPort(), req.workerExecutionIdentity(), deployment);
    }

    private static final java.util.regex.Pattern VALID_HOST_PATTERN =
            java.util.regex.Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9.\\-]*$");

    private static boolean isValidWorkerHost(String host) {
        if (host == null || host.isEmpty() || host.length() > MAX_WORKER_HOST_LENGTH) {
            return false;
        }
        if (!VALID_HOST_PATTERN.matcher(host).matches()) {
            return false;
        }
        try {
            java.net.InetAddress addr = java.net.InetAddress.getByName(host);
            if (addr.isLoopbackAddress() || addr.isLinkLocalAddress()) {
                return false;
            }
        } catch (java.net.UnknownHostException e) {
            // hostname that doesn't resolve yet is acceptable (DNS may lag behind registration)
        }
        return true;
    }
}
