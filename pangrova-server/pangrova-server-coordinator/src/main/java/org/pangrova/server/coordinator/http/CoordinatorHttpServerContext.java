package org.pangrova.server.coordinator.http;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.http.endpoint.copy.CopySourceReadResource;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.observability.core.profiling.ProfilerEndpoint;
import org.pangrova.query.core.profiling.QueryProfileService;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.pangrova.security.core.authentication.AuthenticationManager;
import org.pangrova.streaming.api.cdc.CdcLifecycleManager;

import java.util.function.LongSupplier;

/**
 * Groups CoordinatorHttpServer dependencies into a single parameter object.
 * Keeps the constructor within the ≤4 param governance limit.
 *
 * @param receiver              task status report receiver (required)
 * @param queryManager          query lifecycle manager (nullable — query endpoints skipped if null)
 * @param tracingHttpFilter     tracing filter for W3C context propagation (nullable)
 * @param authenticationManager authentication chain (nullable — auth filter skipped if null)
 * @param healthCheckEndpoint   health check endpoint (nullable — health endpoint skipped if null)
 * @param workerManager         worker registry for heartbeat processing (nullable)
 * @param profilerEndpoint      query profiling endpoint (nullable)
 * @param cdcHandler            CDC lifecycle manager SPI (nullable)
 * @param splitAssignmentManager pull-mode split assignment manager (nullable — split endpoint skipped if null)
 * @param coordinatorEpochSupplier current cluster control term published to Workers (never null)
 * @param copySourceReadResource optional COPY source-read endpoint
 * @param fileSpaceFilesResource optional Volume/Stage Files API ticket endpoint
 */
public record CoordinatorHttpServerContext(
        TaskStatusReceiver receiver,
        QueryManager queryManager,
        TracingHttpFilter tracingHttpFilter,
        AuthenticationManager authenticationManager,
        HealthCheckEndpoint healthCheckEndpoint,
        WorkerManager workerManager,
        ProfilerEndpoint profilerEndpoint,
        CdcLifecycleManager cdcHandler,
        SplitAssignmentManager splitAssignmentManager,
        QueryProfileService queryProfileService,
        MetadataService metadataService,
        String internalAuthSecret,
        LongSupplier coordinatorEpochSupplier,
        CopySourceReadResource copySourceReadResource,
        org.pangrova.coordinator.http.endpoint.file.FileSpaceFilesResource fileSpaceFilesResource) {

    public CoordinatorHttpServerContext {
        checkNotNull(receiver, "receiver must not be null");
        coordinatorEpochSupplier = coordinatorEpochSupplier == null
                ? () -> 1L
                : coordinatorEpochSupplier;
    }

    // CHECKSTYLE.OFF: ParameterNumber
    /** Existing HTTP context without a COPY source-read endpoint. */
    public CoordinatorHttpServerContext(
            TaskStatusReceiver receiver,
            QueryManager queryManager,
            TracingHttpFilter tracingHttpFilter,
            AuthenticationManager authenticationManager,
            HealthCheckEndpoint healthCheckEndpoint,
            WorkerManager workerManager,
            ProfilerEndpoint profilerEndpoint,
            CdcLifecycleManager cdcHandler,
            SplitAssignmentManager splitAssignmentManager,
            QueryProfileService queryProfileService,
            MetadataService metadataService,
            String internalAuthSecret,
            LongSupplier coordinatorEpochSupplier) {
        this(receiver, queryManager, tracingHttpFilter, authenticationManager,
                healthCheckEndpoint, workerManager, profilerEndpoint, cdcHandler,
                splitAssignmentManager, queryProfileService, metadataService,
                internalAuthSecret, coordinatorEpochSupplier, null, null);
    }
    // CHECKSTYLE.ON: ParameterNumber

    // CHECKSTYLE.OFF: ParameterNumber
    /** Backward-compatible constructor with internal auth and single-node epoch. */
    public CoordinatorHttpServerContext(
            TaskStatusReceiver receiver,
            QueryManager queryManager,
            TracingHttpFilter tracingHttpFilter,
            AuthenticationManager authenticationManager,
            HealthCheckEndpoint healthCheckEndpoint,
            WorkerManager workerManager,
            ProfilerEndpoint profilerEndpoint,
            CdcLifecycleManager cdcHandler,
            SplitAssignmentManager splitAssignmentManager,
            QueryProfileService queryProfileService,
            MetadataService metadataService,
            String internalAuthSecret) {
        this(receiver, queryManager, tracingHttpFilter, authenticationManager,
                healthCheckEndpoint, workerManager, profilerEndpoint, cdcHandler,
                splitAssignmentManager, queryProfileService, metadataService,
                internalAuthSecret, () -> 1L);
    }
    // CHECKSTYLE.ON: ParameterNumber

    // CHECKSTYLE.OFF: ParameterNumber
    // Backward-compatible signature: call sites (including tests) pass all dependencies positionally.
    /** Backward-compatible 11-arg constructor without internalAuthSecret. */
    public CoordinatorHttpServerContext(
            TaskStatusReceiver receiver,
            QueryManager queryManager,
            TracingHttpFilter tracingHttpFilter,
            AuthenticationManager authenticationManager,
            HealthCheckEndpoint healthCheckEndpoint,
            WorkerManager workerManager,
            ProfilerEndpoint profilerEndpoint,
            CdcLifecycleManager cdcHandler,
            SplitAssignmentManager splitAssignmentManager,
            QueryProfileService queryProfileService,
            MetadataService metadataService) {
        this(receiver, queryManager, tracingHttpFilter, authenticationManager,
                healthCheckEndpoint, workerManager, profilerEndpoint, cdcHandler,
                splitAssignmentManager, queryProfileService, metadataService, null, () -> 1L);
    }
    // CHECKSTYLE.ON: ParameterNumber

    // CHECKSTYLE.OFF: ParameterNumber
    // Backward-compatible signature: call sites (including tests) pass all dependencies positionally.
    /** Convenience constructor without splitAssignmentManager, queryProfileService, and metadataService. */
    public CoordinatorHttpServerContext(
            TaskStatusReceiver receiver,
            QueryManager queryManager,
            TracingHttpFilter tracingHttpFilter,
            AuthenticationManager authenticationManager,
            HealthCheckEndpoint healthCheckEndpoint,
            WorkerManager workerManager,
            ProfilerEndpoint profilerEndpoint,
            CdcLifecycleManager cdcHandler) {
        this(receiver, queryManager, tracingHttpFilter, authenticationManager,
                healthCheckEndpoint, workerManager, profilerEndpoint, cdcHandler,
                null, null, null, null, () -> 1L);
    }
    // CHECKSTYLE.ON: ParameterNumber

    /** Minimal dependencies — only receiver, all others null. */
    public static CoordinatorHttpServerContext minimal(TaskStatusReceiver receiver) {
        return new CoordinatorHttpServerContext(
                receiver, null, null, null, null, null, null, null,
                null, null, null, null, () -> 1L);
    }
}
