package org.pangrova.server.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SplitRequest;
import org.pangrova.protocol.rest.task.SplitResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * JAX-RS resource for pull-mode split assignment.
 * Workers call this endpoint to request the next split for their stage.
 *
 * <p><b>Status: not yet wired end-to-end.</b> This endpoint is guarded by
 * feature flag {@code pangrova.split.assignment.pull-mode.enabled} (default: false).
 * When pull-mode is disabled, all endpoints return 503 Service Unavailable
 * with a descriptive error message.</p>
 */
@Path("/v1/task")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class SplitResource {

    private static final Logger LOG = LoggerFactory.getLogger(SplitResource.class);

    private final SplitAssignmentManager splitAssignmentManager;

    public SplitResource(SplitAssignmentManager splitAssignmentManager) {
        this.splitAssignmentManager = checkNotNull(splitAssignmentManager,
                "splitAssignmentManager must not be null");
    }

    /** Assigns the next split to a pulling Worker task when pull mode is enabled. */
    @POST
    @Path("/request-split")
    public Response requestSplit(SplitRequest request) {
        if (!splitAssignmentManager.isPullModeEnabled()) {
            return pullModeDisabledResponse();
        }
        if (request == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorDetail("BAD_REQUEST", "USER_ERROR",
                            "Request body must not be null"))
                    .build();
        }

        Optional<SplitAssignment> split = splitAssignmentManager.requestSplit(
                request.queryId(), request.stageId(),
                request.workerId(), request.workerLocality());

        if (split.isEmpty()) {
            LOG.debug("event=no_more_splits queryId={} stageId={} workerId={}",
                    request.queryId(), request.stageId(), request.workerId());
            return Response.ok(SplitResponse.NO_MORE_SPLITS).build();
        }

        return buildSplitAssignedResponse(split.get(), request);
    }

    private Response buildSplitAssignedResponse(SplitAssignment assigned, SplitRequest request) {
        SplitResponse response = new SplitResponse(
                assigned.splitData(),
                assigned.splitIndex(),
                true,
                assigned.dataLocations());

        LOG.debug("event=split_assigned queryId={} stageId={} splitIndex={} workerId={}",
                request.queryId(), request.stageId(), assigned.splitIndex(), request.workerId());
        return Response.ok(response).build();
    }

    /** Records completion of a pulled split when pull mode is enabled. */
    @POST
    @Path("/complete-split")
    public Response completeSplit(SplitCompletionRequest request) {
        if (!splitAssignmentManager.isPullModeEnabled()) {
            return pullModeDisabledResponse();
        }
        if (request == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorDetail("BAD_REQUEST", "USER_ERROR",
                            "Request body must not be null"))
                    .build();
        }

        splitAssignmentManager.markSplitCompleted(
                request.queryId(), request.stageId(), request.splitIndex());

        LOG.debug("event=split_completed queryId={} stageId={} splitIndex={}",
                request.queryId(), request.stageId(), request.splitIndex());
        return Response.ok().build();
    }

    private static Response pullModeDisabledResponse() {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity(new ErrorDetail("SERVICE_UNAVAILABLE", "CONFIGURATION_ERROR",
                        "Pull-mode split assignment is disabled. "
                        + "Set pangrova.split.assignment.pull-mode.enabled=true to enable."))
                .build();
    }

    /**
     * Request body for split completion notification from Worker.
     */
    record SplitCompletionRequest(
            String queryId,
            int stageId,
            int splitIndex
    ) {}
}
