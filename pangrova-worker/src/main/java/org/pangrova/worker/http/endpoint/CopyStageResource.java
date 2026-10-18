package org.pangrova.worker.http.endpoint;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.worker.copy.CopyStageLeases;
import org.pangrova.worker.task.TaskManager;

import java.io.IOException;
import java.util.Objects;

/** Private pre-claim COPY stage; it cannot submit a Native task or write output. */
@Path("/v1/task/copy-stage")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public final class CopyStageResource {
    private final TaskManager tasks;
    private final CopyStageLeases leases;

    /** Binds the private endpoint to task admission and attempt-scoped stage leases. */
    public CopyStageResource(TaskManager tasks, CopyStageLeases leases) {
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.leases = Objects.requireNonNull(leases, "leases");
    }

    /** Stages the source only after private transport and canonical task admission checks. */
    @POST
    public Response stage(SubmitTaskRequest request, @Context HttpServletRequest transport)
            throws IOException {
        if (transport == null || !transport.isSecure()) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity(new ErrorDetail("COPY_SECURE_TRANSPORT_REQUIRED", "SECURITY_ERROR",
                            "COPY stage requires mutual-TLS transport"))
                    .build();
        }
        if (request == null || request.copySourceRead() == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorDetail("COPY_TICKET_REQUIRED", "USER_ERROR",
                            "COPY stage requires a bound private task ticket"))
                    .build();
        }
        var canonical = request.taskRequest();
        tasks.verifyControlEpochForAdmission(canonical.getAttempt().getControlEpoch());
        tasks.verifyAdmissionDeadline(canonical.getDeadlineEpochMs());
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(canonical);
        tasks.verifyExecutionOwnerForAdmission(request);
        return Response.ok(leases.stage(request)).build();
    }
}
