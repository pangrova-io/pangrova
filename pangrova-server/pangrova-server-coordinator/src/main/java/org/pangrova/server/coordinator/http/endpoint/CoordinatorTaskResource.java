package org.pangrova.server.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusReportResponse;

/**
 * JAX-RS resource for task status reporting from Workers.
 * Delegates to {@link TaskStatusReceiver}.
 */
@Path("/v1/task")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class CoordinatorTaskResource {

    private final TaskStatusReceiver receiver;

    public CoordinatorTaskResource(TaskStatusReceiver receiver) {
        this.receiver = checkNotNull(receiver, "receiver must not be null");
    }

    @POST
    @Path("/status-report")
    public Response taskStatusReport(TaskStatusReport report) {
        boolean accepted = receiver.onTaskStatus(report);
        String directive = accepted ? "CONTINUE" : "STALE_REJECTED";
        if (!accepted && receiver.supersedesResultNotice(report)) {
            directive = org.pangrova.protocol.task.TerminalReportDeliveryPolicy.RETIRED_RESULT_NOTICE;
        }
        TaskStatusReportResponse response = new TaskStatusReportResponse(
                report.taskId(), directive);
        return Response.ok(response).build();
    }
}
