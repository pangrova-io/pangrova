package org.pangrova.server.coordinator.http.endpoint;

import com.google.protobuf.InvalidProtocolBufferException;
import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.protocol.http.TaskStatusAck;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusWire;

/**
 * JAX-RS resource for task status reporting from Workers.
 * Delegates to {@link TaskStatusReceiver}.
 */
@Path("/v1/task")
@Produces(TaskStatusWire.MEDIA_TYPE)
@Consumes(TaskStatusWire.MEDIA_TYPE)
public final class CoordinatorTaskResource {

    private final TaskStatusReceiver receiver;

    public CoordinatorTaskResource(TaskStatusReceiver receiver) {
        this.receiver = checkNotNull(receiver, "receiver must not be null");
    }

    /** Accepts a Worker task status report and returns whether the Worker should continue. */
    @POST
    @Path("/status-report")
    public Response taskStatusReport(byte[] body) {
        TaskStatusReport report;
        try {
            report = TaskStatusWire.decode(body);
        } catch (InvalidProtocolBufferException | IllegalArgumentException invalid) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        boolean accepted = receiver.onTaskStatus(report);
        var directive = accepted ? TaskStatusAck.Directive.CONTINUE : TaskStatusAck.Directive.STALE_REJECTED;
        if (!accepted && receiver.supersedesResultNotice(report)) {
            directive = TaskStatusAck.Directive.RETIRED_RESULT_NOTICE;
        }
        byte[] ack = TaskStatusAck.newBuilder().setTaskId(report.taskId()).setSequence(report.reportSequence())
                .setDirective(directive).build().toByteArray();
        return Response.ok(ack, TaskStatusWire.MEDIA_TYPE).build();
    }
}
