package org.pangrova.worker.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.observability.logging.LogContext;
import org.pangrova.observability.tracing.NativeTraceExporter;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.ResultProtocol;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.protocol.rest.task.UpstreamCommitNotification;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * JAX-RS resource for Worker task operations.
 * Exchange inputs are immutable task-admission bindings. Runtime filter
 * updates remain control messages, while row-bearing exchange data stays on
 * the Native Flight data plane.
 */
@Path("/v1/task")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class TaskResource implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TaskResource.class);

    private final TaskManager taskManager;
    private final ExchangeTransport exchangeTransport;
    private final ExchangeSourceRegistry exchangeSourceRegistry;
    private final String workerHost;
    private final int transportPort;
    private final String resultProtocol;
    private final org.pangrova.worker.result.ResultReadTickets resultTickets;
    private final org.pangrova.spi.observability.TracingProvider tracingProvider;

    public record ResultBinding(String protocol, org.pangrova.worker.result.ResultReadTickets tickets) {}

    public TaskResource(TaskManager taskManager, ExchangeTransport exchangeTransport,
                        String workerHost, int transportPort) {
        this(taskManager, exchangeTransport, new ExchangeSourceRegistry(), workerHost, transportPort, null);
    }

    public TaskResource(TaskManager taskManager, ExchangeTransport exchangeTransport,
                        ExchangeSourceRegistry exchangeSourceRegistry,
                        String workerHost, int transportPort,
                        org.pangrova.spi.observability.TracingProvider tracingProvider) {
        this(taskManager, exchangeTransport, exchangeSourceRegistry,
                workerHost, transportPort, tracingProvider, ResultProtocol.GRPC.value());
    }

    /** Creates the resource with all collaborator dependencies. */
    public TaskResource(TaskManager taskManager, ExchangeTransport exchangeTransport,
                        ExchangeSourceRegistry exchangeSourceRegistry,
                        String workerHost, int transportPort,
                        org.pangrova.spi.observability.TracingProvider tracingProvider,
                        String resultProtocol) {
        this(taskManager, exchangeTransport, exchangeSourceRegistry, workerHost, transportPort,
                tracingProvider, new ResultBinding(resultProtocol, null));
    }

    /** Creates the result-producing resource with explicit process-local signing authority. */
    public TaskResource(TaskManager taskManager, ExchangeTransport exchangeTransport,
                        ExchangeSourceRegistry exchangeSourceRegistry,
                        String workerHost, int transportPort,
                        org.pangrova.spi.observability.TracingProvider tracingProvider,
                        ResultBinding resultBinding) {
        this.taskManager = checkNotNull(taskManager, "taskManager must not be null");
        this.exchangeTransport = checkNotNull(exchangeTransport, "exchangeTransport must not be null");
        this.exchangeSourceRegistry = checkNotNull(exchangeSourceRegistry,
                "exchangeSourceRegistry must not be null");
        this.workerHost = checkNotNull(workerHost, "workerHost must not be null");
        this.transportPort = transportPort;
        this.resultProtocol = ResultProtocol.fromValue(resultBinding.protocol()).value();
        this.resultTickets = resultBinding.tickets();
        this.tracingProvider = tracingProvider;
    }

    /** Submits a new task for execution. */
    @POST
    public Response submitTask(SubmitTaskRequest request) {
        if (request == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "BAD_REQUEST", "USER_ERROR",
                            "Request body must not be null"))
                    .build();
        }
        LogContext.set(NativeTraceExporter.currentTraceId(), NativeTraceExporter.currentSpanId());
        org.pangrova.spi.observability.Span span = startSpan("task.submit");
        try {
            return executeSubmit(request);
        } catch (Exception e) {
            return handleSubmitError(request, e);
        } finally {
            endSpan(span);
            LogContext.clear();
        }
    }

    private Response executeSubmit(SubmitTaskRequest request) {
        // Classify a missing or stale control epoch through the fail-closed
        // fencing path before full contract validation. This check is pure and
        // must precede every admission side effect.
        taskManager.verifyControlEpochForAdmission(
                request.taskRequest().getAttempt().getControlEpoch());
        taskManager.verifyAdmissionDeadline(request.taskRequest().getDeadlineEpochMs());
        // Validate the one canonical contract before any admission side effect.
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(
                request.taskRequest());
        taskManager.verifyExecutionOwnerForAdmission(request);
        ResultEndpoint endpoint = resultEndpoint(request.taskRequest());
        TaskDescriptor descriptor = TaskRequestDtoConverter.toTaskDescriptor(request);
        String workerTaskId = taskManager.submitTask(descriptor, request);
        var shuffleEndpoint = taskManager.getShuffleServerHandle(workerTaskId)
                .map(handle -> new org.pangrova.protocol.rest.task.exchange
                        .ShuffleEndpointDescriptor(
                        handle.host(), handle.port(), handle.tlsIdentity(),
                        handle.partitionTickets().stream()
                                .map(ticket -> new org.pangrova.protocol.rest.task.exchange
                                        .ShufflePartitionTicketDescriptor(
                                        ticket.partitionId(),
                                        java.util.Base64.getEncoder().encodeToString(
                                                ticket.signedTicket()),
                                        ticket.expiresAtEpochMs()))
                                .toList()))
                .orElse(null);
        SubmitTaskResponse response = new SubmitTaskResponse(
                true, null, request.taskRequest().getAttempt().getTaskId(),
                endpoint, shuffleEndpoint);
        return Response.ok(response).build();
    }

    private ResultEndpoint resultEndpoint(org.pangrova.protocol.v2.TaskRequest request) {
        if (!request.getExecutionOptions().getDirectResultOutput()) return null;
        if (resultTickets == null) {
            throw new IllegalStateException("Result-producing admission requires signing authority");
        }
        return resultTickets.endpoint(request, workerHost, transportPort, resultProtocol);
    }

    private Response handleSubmitError(SubmitTaskRequest request, Exception e) {
        if (e instanceof org.pangrova.worker.task.TaskAdmissionException admission) {
            int status = switch (admission.reason()) {
                case EXPIRED -> Response.Status.REQUEST_TIMEOUT.getStatusCode();
                case IDENTITY_CONFLICT, SEALED -> Response.Status.CONFLICT.getStatusCode();
                case CAPACITY, CLOSED -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
            };
            return Response.status(status).entity(new SubmitTaskResponse(
                    false, admission.getMessage(), null, null, null)).build();
        }
        if (e instanceof org.pangrova.worker.task.ControlEpochFenceException fenceError) {
            int status = epochRejectionStatus(fenceError);
            LOG.warn("event=task_submit_epoch_rejected queryId={} reason={} error={}",
                    queryId(request), fenceError.reason(), fenceError.getMessage());
            return Response.status(status)
                    .entity(new SubmitTaskResponse(
                            false, fenceError.getMessage(), null, null, null))
                    .build();
        }
        if (e instanceof org.pangrova.worker.task.ExecutionOwnerFenceException) {
            LOG.warn("event=task_submit_owner_rejected queryId={} error={}",
                    queryId(request), e.getMessage());
            return Response.status(Response.Status.CONFLICT)
                    .entity(new SubmitTaskResponse(
                            false, e.getMessage(), null, null, null))
                    .build();
        }
        if (e instanceof org.pangrova.common.exception.PangrovaException resourceError
                && resourceError.getErrorCode()
                        == org.pangrova.common.exception.PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED) {
            LOG.warn("event=task_submit_resource_rejected queryId={} error={}",
                    queryId(request), e.getMessage());
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new SubmitTaskResponse(false, e.getMessage(), null, null, null))
                    .build();
        }
        LOG.error("event=task_submit_failed queryId={} error={}",
                queryId(request), e.getMessage(), e);
        SubmitTaskResponse errorResponse = new SubmitTaskResponse(
                false, e.getMessage(), null, null, null);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(errorResponse)
                .build();
    }

    private static String queryId(SubmitTaskRequest request) {
        return request.taskRequest().hasAttempt()
                ? request.taskRequest().getAttempt().getQueryOrJobId()
                : "unknown";
    }

    /** Seals the exact issued admission; only a real canonical drain receipt qualifies as success. */
    @DELETE
    @Path("/{taskId}")
    @Consumes("application/x-protobuf")
    @Produces("application/x-protobuf")
    public Response cancelTask(@PathParam("taskId") String taskId, byte[] canonicalSeal) {
        try {
            if (canonicalSeal == null || canonicalSeal.length == 0) {
                throw new IllegalArgumentException("Exact canonical admission seal is required");
            }
            var request = org.pangrova.protocol.v2.TaskAdmissionSealRequest.parseFrom(canonicalSeal);
            org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.validate(request, taskId);
            var receipt = taskManager.sealAdmission(request);
            return receipt.isPresent()
                    ? Response.ok(receipt.orElseThrow().toByteArray(), "application/x-protobuf").build()
                    : Response.status(Response.Status.ACCEPTED).build();
        } catch (com.google.protobuf.InvalidProtocolBufferException | IllegalArgumentException failure) {
            return sealError(Response.Status.BAD_REQUEST, failure);
        } catch (org.pangrova.worker.task.TaskAdmissionException failure) {
            return sealError(failure.reason() == org.pangrova.worker.task.TaskAdmissionException.Reason.CAPACITY
                    ? Response.Status.SERVICE_UNAVAILABLE : Response.Status.CONFLICT, failure);
        } catch (org.pangrova.worker.task.ControlEpochFenceException failure) {
            return sealError(epochRejectionStatus(failure), failure.reason().name(), failure);
        } catch (org.pangrova.worker.task.ExecutionOwnerFenceException failure) {
            return sealError(Response.Status.CONFLICT, failure);
        } catch (RuntimeException failure) {
            LOG.error("event=task_admission_seal_failed taskId={}", taskId, failure);
            return sealError(Response.Status.INTERNAL_SERVER_ERROR, failure);
        }
    }

    private static Response sealError(Response.Status status, Exception failure) {
        return sealError(status.getStatusCode(), status.name(), failure);
    }

    private static Response sealError(int status, String code, Exception failure) {
        return Response.status(status).type(MediaType.APPLICATION_JSON)
                .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                        code, "SYSTEM_ERROR", failure.getMessage())).build();
    }

    private static int epochRejectionStatus(org.pangrova.worker.task.ControlEpochFenceException failure) {
        return switch (failure.reason()) {
            case CONTROL_PLANE_NOT_READY -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
            // No admission/seal side effect: wait for an authenticated heartbeat to observe this epoch.
            case EPOCH_AHEAD -> 425;
            default -> Response.Status.CONFLICT.getStatusCode();
        };
    }

    /** Handles an upstream commit notification for a running task. */
    @POST
    @Path("/upstream-commit")
    public Response onUpstreamCommit(UpstreamCommitNotification notification) {
        if (notification == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "BAD_REQUEST", "USER_ERROR",
                            "Request body must not be null"))
                    .build();
        }
        exchangeSourceRegistry.notifyCommit(notification.queryId(), notification.upstreamStageId());
        LOG.info("event=upstream_commit_received queryId={} upstreamStageId={}",
                notification.queryId(), notification.upstreamStageId());
        return Response.ok().build();
    }

    /** Injects a dynamic filter into a running task. */
    @POST
    @Path("/{taskId}/dynamic-filter")
    public Response injectDynamicFilter(@PathParam("taskId") String taskId,
                                        org.pangrova.protocol.rest.task.InjectDynamicFilterRequest request) {
        if (request == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "BAD_REQUEST", "USER_ERROR",
                            "Request body must not be null"))
                    .build();
        }
        String workerTaskId = taskManager.resolveWorkerTaskId(taskId);
        OptionalLong handle = workerTaskId == null
                ? OptionalLong.empty() : taskManager.getExecutorHandle(workerTaskId);
        if (handle.isEmpty()) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "CONFLICT", "USER_ERROR",
                            "Executor not ready for task: " + taskId))
                    .build();
        }
        byte[] commandProto = org.pangrova.worker.jni.NativeCommandBuilder.injectRuntimeFilter(
                request.planNodeId(), request.filterProto());
        Optional<org.pangrova.spi.execution.TaskExecutor> executor = taskManager.getSharedNativeExecutor();
        if (executor.isEmpty()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "NATIVE_EXECUTOR_UNAVAILABLE", "SYSTEM_ERROR",
                            "Native executor unavailable for task: " + taskId))
                    .build();
        }
        byte[] response = executor.get().executeCommand(handle.getAsLong(), commandProto);
        org.pangrova.worker.jni.NativeCommandBuilder.requireSuccess(
                response, "INJECT_DYNAMIC_FILTER task=" + taskId);
        LOG.info("event=dynamic_filter_injected taskId={} planNodeId={}", taskId, request.planNodeId());
        return Response.ok().build();
    }

    private org.pangrova.spi.observability.Span startSpan(String operationName) {
        if (tracingProvider == null) {
            return null;
        }
        return tracingProvider.startSpan(operationName, org.pangrova.spi.observability.SpanKind.SERVER);
    }

    private static void endSpan(org.pangrova.spi.observability.Span span) {
        if (span != null) {
            span.end();
        }
    }

    @Override
    public void close() {
        // No resources to close — transport SPIs are managed by bootstrap
    }
}
