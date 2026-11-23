package org.pangrova.worker.http.endpoint;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.servlet.http.HttpServletRequest;
import org.pangrova.observability.logging.LogContext;
import org.pangrova.observability.sdk.tracing.otel.CurrentSpanIds;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.protocol.http.TaskAdmitted;
import org.pangrova.protocol.http.TaskRejected;
import org.pangrova.protocol.http.TaskResultEndpoint;
import org.pangrova.protocol.http.TaskShuffleEndpoint;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.protocol.rest.task.UpstreamCommitNotification;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * JAX-RS resource for Worker task operations. Exchange inputs are immutable task-admission
 * bindings. Runtime filter updates remain control messages, while row-bearing exchange data stays
 * on the Native Flight data plane.
 */
@Path("/v1/task")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class TaskResource implements Closeable {

    private static final int HTTP_TOO_EARLY = 425;
    private static final int MAX_PENDING_RETRY_ACKS = 128;
    private static final Logger LOG = LoggerFactory.getLogger(TaskResource.class);

    private final Semaphore pendingRetryAcks = new Semaphore(MAX_PENDING_RETRY_ACKS);

    private final TaskManager taskManager;
    private final ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport;
    private final ExchangeSourceRegistry exchangeSourceRegistry;
    private final String workerHost;
    private final int transportPort;
    private final String resultProtocol;
    private final org.pangrova.worker.result.ResultReadTickets resultTickets;
    private final org.pangrova.observability.api.tracing.TracingProvider tracingProvider;

    public record ResultBinding(
            String protocol, org.pangrova.worker.result.ResultReadTickets tickets) {}

    /** Creates task endpoints with the supplied actual manager and transport owners. */
    public TaskResource(
            TaskManager taskManager,
            ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport,
            String workerHost,
            int transportPort) {
        this(
                taskManager,
                exchangeTransport,
                new ExchangeSourceRegistry(),
                workerHost,
                transportPort,
                null);
    }

    /** Creates task endpoints with the supplied actual manager and transport owners. */
    public TaskResource(
            TaskManager taskManager,
            ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport,
            ExchangeSourceRegistry exchangeSourceRegistry,
            String workerHost,
            int transportPort,
            org.pangrova.observability.api.tracing.TracingProvider tracingProvider) {
        this(
                taskManager,
                exchangeTransport,
                exchangeSourceRegistry,
                workerHost,
                transportPort,
                tracingProvider,
                ResultProtocol.GRPC.value());
    }

    /** Creates task endpoints with the supplied actual manager and transport owners. */
    public TaskResource(
            TaskManager taskManager,
            ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport,
            ExchangeSourceRegistry exchangeSourceRegistry,
            String workerHost,
            int transportPort,
            org.pangrova.observability.api.tracing.TracingProvider tracingProvider,
            String resultProtocol) {
        this(
                taskManager,
                exchangeTransport,
                exchangeSourceRegistry,
                workerHost,
                transportPort,
                tracingProvider,
                new ResultBinding(resultProtocol, null));
    }

    /** Creates the result-producing resource with explicit process-local signing authority. */
    public TaskResource(
            TaskManager taskManager,
            ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport,
            ExchangeSourceRegistry exchangeSourceRegistry,
            String workerHost,
            int transportPort,
            org.pangrova.observability.api.tracing.TracingProvider tracingProvider,
            ResultBinding resultBinding) {
        this.taskManager = checkNotNull(taskManager, "taskManager must not be null");
        this.exchangeTransport =
                checkNotNull(exchangeTransport, "exchangeTransport must not be null");
        this.exchangeSourceRegistry =
                checkNotNull(exchangeSourceRegistry, "exchangeSourceRegistry must not be null");
        this.workerHost = checkNotNull(workerHost, "workerHost must not be null");
        this.transportPort = transportPort;
        this.resultProtocol = ResultProtocol.fromValue(resultBinding.protocol()).value();
        this.resultTickets = resultBinding.tickets();
        this.tracingProvider = tracingProvider;
    }

    /** Submits a new task for execution. */
    @POST
    @Consumes(SubmitTaskRequest.MEDIA_TYPE)
    @Produces(SubmitTaskRequest.MEDIA_TYPE)
    public CompletionStage<Response> submitTask(byte[] body, @Context HttpServletRequest transport) {
        if (body == null || body.length == 0) {
            return CompletableFuture.completedFuture(rejectSubmit(
                    Response.Status.BAD_REQUEST.getStatusCode(), "Task submission payload is required"));
        }
        SubmitTaskRequest request;
        try {
            request = SubmitTaskRequest.decode(body);
        } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
            return CompletableFuture.completedFuture(rejectSubmit(
                    Response.Status.BAD_REQUEST.getStatusCode(), "Invalid binary task submission"));
        }
        return submitTask(request, transport != null && transport.isSecure());
    }

    private CompletionStage<Response> submitTask(SubmitTaskRequest request, boolean secureTransport) {
        if (request.copySourceRead() != null && !secureTransport) {
            return CompletableFuture.completedFuture(rejectSubmit(
                    Response.Status.FORBIDDEN.getStatusCode(), "COPY_SECURE_TRANSPORT_REQUIRED",
                    "COPY private task requires mutual-TLS transport"));
        }
        LogContext.set(CurrentSpanIds.currentTraceId(), CurrentSpanIds.currentSpanId());
        org.pangrova.observability.api.tracing.Span span = startSpan("task.submit");
        try {
            return executeSubmit(request).whenComplete((result, failure) -> endSpan(span));
        } catch (Exception failure) {
            endSpan(span);
            return CompletableFuture.completedFuture(handleSubmitError(queryId(request), failure));
        } catch (Error failure) {
            endSpan(span);
            throw failure;
        } finally {
            LogContext.clear();
        }
    }

    private CompletionStage<Response> executeSubmit(SubmitTaskRequest request) {
        // Classify a missing or stale control epoch through the fail-closed
        // fencing path before full contract validation. This check is pure and
        // must precede every admission side effect.
        taskManager.verifyControlEpochForAdmission(
                request.taskRequest().getAttempt().getControlEpoch());
        taskManager.verifyAdmissionDeadline(request.taskRequest().getDeadlineEpochMs());
        // Validate the one canonical contract before any admission side effect.
        ValidatedTaskSubmission validated;
        try {
            validated = new ValidatedTaskSubmission(request);
        } catch (IllegalArgumentException invalidRequest) {
            return CompletableFuture.completedFuture(rejectSubmit(
                    Response.Status.BAD_REQUEST.getStatusCode(), invalidRequest.getMessage()));
        }
        taskManager.verifyExecutionOwnerForAdmission(request);
        ResultEndpoint endpoint = resultEndpoint(validated);
        TaskDescriptor descriptor = TaskRequestDtoConverter.toTaskDescriptor(request);
        var ownership = taskManager.submitValidatedTask(descriptor, validated);
        var attempt = request.taskRequest().getAttempt();
        String taskId = attempt.getTaskId();
        String query = attempt.getQueryOrJobId();
        boolean pendingRetry = !ownership.completed();
        if (pendingRetry && !pendingRetryAcks.tryAcquire()) {
            return CompletableFuture.completedFuture(rejectSubmit(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(),
                    "Worker has too many pending exact task acknowledgements"));
        }
        var response = ownership.completion().handle((workerTaskId, failure) ->
                submissionResponse(taskId, query, endpoint, workerTaskId, failure));
        if (!pendingRetry) return response;
        // An HTTP deadline must not settle the shared admission or release its observer capacity early.
        response.whenComplete((ignored, failure) -> pendingRetryAcks.release());
        long remainingMs = Math.max(1L, request.taskRequest().getDeadlineEpochMs() - System.currentTimeMillis());
        return response.toCompletableFuture().copy().completeOnTimeout(
                rejectSubmit(Response.Status.REQUEST_TIMEOUT.getStatusCode(), "Exact task acknowledgement expired"),
                remainingMs, TimeUnit.MILLISECONDS);
    }

    private Response submissionResponse(String taskId, String queryId, ResultEndpoint endpoint,
            String workerTaskId, Throwable failure) {
        if (failure != null) {
            var cause = failure instanceof CompletionException completion ? completion.getCause() : failure;
            if (cause instanceof Error error) throw error;
            return handleSubmitError(queryId, cause);
        }
        var admitted = TaskAdmitted.newBuilder().setTaskId(taskId);
        if (endpoint != null) {
            admitted.setResultEndpoint(TaskResultEndpoint.newBuilder().setHost(endpoint.host())
                    .setPort(endpoint.port()).setTicket(endpoint.ticket()).setProtocol(endpoint.protocol()));
        }
        taskManager.getShuffleServerHandle(workerTaskId).ifPresent(handle -> {
            var shuffle = TaskShuffleEndpoint.newBuilder().setHost(handle.host()).setPort(handle.port())
                    .setTlsIdentity(handle.tlsIdentity());
            for (var ticket : handle.partitionTickets()) {
                shuffle.addPartitionTickets(org.pangrova.protocol.v2.ShufflePartitionTicket.newBuilder()
                        .setPartitionId(ticket.partitionId())
                        .setSignedTicket(ByteString.copyFrom(ticket.signedTicket()))
                        .setExpiresAtEpochMs(ticket.expiresAtEpochMs()));
            }
            admitted.setShuffleEndpoint(shuffle);
        });
        return submitAck(Response.Status.OK.getStatusCode(), TaskSubmitAck.newBuilder().setAdmitted(admitted).build());
    }

    private ResultEndpoint resultEndpoint(ValidatedTaskSubmission request) {
        if (!request.submission().taskRequest().getExecutionOptions().getDirectResultOutput()) return null;
        if (resultTickets == null) {
            throw new IllegalStateException(
                    "Result-producing admission requires signing authority");
        }
        return resultTickets.endpoint(request, workerHost, transportPort, resultProtocol);
    }

    private Response handleSubmitError(String queryId, Throwable e) {
        if (e instanceof org.pangrova.worker.exception.TaskAdmissionException admission) {
            int status = switch (admission.reason()) {
                case EXPIRED -> Response.Status.REQUEST_TIMEOUT.getStatusCode();
                case IDENTITY_CONFLICT, SEALED -> Response.Status.CONFLICT.getStatusCode();
                case CAPACITY, CLOSED ->
                                Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
            };
            return rejectSubmit(status, admission.getMessage());
        }
        if (e instanceof org.pangrova.worker.exception.ControlEpochFenceException fenceError) {
            int status = epochRejectionStatus(fenceError);
            LOG.warn(
                    "event=task_submit_epoch_rejected queryId={} reason={} error={}",
                    queryId,
                    fenceError.reason(),
                    fenceError.getMessage());
            return rejectSubmit(status, fenceError.getMessage());
        }
        if (e instanceof org.pangrova.worker.exception.ExecutionOwnerFenceException) {
            LOG.warn(
                    "event=task_submit_owner_rejected queryId={} error={}",
                    queryId,
                    e.getMessage());
            return rejectSubmit(Response.Status.CONFLICT.getStatusCode(), e.getMessage());
        }
        if (e instanceof org.pangrova.common.exception.PangrovaException resourceError
                && resourceError.getErrorCode()
                        == org.pangrova.common.enums.PangrovaErrorCode
                                .RESOURCE_MEMORY_EXCEEDED) {
            LOG.warn(
                    "event=task_submit_resource_rejected queryId={} error={}",
                    queryId,
                    e.getMessage());
            return rejectSubmit(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), e.getMessage());
        }
        LOG.error(
                "event=task_submit_failed queryId={} error={}",
                queryId,
                e.getMessage(),
                e);
        return rejectSubmit(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage());
    }

    private static Response rejectSubmit(int status, String reason) {
        return rejectSubmit(status, status == HTTP_TOO_EARLY ? "CONTROL_EPOCH_AHEAD"
                : Response.Status.fromStatusCode(status).name(), reason);
    }

    private static Response rejectSubmit(int status, String code, String reason) {
        return submitAck(status, TaskSubmitAck.newBuilder().setRejected(TaskRejected.newBuilder()
                .setCode(code).setReason(reason == null ? "Task submission failed" : reason)).build());
    }

    private static Response submitAck(int status, TaskSubmitAck ack) {
        return Response.status(status).type(SubmitTaskRequest.MEDIA_TYPE).entity(ack.toByteArray()).build();
    }

    private static String queryId(SubmitTaskRequest request) {
        return request.taskRequest().hasAttempt()
                ? request.taskRequest().getAttempt().getQueryOrJobId()
                : "unknown";
    }

    /**
     * Seals the exact issued admission; only a real canonical drain receipt qualifies as success.
     */
    @DELETE
    @Path("/{taskId}")
    @Consumes("application/x-protobuf")
    @Produces("application/x-protobuf")
    public Response cancelTask(@PathParam("taskId") String taskId, byte[] canonicalSeal) {
        try {
            if (canonicalSeal == null || canonicalSeal.length == 0) {
                throw new IllegalArgumentException("Exact canonical admission seal is required");
            }
            var request =
                    org.pangrova.protocol.v2.TaskAdmissionSealRequest.parseFrom(canonicalSeal);
            org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.validate(request, taskId);
            var receipt = taskManager.sealAdmission(request);
            return receipt.isPresent()
                    ? Response.ok(receipt.orElseThrow().toByteArray(), "application/x-protobuf")
                            .build()
                    : Response.status(Response.Status.ACCEPTED).build();
        } catch (com.google.protobuf.InvalidProtocolBufferException
                | IllegalArgumentException failure) {
            return sealError(Response.Status.BAD_REQUEST, failure);
        } catch (org.pangrova.worker.exception.TaskAdmissionException failure) {
            return sealError(
                    failure.reason()
                                    == org.pangrova.worker.enums.TaskAdmissionReason
                                            .CAPACITY
                            ? Response.Status.SERVICE_UNAVAILABLE
                            : Response.Status.CONFLICT,
                    failure);
        } catch (org.pangrova.worker.exception.ControlEpochFenceException failure) {
            return sealError(epochRejectionStatus(failure), failure.reason().name(), failure);
        } catch (org.pangrova.worker.exception.ExecutionOwnerFenceException failure) {
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
        return Response.status(status)
                .type(MediaType.APPLICATION_JSON)
                .entity(
                        new org.pangrova.common.client.error.ErrorDetail(
                                code, "SYSTEM_ERROR", failure.getMessage()))
                .build();
    }

    private static int epochRejectionStatus(
            org.pangrova.worker.exception.ControlEpochFenceException failure) {
        return switch (failure.reason()) {
            case CONTROL_PLANE_NOT_READY -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
            // No admission/seal side effect: wait for an authenticated heartbeat to observe
            // this epoch.
            case EPOCH_AHEAD -> HTTP_TOO_EARLY;
            default -> Response.Status.CONFLICT.getStatusCode();
        };
    }

    /** Handles an upstream commit notification for a running task. */
    @POST
    @Path("/upstream-commit")
    public Response onUpstreamCommit(UpstreamCommitNotification notification) {
        if (notification == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(
                            new org.pangrova.common.client.error.ErrorDetail(
                                    "BAD_REQUEST", "USER_ERROR", "Request body must not be null"))
                    .build();
        }
        exchangeSourceRegistry.notifyCommit(notification.queryId(), notification.upstreamStageId());
        LOG.info(
                "event=upstream_commit_received queryId={} upstreamStageId={}",
                notification.queryId(),
                notification.upstreamStageId());
        return Response.ok().build();
    }

    /** Injects a dynamic filter into a running task. */
    @POST
    @Path("/{taskId}/dynamic-filter")
    public Response injectDynamicFilter(
            @PathParam("taskId") String taskId,
            org.pangrova.protocol.rest.task.InjectDynamicFilterRequest request) {
        if (request == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(
                            new org.pangrova.common.client.error.ErrorDetail(
                                    "BAD_REQUEST", "USER_ERROR", "Request body must not be null"))
                    .build();
        }
        String workerTaskId = taskManager.resolveWorkerTaskId(taskId);
        OptionalLong handle =
                workerTaskId == null
                        ? OptionalLong.empty()
                        : taskManager.getExecutorHandle(workerTaskId);
        if (handle.isEmpty()) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(
                            new org.pangrova.common.client.error.ErrorDetail(
                                    "CONFLICT",
                                    "USER_ERROR",
                                    "Executor not ready for task: " + taskId))
                    .build();
        }
        byte[] commandProto =
                org.pangrova.worker.jni.NativeCommandBuilder.injectRuntimeFilter(
                        request.planNodeId(), request.filterProto());
        Optional<org.pangrova.worker.execution.TaskExecutor> executor =
                taskManager.getSharedNativeExecutor();
        if (executor.isEmpty()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(
                            new org.pangrova.common.client.error.ErrorDetail(
                                    "NATIVE_EXECUTOR_UNAVAILABLE",
                                    "SYSTEM_ERROR",
                                    "Native executor unavailable for task: " + taskId))
                    .build();
        }
        byte[] response = executor.get().executeCommand(handle.getAsLong(), commandProto);
        org.pangrova.worker.jni.NativeCommandBuilder.requireSuccess(
                response, "INJECT_DYNAMIC_FILTER task=" + taskId);
        LOG.info(
                "event=dynamic_filter_injected taskId={} planNodeId={}",
                taskId,
                request.planNodeId());
        return Response.ok().build();
    }

    private org.pangrova.observability.api.tracing.Span startSpan(String operationName) {
        if (tracingProvider == null) {
            return null;
        }
        return tracingProvider.startSpan(
                operationName, org.pangrova.observability.api.enums.SpanKind.SERVER);
    }

    private static void endSpan(org.pangrova.observability.api.tracing.Span span) {
        if (span != null) {
            span.end();
        }
    }

    @Override
    public void close() {
        // No resources to close — transport SPIs are managed by bootstrap
    }
}
