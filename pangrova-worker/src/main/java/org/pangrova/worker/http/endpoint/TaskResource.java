package org.pangrova.worker.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.annotation.Nullable;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
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
import org.pangrova.protocol.rest.task.UpdateExchangeSourcesRequest;
import org.pangrova.protocol.rest.task.UpstreamCommitNotification;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.worker.exchange.source.ExchangeSourceConverter;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.execution.VectorSearchResultBuffer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.convert.TaskRequestConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * JAX-RS resource for Worker task operations.
 * Exchange source updates are delegated to the {@link ExchangeTransport} SPI.
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
    private final org.pangrova.spi.observability.TracingProvider tracingProvider;
    @Nullable
    private final VectorSearchResultBuffer vectorResultBuffer;

    public TaskResource(TaskManager taskManager, ExchangeTransport exchangeTransport,
                        String workerHost, int transportPort) {
        this(taskManager, exchangeTransport, new ExchangeSourceRegistry(), workerHost, transportPort, null);
    }

    public TaskResource(TaskManager taskManager, ExchangeTransport exchangeTransport,
                        ExchangeSourceRegistry exchangeSourceRegistry,
                        String workerHost, int transportPort,
                        org.pangrova.spi.observability.TracingProvider tracingProvider) {
        this(taskManager, exchangeTransport, exchangeSourceRegistry,
                workerHost, transportPort, tracingProvider, null);
    }

    /** Creates the resource with all collaborator dependencies. */
    public TaskResource(TaskManager taskManager, ExchangeTransport exchangeTransport,
                        ExchangeSourceRegistry exchangeSourceRegistry,
                        String workerHost, int transportPort,
                        org.pangrova.spi.observability.TracingProvider tracingProvider,
                        @Nullable VectorSearchResultBuffer vectorResultBuffer) {
        this.taskManager = checkNotNull(taskManager, "taskManager must not be null");
        this.exchangeTransport = checkNotNull(exchangeTransport, "exchangeTransport must not be null");
        this.exchangeSourceRegistry = checkNotNull(exchangeSourceRegistry,
                "exchangeSourceRegistry must not be null");
        this.workerHost = checkNotNull(workerHost, "workerHost must not be null");
        this.transportPort = transportPort;
        this.tracingProvider = tracingProvider;
        this.vectorResultBuffer = vectorResultBuffer;
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
        TaskDescriptor descriptor = TaskRequestConverter.toTaskDescriptor(request);
        String workerTaskId = taskManager.submitTask(descriptor, request);
        int shufflePort = taskManager.getShufflePort(workerTaskId);
        int effectivePort = shufflePort > 0 ? shufflePort : transportPort;
        ResultEndpoint endpoint = new ResultEndpoint(
                workerHost, effectivePort, request.taskId(), ResultProtocol.GRPC.value());
        SubmitTaskResponse response = new SubmitTaskResponse(true, null, request.taskId(), endpoint);
        return Response.ok(response).build();
    }

    private Response handleSubmitError(SubmitTaskRequest request, Exception e) {
        LOG.error("event=task_submit_failed queryId={} error={}",
                request.queryId(), e.getMessage(), e);
        SubmitTaskResponse errorResponse = new SubmitTaskResponse(
                false, e.getMessage(), null, null);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(errorResponse)
                .build();
    }

    /** Cancels the task identified by {@code taskId}. */
    @DELETE
    @Path("/{taskId}")
    public Response cancelTask(@PathParam("taskId") String taskId) {
        org.pangrova.worker.task.TaskCancelResult result = taskManager.cancelTask(taskId);
        if (result.status() == org.pangrova.worker.task.TaskCancelResult.Status.NOT_FOUND) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "NOT_FOUND", "USER_ERROR",
                            result.errorMessage()))
                    .build();
        }
        if (!result.isSuccess()) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "CONFLICT", "USER_ERROR",
                            result.errorMessage()))
                    .build();
        }
        LOG.info("event=task_cancelled taskId={}", taskId);
        return Response.ok().build();
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
        OptionalLong handle = taskManager.getExecutorHandle(taskId);
        if (handle.isEmpty()) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "CONFLICT", "USER_ERROR",
                            "Executor not ready for task: " + taskId))
                    .build();
        }
        byte[] commandProto = org.pangrova.worker.jni.NativeCommandBuilder.injectDynamicFilter(
                handle.getAsLong(), request.planNodeId(), request.filterProto());
        Optional<org.pangrova.spi.execution.TaskExecutor> executor = taskManager.getSharedNativeExecutor();
        if (executor.isEmpty()) {
            LOG.warn("event=dynamic_filter_inject_skip taskId={} reason=no_executor", taskId);
            return Response.ok().build();
        }
        executor.get().executeCommand(handle.getAsLong(), commandProto);
        LOG.info("event=dynamic_filter_injected taskId={} planNodeId={}", taskId, request.planNodeId());
        return Response.ok().build();
    }

    /** Updates the exchange sources of a running task. */
    @POST
    @Path("/{taskId}/exchange-sources")
    public Response updateExchangeSources(@PathParam("taskId") String taskId,
                                          UpdateExchangeSourcesRequest request) {
        OptionalLong handle = taskManager.getExecutorHandle(taskId);
        if (handle.isEmpty()) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "CONFLICT", "USER_ERROR",
                            "Executor not ready for task: " + taskId))
                    .build();
        }
        List<ExchangeEndpoint> spiSources = ExchangeSourceConverter.toSpiEndpoints(
                request.exchangeSources());
        exchangeTransport.addSources(handle.getAsLong(), spiSources);
        LOG.info("event=exchange_sources_update taskId={} reason={} sourceCount={}",
                taskId, request.reason(), request.exchangeSources().size());
        return Response.ok(new ExchangeAckResponse(taskId, true)).build();
    }

    /** Streams the serialized vector search result of a completed task. */
    @GET
    @Path("/{taskId}/vector-result")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getVectorResult(@PathParam("taskId") String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "BAD_REQUEST", "USER_ERROR",
                            "taskId must not be blank"))
                    .build();
        }
        if (vectorResultBuffer == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "NOT_FOUND", "SYSTEM_ERROR",
                            "Vector result buffer not available on this worker"))
                    .build();
        }
        byte[] data = vectorResultBuffer.take(taskId);
        if (data == null) {
            LOG.info("event=vector_result_not_found taskId={}", taskId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new org.pangrova.protocol.rest.error.ErrorDetail(
                            "NOT_FOUND", "USER_ERROR",
                            "No vector result found for task: " + taskId))
                    .build();
        }
        LOG.info("event=vector_result_fetched taskId={} bytes={}", taskId, data.length);
        return Response.ok(data).build();
    }

    record ExchangeAckResponse(String taskId, boolean acknowledged) {}

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
