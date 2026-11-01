package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.admission.QueryRejectedException;
import org.pangrova.coordinator.query.execution.AsyncSubmitConfig;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.client.protocol.SubmitQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns asynchronous submission of the same query dispatch path, never a second executor. */
final class QueryAsyncSubmission {
    private static final Logger LOG = LoggerFactory.getLogger(QueryAsyncSubmission.class);
    private final QueryDispatcher dispatcher;
    private final AsyncSubmitConfig config;
    private final ExecutorService executor;
    private final FailureHandler failureHandler;
    private final ResultHandler resultHandler;

    QueryAsyncSubmission(QueryDispatcher dispatcher, AsyncSubmitConfig config, ExecutorService executor,
                         FailureHandler failureHandler, ResultHandler resultHandler) {
        this.dispatcher = dispatcher;
        this.config = config;
        this.executor = executor;
        this.failureHandler = failureHandler;
        this.resultHandler = resultHandler;
    }

    SubmitQueryResponse execute(
            String queryId, String sql, CoordinatorSessionContext session, QueryStateMachine state) {
        Context context = new Context(queryId, sql, session, state, new CountDownLatch(1), new AtomicReference<>());
        state.resultEndpointReady().thenRun(context.ready()::countDown);
        state.writeDispatchReady().thenRun(context.ready()::countDown);
        state.addStateChangeListener((previous, current) -> {
            if ((current == QueryStateMachine.State.FAILED && state.getError().isPresent())
                    || current == QueryStateMachine.State.CANCELLED) {
                context.ready().countDown();
            }
        });
        Future<?> dispatch = submit(context);
        awaitReady(context, dispatch);
        requireResult(context, dispatch);
        return new SubmitQueryResponse(queryId, state.getState().name(), state.getResultEndpoint().orElse(null),
                dispatcher.removeColumnNames(queryId));
    }

    private Future<?> submit(Context context) {
        try {
            return executor.submit(() -> {
                var previousLogContext = org.pangrova.observability.sdk.logging.mdc.LogContext.snapshot();
                try (var scope = context.session().traceContext().makeCurrent()) {
                    org.pangrova.observability.sdk.logging.mdc.LogContext.set(
                            context.session().traceContext().traceId(), context.session().traceContext().spanId());
                    org.pangrova.observability.sdk.logging.mdc.LogContext.setQueryId(context.queryId());
                    resultHandler.accept(context.queryId(), context.state(),
                            dispatcher.execute(context.state(), context.sql(), context.session()));
                } catch (RuntimeException | Error failure) {
                    context.failure().set(failure);
                    LOG.error("event=async_query_execution_failed queryId={}", context.queryId(), failure);
                    String code = failure instanceof PangrovaException known
                            ? known.getErrorCode().getCode() : "INTERNAL_ERROR";
                    fail(context, code, failure.getClass().getSimpleName(),
                            String.valueOf(failure.getMessage()), "async_dispatch_failed");
                } finally {
                    org.pangrova.observability.sdk.logging.mdc.LogContext.restore(previousLogContext);
                    context.ready().countDown();
                }
            });
        } catch (RejectedExecutionException overloaded) {
            fail(context, "RESOURCE_EXHAUSTED", "ASYNC_SUBMIT_QUEUE_FULL",
                    "Coordinator query dispatch queue is full", "async_submit_overloaded");
            throw new QueryRejectedException("Coordinator query dispatch queue is full");
        }
    }

    private void awaitReady(Context context, Future<?> dispatch) {
        try {
            if (!context.ready().await(config.submitTimeoutMs(), TimeUnit.MILLISECONDS)) {
                dispatch.cancel(true);
                fail(context, "QUERY_TIMEOUT", "ASYNC_SUBMIT_TIMEOUT",
                        "Result endpoint or write dispatch was not ready before submit timeout",
                        "async_submit_timeout");
                throw new PangrovaException(PangrovaErrorCode.RUNTIME_QUERY_TIMEOUT,
                        "Result endpoint or write dispatch was not ready before submit timeout: " + context.queryId());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            dispatch.cancel(true);
            fail(context, "QUERY_CANCELLED", "ASYNC_SUBMIT_INTERRUPTED",
                    "Query submission was interrupted", "async_submit_interrupted");
            throw new QueryRejectedException("Query submission was interrupted");
        }
    }

    private void requireResult(Context context, Future<?> dispatch) {
        Throwable failure = context.failure().get();
        if (failure instanceof Error fatal) {
            throw fatal;
        }
        if (failure instanceof RuntimeException rejected) {
            throw rejected;
        }
        var state = context.state().getState();
        if (state == QueryStateMachine.State.FAILED || state == QueryStateMachine.State.CANCELLED) {
            dispatch.cancel(true);
            throw new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    context.state().getError().map(ErrorDetail::message)
                            .orElse("Query ended before the result endpoint was ready"));
        }
        if (context.state().getResultEndpoint().isEmpty() && !context.state().isWriteDispatchReady()
                || state != QueryStateMachine.State.RUNNING && state != QueryStateMachine.State.FINISHING
                    && state != QueryStateMachine.State.FINISHED) {
            if (!context.state().isTerminal()) {
                fail(context, "INTERNAL_ERROR", "RESULT_ENDPOINT_UNAVAILABLE",
                        "Query dispatch completed without a ready result endpoint or write dispatch",
                        "async_result_endpoint_unavailable");
            }
            throw new IllegalStateException(
                    "Query dispatch completed without a ready result endpoint or write dispatch: " + context.queryId());
        }
    }

    private void fail(Context context, String code, String type, String message, String reason) {
        failureHandler.fail(context.queryId(), new ErrorDetail(code, type, message), reason);
    }

    @FunctionalInterface
    interface FailureHandler {
        void fail(String queryId, ErrorDetail error, String reason);
    }

    @FunctionalInterface
    interface ResultHandler {
        void accept(String queryId, QueryStateMachine state, DispatchResult result);
    }

    private record Context(
            String queryId, String sql, CoordinatorSessionContext session, QueryStateMachine state,
            CountDownLatch ready, AtomicReference<Throwable> failure) { }
}
