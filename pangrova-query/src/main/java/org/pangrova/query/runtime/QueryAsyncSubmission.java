package org.pangrova.query.runtime;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.runtime.enums.SubmissionReadiness;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.observability.logging.LogContext;
import org.pangrova.query.lifecycle.QueryExecutionPort;
import org.pangrova.query.lifecycle.QueryDispatchOutcome;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns asynchronous submission of the same query dispatch path, never a second executor. */
final class QueryAsyncSubmission {
    private static final Logger LOG = LoggerFactory.getLogger(QueryAsyncSubmission.class);
    private final QueryExecutionPort dispatcher;
    private final AsyncSubmitConfig config;
    private final ExecutorService executor;
    private final ScheduledExecutorService deadlines;
    private final FailureHandler failureHandler;
    private final ResultHandler resultHandler;

    QueryAsyncSubmission(QueryExecutionPort dispatcher, AsyncSubmitConfig config, ExecutorService executor,
            ScheduledExecutorService deadlines, FailureHandler failureHandler, ResultHandler resultHandler) {
        this.dispatcher = dispatcher;
        this.config = config;
        this.executor = executor;
        this.deadlines = deadlines;
        this.failureHandler = failureHandler;
        this.resultHandler = resultHandler;
    }

    Pending start(String queryId, String sql, QuerySession session, QueryStateMachine state) {
        Context context = new Context(queryId, sql, session, state, new CompletableFuture<>(), new AtomicReference<>());
        Future<?> dispatch = submit(context);
        var endpoint = state.resultEndpointReady();
        var write = state.writeDispatchReady();
        var terminal = state.terminalStateReady();
        endpoint.thenRun(() -> context.ready().complete(SubmissionReadiness.READY));
        write.thenRun(() -> context.ready().complete(SubmissionReadiness.READY));
        terminal.thenAccept(current -> {
            if ((current == QueryState.FAILED && state.getError().isPresent())
                    || current == QueryState.CANCELLED
                    || current == QueryState.FINISHED
                        && state.status().disposition() == ResultDisposition.NONE) {
                context.ready().complete(SubmissionReadiness.READY);
            }
        });
        context.ready().whenComplete((ready, failure) -> {
            endpoint.cancel(false);
            write.cancel(false);
            terminal.cancel(false);
        });
        try {
            var timeout = context.ready().isDone() ? null : deadlines.schedule(
                    () -> { context.ready().complete(SubmissionReadiness.TIMED_OUT); },
                    config.submitTimeoutMs(), TimeUnit.MILLISECONDS);
            var receipt = context.ready().thenApply(readiness -> {
                if (timeout != null) timeout.cancel(false);
                return buildReceipt(context, dispatch, readiness);
            });
            return new Pending(receipt, () -> context.ready().complete(SubmissionReadiness.INTERRUPTED));
        } catch (RejectedExecutionException stopping) {
            dispatch.cancel(true);
            context.ready().completeExceptionally(stopping);
            throw new QueryRejectedException("Coordinator query submission is stopping");
        }
    }

    private QuerySubmissionResult buildReceipt(Context context, Future<?> dispatch, SubmissionReadiness readiness) {
        if (readiness == SubmissionReadiness.TIMED_OUT) {
            try {
                fail(context, "QUERY_TIMEOUT", "ASYNC_SUBMIT_TIMEOUT",
                        "Result endpoint or write dispatch was not ready before submit timeout",
                        "async_submit_timeout");
            } finally {
                dispatch.cancel(true);
            }
            throw new PangrovaException(PangrovaErrorCode.RUNTIME_QUERY_TIMEOUT,
                    "Result endpoint or write dispatch was not ready before submit timeout: " + context.queryId());
        }
        if (readiness == SubmissionReadiness.INTERRUPTED) {
            try {
                fail(context, "QUERY_CANCELLED", "ASYNC_SUBMIT_INTERRUPTED",
                        "Query submission was interrupted", "async_submit_interrupted");
            } finally {
                dispatch.cancel(true);
            }
            throw new QueryRejectedException("Query submission was interrupted");
        }
        requireResult(context);
        var state = context.state();
        var status = state.status();
        return new QuerySubmissionResult(context.queryId(), status.state(), status.disposition(),
                status.endpoint(), dispatcher.removeColumnNames(context.queryId()), state.branchSelection(), null);
    }

    private Future<?> submit(Context context) {
        try {
            return executor.submit(() -> start(context));
        } catch (RejectedExecutionException overloaded) {
            fail(context, "RESOURCE_EXHAUSTED", "ASYNC_SUBMIT_QUEUE_FULL",
                    "Coordinator query dispatch queue is full", "async_submit_overloaded");
            throw new QueryRejectedException("Coordinator query dispatch queue is full");
        }
    }

    private void start(Context context) {
        CompletionStage<QueryDispatchOutcome> completion;
        var previousLogContext = LogContext.snapshot();
        try (var scope = context.session().traceContext().makeCurrent()) {
            setLogContext(context);
            completion = dispatcher.dispatchAsync(context.state(), context.sql(), context.session());
        } catch (RuntimeException | Error failure) {
            finish(context, null, failure);
            return;
        } finally {
            LogContext.restore(previousLogContext);
        }
        completion.whenComplete((result, failure) -> finish(context, result, failure));
    }

    private void finish(Context context, QueryDispatchOutcome result, Throwable failure) {
        var previousLogContext = LogContext.snapshot();
        try (var scope = context.session().traceContext().makeCurrent()) {
            setLogContext(context);
            if (failure == null) {
                resultHandler.accept(context.queryId(), context.state(), result);
            } else {
                reportFailure(context, unwrap(failure));
            }
        } catch (RuntimeException | Error callbackFailure) {
            if (failure == null) {
                reportFailure(context, callbackFailure);
            } else {
                LOG.error("event=async_query_failure_publication_failed queryId={}",
                        context.queryId(), callbackFailure);
            }
        } finally {
            LogContext.restore(previousLogContext);
            context.ready().complete(SubmissionReadiness.READY);
        }
    }

    private void reportFailure(Context context, Throwable failure) {
        if (failure instanceof CancellationException && context.state().isTerminal()) return;
        context.failure().set(failure);
        LOG.error("event=async_query_execution_failed queryId={}", context.queryId(), failure);
        String code = failure instanceof PangrovaException known
                ? known.getErrorCode().getCode() : "INTERNAL_ERROR";
        try {
            fail(context, code, failure.getClass().getSimpleName(),
                    String.valueOf(failure.getMessage()), "async_dispatch_failed");
        } catch (RuntimeException | Error publication) {
            if (publication != failure) failure.addSuppressed(publication);
            LOG.error("event=async_query_failure_publication_failed queryId={}", context.queryId(), publication);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
    }

    private static void setLogContext(Context context) {
        var trace = context.session().traceContext();
        LogContext.set(trace.traceId(), trace.spanId());
        LogContext.setQueryId(context.queryId());
    }

    private void requireResult(Context context) {
        Throwable failure = context.failure().get();
        if (failure instanceof Error fatal) {
            throw fatal;
        }
        if (failure instanceof RuntimeException rejected) {
            throw rejected;
        }
        var status = context.state().status();
        var state = status.state();
        if (state == QueryState.FAILED || state == QueryState.CANCELLED) {
            throw new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    context.state().getError().map(QueryFailure::message)
                            .orElse("Query ended before its result disposition was ready"));
        }
        boolean pendingWriteDispatch = status.disposition() == ResultDisposition.PENDING
                && context.state().isWriteDispatchReady();
        if (status.disposition() == ResultDisposition.PENDING && !pendingWriteDispatch
                || state != QueryState.RUNNING && state != QueryState.FINISHING
                    && state != QueryState.FINISHED) {
            if (!context.state().isTerminal()) {
                fail(context, "INTERNAL_ERROR", "RESULT_ENDPOINT_UNAVAILABLE",
                        "Query dispatch completed without a resolved result disposition",
                        "async_result_endpoint_unavailable");
            }
            throw new IllegalStateException(
                    "Query dispatch completed without a resolved result disposition: " + context.queryId());
        }
    }

    private void fail(Context context, String code, String type, String message, String reason) {
        failureHandler.fail(context.queryId(), new QueryFailure(code, type, message), reason);
    }

    @FunctionalInterface
    interface FailureHandler {
        void fail(String queryId, QueryFailure error, String reason);
    }

    @FunctionalInterface
    interface ResultHandler {
        void accept(String queryId, QueryStateMachine state, QueryDispatchOutcome result);
    }

    record Pending(CompletableFuture<QuerySubmissionResult> receipt, Runnable interrupt) {
        static Pending completed(QuerySubmissionResult receipt) {
            return new Pending(CompletableFuture.completedFuture(receipt), () -> { });
        }
    }

    private record Context(
            String queryId, String sql, QuerySession session, QueryStateMachine state,
            CompletableFuture<SubmissionReadiness> ready, AtomicReference<Throwable> failure) { }
}
