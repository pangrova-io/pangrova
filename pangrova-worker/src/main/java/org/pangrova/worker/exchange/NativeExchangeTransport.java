package org.pangrova.worker.exchange;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.google.protobuf.Message;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.ConnectExchangeSourceRequest;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.FinishExchangeSourceRequest;
import org.pangrova.protocol.v2.NoMoreExchangeSourcesRequest;
import org.pangrova.protocol.v2.PushExchangeBatchRequest;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.shuffle.exchange.DirectExchangeTransport;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.shuffle.exchange.ExchangeInput;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.shuffle.exchange.ExternalShuffleTransport;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.exchange.remote.RemoteShuffleClientException;
import org.pangrova.worker.exchange.remote.RemoteShuffleClientPool;
import org.pangrova.worker.exchange.remote.RemoteShuffleExchangeSource;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Binds canonical direct or external exchange inputs to one Native executor. */
public final class NativeExchangeTransport implements ExchangeTransport, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeExchangeTransport.class);
    private static final int DEFAULT_READER_THREADS = Math.max(
            2, Math.min(32, Runtime.getRuntime().availableProcessors() * 2));
    private static final AtomicInteger READER_THREAD_ID = new AtomicInteger();

    private final TaskExecutor taskExecutor;
    private final RemoteShuffleClientPool clientPool;
    private final ThreadPoolExecutor readerExecutor;
    private final ConcurrentMap<Long, ExternalShuffleInputSession> sessions =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Creates a direct-exchange transport with no external-shuffle reader. */
    public NativeExchangeTransport(TaskExecutor taskExecutor) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.clientPool = null;
        this.readerExecutor = null;
    }

    /** Creates a transport that owns the supplied external-shuffle client pool. */
    public NativeExchangeTransport(
            TaskExecutor taskExecutor,
            RemoteShuffleClientPool clientPool) {
        this(taskExecutor, clientPool, DEFAULT_READER_THREADS);
    }

    NativeExchangeTransport(
            TaskExecutor taskExecutor,
            RemoteShuffleClientPool clientPool,
            int readerThreads) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.clientPool = checkNotNull(clientPool, "clientPool must not be null");
        if (readerThreads <= 0) {
            throw new IllegalArgumentException("readerThreads must be positive");
        }
        this.readerExecutor = newReaderExecutor(readerThreads);
    }

    @Override
    public void bindInputs(
            long executorHandle,
            ExchangeTaskAttempt consumerAttempt,
            List<ExchangeInput> inputs) {
        requireOpenHandle(executorHandle);
        checkNotNull(consumerAttempt, "consumerAttempt must not be null");
        checkNotNull(inputs, "inputs must not be null");
        if (inputs.isEmpty()) {
            return;
        }
        int externalCount = countExternalInputs(inputs);
        requireExternalReader(externalCount);
        validateExternalAttempts(inputs, consumerAttempt);
        ExternalShuffleInputSession session = installSession(executorHandle, externalCount);
        try {
            try {
                bindAll(executorHandle, consumerAttempt, inputs, session);
            } finally {
                if (session != null) session.bindingFinished();
            }
        } catch (RuntimeException failure) {
            if (session != null) {
                try {
                    releaseInputs(executorHandle);
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
        LOG.info("event=native_exchange_inputs_bound executorHandle={} inputCount={} externalCount={}",
                executorHandle, inputs.size(), externalCount);
    }

    private void bindAll(
            long handle,
            ExchangeTaskAttempt attempt,
            List<ExchangeInput> inputs,
            ExternalShuffleInputSession session) {
        List<PendingExternalInput> pending = connectInputs(
                handle, attempt, inputs, session);
        sealInputs(handle, attempt, inputs);
        startExternalReaders(handle, attempt, pending, session);
    }

    private List<PendingExternalInput> connectInputs(
            long handle,
            ExchangeTaskAttempt attempt,
            List<ExchangeInput> inputs,
            ExternalShuffleInputSession session) {
        List<PendingExternalInput> pending = new ArrayList<>();
        for (ExchangeInput input : inputs) {
            if (input.transport() instanceof DirectExchangeTransport direct) {
                connectDirect(handle, attempt, input, direct);
            } else if (input.transport() instanceof ExternalShuffleTransport external) {
                pending.add(connectExternal(
                        handle, attempt, input, external, session));
            } else {
                throw new IllegalArgumentException(
                        "Unsupported exchange transport: " + input.transport().getClass());
            }
        }
        return pending;
    }

    private void connectDirect(
            long handle,
            ExchangeTaskAttempt attempt,
            ExchangeInput input,
            DirectExchangeTransport direct) {
        for (ExchangeEndpoint source : direct.sources()) {
            ConnectExchangeSourceRequest payload = ConnectExchangeSourceRequest.newBuilder()
                    .setPlanNodeId(input.planNodeId())
                    .setExchangeId(input.exchangeId())
                    .setDirect(ExchangeProtocolMapper.directSource(source))
                    .build();
            execute(handle, attempt,
                    RuntimeCommandType.RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE,
                    payload, "connect direct exchange source " + source.sourceTaskId());
        }
    }

    private PendingExternalInput connectExternal(
            long handle,
            ExchangeTaskAttempt attempt,
            ExchangeInput input,
            ExternalShuffleTransport external,
            ExternalShuffleInputSession session) {
        ConnectExchangeSourceRequest payload = ConnectExchangeSourceRequest.newBuilder()
                .setPlanNodeId(input.planNodeId())
                .setExchangeId(input.exchangeId())
                .setExternal(ExchangeProtocolMapper.externalInput(external))
                .build();
        execute(handle, attempt,
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE,
                payload, "connect external shuffle " + external.shuffleId());
        RemoteShuffleExchangeSource source = new RemoteShuffleExchangeSource(
                external, clientPool.borrow(external.selectedEndpoint()));
        session.addSource(source);
        return new PendingExternalInput(input, source);
    }

    private void sealInputs(
            long handle,
            ExchangeTaskAttempt attempt,
            List<ExchangeInput> inputs) {
        for (ExchangeInput input : inputs) {
            NoMoreExchangeSourcesRequest payload = NoMoreExchangeSourcesRequest.newBuilder()
                    .setPlanNodeId(input.planNodeId())
                    .setExchangeId(input.exchangeId())
                    .setExpectedSourcePartitions(input.expectedSourcePartitions())
                    .build();
            execute(handle, attempt,
                    RuntimeCommandType.RUNTIME_COMMAND_TYPE_NO_MORE_EXCHANGE_SOURCES,
                    payload, "seal exchange input " + input.planNodeId());
        }
    }

    private void startExternalReaders(
            long handle,
            ExchangeTaskAttempt attempt,
            List<PendingExternalInput> pending,
            ExternalShuffleInputSession session) {
        for (PendingExternalInput input : pending) {
            ExternalShuffleInputPump pump = new ExternalShuffleInputPump(
                    () -> pumpExternalInput(handle, attempt, input, session));
            session.addPump(pump);
            try {
                readerExecutor.execute(pump);
            } catch (RuntimeException schedulingFailure) {
                pump.cancel();
                throw schedulingFailure;
            }
        }
    }

    private void pumpExternalInput(
            long handle,
            ExchangeTaskAttempt attempt,
            PendingExternalInput pending,
            ExternalShuffleInputSession session) {
        try (RemoteShuffleExchangeSource source = pending.source()) {
            BatchChunk chunk;
            while ((chunk = source.nextVerifiedChunk()) != null) {
                if (session.isClosed()) {
                    break;
                }
                pushChunk(handle, attempt, pending.input(), chunk);
            }
            if (!session.isClosed()) {
                finishInput(handle, attempt, pending.input(),
                        source.lastCompletedSequence(), null);
            }
        } catch (RuntimeException failure) {
            finishFailedInput(
                    handle, attempt, pending.input(),
                    pending.source().lastCompletedSequence(), failure, session);
        }
    }

    private void pushChunk(
            long handle,
            ExchangeTaskAttempt attempt,
            ExchangeInput input,
            BatchChunk chunk) {
        PushExchangeBatchRequest payload = PushExchangeBatchRequest.newBuilder()
                .setPlanNodeId(input.planNodeId())
                .setExchangeId(input.exchangeId())
                .setChunk(chunk)
                .build();
        execute(handle, attempt,
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH,
                payload, "push external exchange batch chunk "
                        + chunk.getChunkIndex() + "/" + chunk.getChunkCount());
    }

    private void finishFailedInput(
            long handle,
            ExchangeTaskAttempt attempt,
            ExchangeInput input,
            long lastSequence,
            RuntimeException failure,
            ExternalShuffleInputSession session) {
        if (session.isClosed()) {
            return;
        }
        try {
            finishInput(handle, attempt, input, lastSequence, toExecutionError(failure));
        } catch (RuntimeException finishFailure) {
            failure.addSuppressed(finishFailure);
            LOG.error("event=external_exchange_finish_failed executorHandle={} planNodeId={} error={}",
                    handle, input.planNodeId(), finishFailure.getMessage(), failure);
            cancelNativeTask(handle, failure);
        }
    }

    private void finishInput(
            long handle,
            ExchangeTaskAttempt attempt,
            ExchangeInput input,
            long lastSequence,
            ExecutionError failure) {
        FinishExchangeSourceRequest.Builder payload = FinishExchangeSourceRequest.newBuilder()
                .setPlanNodeId(input.planNodeId())
                .setExchangeId(input.exchangeId())
                .setLastSequence(lastSequence);
        if (failure != null) {
            payload.setFailure(failure);
        }
        execute(handle, attempt,
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE,
                payload.build(), "finish external exchange source " + input.planNodeId());
    }

    private void cancelNativeTask(long handle, RuntimeException failure) {
        try {
            taskExecutor.cancelTask(handle);
        } catch (RuntimeException cancelFailure) {
            failure.addSuppressed(cancelFailure);
        }
    }

    @Override
    public void releaseInputs(long executorHandle) {
        ExternalShuffleInputSession session = sessions.get(executorHandle);
        if (session != null) {
            session.close();
            sessions.remove(executorHandle, session);
            LOG.debug("event=native_exchange_inputs_released executorHandle={}", executorHandle);
        }
    }

    @Override
    public void close() {
        closed.set(true);
        RuntimeException failure = null;
        for (long handle : List.copyOf(sessions.keySet())) {
            try {
                releaseInputs(handle);
            } catch (RuntimeException cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        if (readerExecutor != null) {
            readerExecutor.shutdownNow();
            try {
                awaitReaderTermination();
            } catch (RuntimeException shutdownFailure) {
                if (failure == null) failure = shutdownFailure;
                else failure.addSuppressed(shutdownFailure);
            }
        }
        if (failure != null) throw failure;
        if (clientPool != null) {
            clientPool.close();
        }
    }

    private void awaitReaderTermination() {
        try {
            if (!readerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("External shuffle reader executor has not exited");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while joining exchange reader executor", failure);
        }
    }

    private ExternalShuffleInputSession installSession(long handle, int externalCount) {
        if (externalCount == 0) {
            return null;
        }
        ExternalShuffleInputSession session = new ExternalShuffleInputSession();
        ExternalShuffleInputSession existing = sessions.putIfAbsent(handle, session);
        if (existing != null) {
            throw new IllegalStateException(
                    "Exchange inputs already bound for executorHandle=" + handle);
        }
        if (closed.get()) {
            session.bindingFinished();
            releaseInputs(handle);
            throw new IllegalStateException("NativeExchangeTransport is closed");
        }
        return session;
    }

    private void requireExternalReader(int externalCount) {
        if (externalCount > 0 && (clientPool == null || readerExecutor == null)) {
            throw new IllegalStateException(
                    "External shuffle input requires a configured reader pool");
        }
    }

    private void requireOpenHandle(long executorHandle) {
        if (executorHandle <= 0) {
            throw new IllegalArgumentException("executorHandle must be positive");
        }
        if (closed.get()) {
            throw new IllegalStateException("NativeExchangeTransport is closed");
        }
    }

    private static int countExternalInputs(List<ExchangeInput> inputs) {
        return Math.toIntExact(inputs.stream()
                .filter(input -> input.transport() instanceof ExternalShuffleTransport)
                .count());
    }

    private static void validateExternalAttempts(
            List<ExchangeInput> inputs, ExchangeTaskAttempt attempt) {
        for (ExchangeInput input : inputs) {
            if (input.transport() instanceof ExternalShuffleTransport external
                    && !external.consumerAttempt().equals(attempt)) {
                throw new IllegalArgumentException(
                        "External shuffle consumer attempt does not match command envelope");
            }
        }
    }

    private static ExecutionError toExecutionError(RuntimeException failure) {
        boolean corruption = failure instanceof RemoteShuffleClientException
                && failure.getMessage() != null
                && failure.getMessage().contains("corruption");
        return ExecutionError.newBuilder()
                .setErrorClass(corruption
                        ? ErrorClass.ERROR_CLASS_DATA_CORRUPTION
                        : ErrorClass.ERROR_CLASS_TRANSPORT)
                .setCode(corruption
                        ? "EXTERNAL_SHUFFLE_CORRUPTION"
                        : "EXTERNAL_SHUFFLE_READ_FAILED")
                .setMessage(failure.getMessage() == null
                        ? failure.getClass().getSimpleName() : failure.getMessage())
                .setRetryable(!corruption)
                .setFailedComponent("external-shuffle-ingress")
                .build();
    }

    private void execute(
            long executorHandle,
            ExchangeTaskAttempt attempt,
            RuntimeCommandType type,
            Message payload,
            String operation) {
        byte[] command = NativeCommandBuilder.buildCommand(
                type, ExchangeProtocolMapper.taskAttempt(attempt), payload);
        byte[] response = taskExecutor.executeCommand(executorHandle, command);
        NativeCommandBuilder.requireSuccess(response, operation);
    }

    private static ThreadPoolExecutor newReaderExecutor(int threads) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                threads, threads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(Math.multiplyExact(threads, 4)),
                task -> {
                    Thread thread = new Thread(
                            task, "external-shuffle-reader-"
                                    + READER_THREAD_ID.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
        executor.prestartAllCoreThreads();
        return executor;
    }

    private record PendingExternalInput(
            ExchangeInput input, RemoteShuffleExchangeSource source) {}

}
