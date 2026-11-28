package org.pangrova.worker.exchange;

import org.pangrova.shuffle.api.enums.ShuffleReadFailureReason;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.google.protobuf.Message;
import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.ConnectExchangeSourceRequest;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.FinishExchangeSourceRequest;
import org.pangrova.protocol.v2.NoMoreExchangeSourcesRequest;
import org.pangrova.protocol.v2.PushExchangeBatchRequest;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.shuffle.api.exception.ShuffleReadException;
import org.pangrova.shuffle.nativewire.NativeExchangeAttempts;
import org.pangrova.shuffle.nativewire.NativeShuffleReaders;
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
public final class NativeExchangeTransport
        implements ExchangeTransport<TaskAttemptId, ExchangeInputBinding>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeExchangeTransport.class);
    private static final int DEFAULT_READER_THREADS = Math.max(
            2, Math.min(32, Runtime.getRuntime().availableProcessors() * 2));
    private static final AtomicInteger READER_THREAD_ID = new AtomicInteger();

    private final TaskExecutor taskExecutor;
    private final NativeShuffleReaders readers;
    private final ThreadPoolExecutor readerExecutor;
    private final ConcurrentMap<Long, ExternalShuffleInputSession> sessions =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Creates a direct-exchange transport with no external-shuffle reader. */
    public NativeExchangeTransport(TaskExecutor taskExecutor) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.readers = null;
        this.readerExecutor = null;
    }

    /** Creates a transport that owns the supplied external-shuffle client pool. */
    public NativeExchangeTransport(
            TaskExecutor taskExecutor,
            NativeShuffleReaders readers) {
        this(taskExecutor, readers, DEFAULT_READER_THREADS);
    }

    NativeExchangeTransport(
            TaskExecutor taskExecutor,
            NativeShuffleReaders readers,
            int readerThreads) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.readers = checkNotNull(readers, "readers must not be null");
        if (readerThreads <= 0) {
            throw new IllegalArgumentException("readerThreads must be positive");
        }
        this.readerExecutor = newReaderExecutor(readerThreads);
    }

    @Override
    public void bindInputs(
            long executorHandle,
            TaskAttemptId consumerAttempt,
            List<ExchangeInputBinding> inputs) {
        requireOpenHandle(executorHandle);
        checkNotNull(consumerAttempt, "consumerAttempt must not be null");
        checkNotNull(inputs, "inputs must not be null");
        if (inputs.isEmpty()) {
            return;
        }
        int externalCount = countExternalInputs(inputs);
        requireExternalReader(externalCount);
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
            TaskAttemptId attempt,
            List<ExchangeInputBinding> inputs,
            ExternalShuffleInputSession session) {
        List<PendingExternalInput> pending = connectInputs(handle, attempt, inputs);
        sealInputs(handle, attempt, inputs);
        startExternalReaders(handle, attempt, pending, session);
    }

    private List<PendingExternalInput> connectInputs(
            long handle,
            TaskAttemptId attempt,
            List<ExchangeInputBinding> inputs) {
        List<PendingExternalInput> pending = new ArrayList<>();
        for (ExchangeInputBinding input : inputs) {
            switch (input.getTransportCase()) {
                case DIRECT -> connectDirect(handle, attempt, input);
                case EXTERNAL -> pending.add(connectExternal(handle, attempt, input));
                default -> throw new IllegalArgumentException("Exchange input transport is required");
            }
        }
        return pending;
    }

    private void connectDirect(
            long handle,
            TaskAttemptId attempt,
            ExchangeInputBinding input) {
        for (var source : input.getDirect().getSourcesList()) {
            byte[] command = NativeCommandBuilder.connectDirectExchange(attempt, input, source);
            NativeCommandBuilder.requireSuccess(taskExecutor.executeCommand(handle, command),
                    "connect direct exchange source " + source.getSourceTaskId());
        }
    }

    private PendingExternalInput connectExternal(
            long handle,
            TaskAttemptId attempt,
            ExchangeInputBinding input) {
        var external = input.getExternal();
        var partitions = readers.prepare(external, NativeExchangeAttempts.decode(attempt));
        ConnectExchangeSourceRequest payload = ConnectExchangeSourceRequest.newBuilder()
                .setPlanNodeId(input.getPlanNodeId())
                .setExchangeId(input.getExchangeId())
                .setExternal(external)
                .build();
        execute(handle, attempt,
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE,
                payload, "connect external shuffle " + external.getShuffleId());
        return new PendingExternalInput(input, partitions);
    }

    private void sealInputs(
            long handle,
            TaskAttemptId attempt,
            List<ExchangeInputBinding> inputs) {
        for (ExchangeInputBinding input : inputs) {
            NoMoreExchangeSourcesRequest payload = NoMoreExchangeSourcesRequest.newBuilder()
                    .setPlanNodeId(input.getPlanNodeId())
                    .setExchangeId(input.getExchangeId())
                    .setExpectedSourcePartitions(input.getExpectedSourcePartitions())
                    .build();
            execute(handle, attempt,
                    RuntimeCommandType.RUNTIME_COMMAND_TYPE_NO_MORE_EXCHANGE_SOURCES,
                    payload, "seal exchange input " + input.getPlanNodeId());
        }
    }

    private void startExternalReaders(
            long handle,
            TaskAttemptId attempt,
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
            TaskAttemptId attempt,
            PendingExternalInput pending,
            ExternalShuffleInputSession session) {
        long sequenceBase = 0;
        var input = pending.input();
        var publications = input.getExternal().getPublicationsList();
        try {
            for (int index = 0; index < publications.size() && !session.isClosed(); index++) {
                var publication = publications.get(index);
                long offset = Math.subtractExact(sequenceBase, publication.getStartSequence());
                var source = pending.partitions().open(index);
                try (source) {
                    session.addSource(source);
                    BatchChunk chunk;
                    while (!session.isClosed() && (chunk = source.nextVerifiedChunk()) != null
                            && !session.isClosed()) {
                        pushChunk(handle, attempt, input, chunk, offset);
                    }
                    if (!session.isClosed()
                            && source.lastCompletedSequence() != publication.getLastSequence()) {
                        throw new IllegalStateException("External shuffle partition ended before its publication");
                    }
                }
                session.removeClosedSource(source);
                sequenceBase = Math.addExact(sequenceBase,
                        publication.getLastSequence() - publication.getStartSequence() + 1);
            }
            if (!session.isClosed()) {
                finishInput(handle, attempt, input, sequenceBase - 1, null);
            }
        } catch (RuntimeException failure) {
            finishFailedInput(handle, attempt, input, sequenceBase - 1, failure, session);
        }
    }

    private void pushChunk(
            long handle,
            TaskAttemptId attempt,
            ExchangeInputBinding input,
            BatchChunk chunk,
            long sequenceOffset) {
        var chunkMetadata = chunk.toBuilder().clearData();
        if (sequenceOffset != 0) {
            chunkMetadata.setBatchSequence(Math.addExact(sequenceOffset, chunk.getBatchSequence()));
        }
        PushExchangeBatchRequest payload = PushExchangeBatchRequest.newBuilder()
                .setPlanNodeId(input.getPlanNodeId())
                .setExchangeId(input.getExchangeId())
                .setChunk(chunkMetadata)
                .build();
        var metadata = NativeCommandBuilder.buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH,
                attempt, payload);
        byte[] response = taskExecutor.pushExchangeChunk(handle, metadata, chunk.getData().toByteArray());
        NativeCommandBuilder.requireSuccess(response, "push external exchange batch chunk");
    }

    private void finishFailedInput(
            long handle,
            TaskAttemptId attempt,
            ExchangeInputBinding input,
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
                    handle, input.getPlanNodeId(), finishFailure.getMessage(), failure);
            cancelNativeTask(handle, failure);
        }
    }

    private void finishInput(
            long handle,
            TaskAttemptId attempt,
            ExchangeInputBinding input,
            long lastSequence,
            ExecutionError failure) {
        FinishExchangeSourceRequest.Builder payload = FinishExchangeSourceRequest.newBuilder()
                .setPlanNodeId(input.getPlanNodeId())
                .setExchangeId(input.getExchangeId())
                .setLastSequence(lastSequence);
        if (failure != null) {
            payload.setFailure(failure);
        }
        execute(handle, attempt,
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE,
                payload.build(), "finish external exchange source " + input.getPlanNodeId());
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
        if (readers != null) {
            readers.close();
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
        if (externalCount > 0 && (readers == null || readerExecutor == null)) {
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

    private static int countExternalInputs(List<ExchangeInputBinding> inputs) {
        return Math.toIntExact(inputs.stream()
                .filter(ExchangeInputBinding::hasExternal)
                .count());
    }

    private static ExecutionError toExecutionError(RuntimeException failure) {
        boolean corruption = failure instanceof ShuffleReadException readFailure
                && readFailure.reason() == ShuffleReadFailureReason.CORRUPTION;
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
            TaskAttemptId attempt,
            RuntimeCommandType type,
            Message payload,
            String operation) {
        byte[] command = NativeCommandBuilder.buildCommand(
                type, attempt, payload);
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
            ExchangeInputBinding input, NativeShuffleReaders.PartitionReaders partitions) {}

}
