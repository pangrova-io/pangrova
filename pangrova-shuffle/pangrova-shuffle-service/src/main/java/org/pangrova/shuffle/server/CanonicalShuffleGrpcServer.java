package org.pangrova.shuffle.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.pangrova.protocol.v2.shuffle.CleanupRequest;
import org.pangrova.protocol.v2.shuffle.CleanupResponse;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptResponse;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.protocol.v2.shuffle.HeartbeatRequest;
import org.pangrova.protocol.v2.shuffle.HeartbeatResponse;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.protocol.v2.shuffle.WriteBatchResponse;
import org.pangrova.shuffle.deployment.ShuffleServiceConfig;
import org.pangrova.shuffle.deployment.CoordinatorPeerInterceptor;
import org.pangrova.shuffle.server.error.ShuffleServiceFailure;
import org.pangrova.shuffle.transport.BatchStreamAssembler;
import org.pangrova.shuffle.transport.CompleteWriteBatch;
import org.pangrova.shuffle.transport.RejectedWriteStream;
import org.pangrova.shuffle.transport.SerialExecutor;
import org.pangrova.shuffle.transport.PartitionWriteAccumulator;

import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

/** gRPC boundary for the canonical v2 durable shuffle state machine. */
final class CanonicalShuffleGrpcServer extends ShuffleServiceGrpc.ShuffleServiceImplBase
        implements AutoCloseable {
    private static final int BUFFER_PEAK_MULTIPLIER = 3;

    private final ShuffleServiceEngine engine;
    private final ShuffleServiceConfig.Limits limits;
    private final ExecutorService ioExecutor;
    private final Semaphore writeStreams;
    private final Semaphore memoryBytes;

    CanonicalShuffleGrpcServer(ShuffleServiceEngine engine, ShuffleServiceConfig.Limits limits) {
        this.engine = engine;
        this.limits = limits;
        this.ioExecutor = boundedExecutor(limits);
        this.writeStreams = new Semaphore(limits.maxWriteStreams());
        this.memoryBytes = new Semaphore(limits.maxInflightBytes());
    }

    @Override
    public void registerShuffle(
            RegisterShuffleRequest request,
            StreamObserver<RegisterShuffleResponse> response) {
        dispatchUnary(response, () -> engine.register(request));
    }

    @Override
    public StreamObserver<WriteBatchRequest> writeBatch(
            StreamObserver<WriteBatchResponse> response) {
        if (!(response instanceof ServerCallStreamObserver<WriteBatchResponse> serverResponse)) {
            throw new IllegalStateException("server response observer is required");
        }
        if (!writeStreams.tryAcquire()) {
            response.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("too many concurrent shuffle writes").asRuntimeException());
            return new RejectedWriteStream();
        }
        return new WriteStream(serverResponse);
    }

    @Override
    public void commitProducerAttempt(
            CommitProducerAttemptRequest request,
            StreamObserver<CommitProducerAttemptResponse> response) {
        dispatchUnary(response, () -> engine.commit(request));
    }

    @Override
    public void finalizeShuffle(
            FinalizeShuffleRequest request,
            StreamObserver<FinalizeShuffleResponse> response) {
        dispatchUnary(response, () -> engine.finalizeShuffle(request));
    }

    @Override
    public void readPartition(
            ReadPartitionRequest request,
            StreamObserver<ReadBatchResponse> response) {
        if (!(response instanceof ServerCallStreamObserver<ReadBatchResponse> serverResponse)) {
            response.onError(status(new IllegalStateException(
                    "server response observer is required")));
            return;
        }
        PendingRead pending = new PendingRead(request, serverResponse, Context.current());
        serverResponse.setOnCancelHandler(pending::cancel);
        if (!tryExecute(pending::start)) {
            response.onError(overloaded());
        }
    }

    @Override
    public void cleanup(
            CleanupRequest request,
            StreamObserver<CleanupResponse> response) {
        dispatchUnary(response, () -> engine.cleanup(request));
    }

    @Override
    public void renewAccess(
            RenewAccessRequest request,
            StreamObserver<RenewAccessResponse> response) {
        boolean coordinatorPeer = CoordinatorPeerInterceptor.isCoordinatorPeer();
        dispatchUnary(response, () -> engine.renewAccess(request, coordinatorPeer));
    }

    @Override
    public void heartbeat(
            HeartbeatRequest request,
            StreamObserver<HeartbeatResponse> response) {
        boolean accepted = !request.getWorkerId().isBlank() && request.getTimestampMs() > 0;
        response.onNext(HeartbeatResponse.newBuilder().setAccepted(accepted).build());
        response.onCompleted();
    }

    private <T> void dispatchUnary(StreamObserver<T> response, Operation<T> operation) {
        Context context = Context.current();
        if (!tryExecute(context.wrap(() -> runUnary(response, operation, context)))) {
            response.onError(overloaded());
        }
    }

    private static <T> void runUnary(
            StreamObserver<T> response,
            Operation<T> operation,
            Context context) {
        try {
            T result = operation.run();
            if (!context.isCancelled()) {
                response.onNext(result);
                response.onCompleted();
            }
        } catch (RuntimeException failure) {
            if (!context.isCancelled()) {
                response.onError(status(failure));
            }
        }
    }

    static Throwable status(RuntimeException failure) {
        if (failure instanceof StatusRuntimeException) {
            return failure;
        }
        Status status;
        if (failure instanceof SecurityException) {
            status = Status.PERMISSION_DENIED;
        } else if (failure instanceof IllegalArgumentException) {
            status = Status.INVALID_ARGUMENT;
        } else if (failure instanceof ShuffleServiceFailure serviceFailure) {
            status = switch (serviceFailure.kind()) {
                case NOT_FOUND -> Status.NOT_FOUND;
                case DEADLINE -> Status.DEADLINE_EXCEEDED;
                case CORRUPT -> Status.DATA_LOSS;
            };
        } else if (failure instanceof IllegalStateException) {
            status = Status.FAILED_PRECONDITION;
        } else {
            status = Status.INTERNAL;
        }
        return status.withDescription(failure.getMessage()).withCause(failure).asRuntimeException();
    }

    @FunctionalInterface
    private interface Operation<T> {
        T run();
    }

    private boolean tryExecute(Runnable operation) {
        try {
            ioExecutor.execute(operation);
            return true;
        } catch (RejectedExecutionException rejected) {
            return false;
        }
    }

    static StatusRuntimeException overloaded() {
        return Status.RESOURCE_EXHAUSTED.withDescription(
                "shuffle operation admission queue is full").asRuntimeException();
    }

    static ExecutorService boundedExecutor(ShuffleServiceConfig.Limits limits) {
        AtomicInteger sequence = new AtomicInteger();
        ThreadFactory factory = task -> {
            Thread thread = new Thread(task, "shuffle-io-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return new ThreadPoolExecutor(limits.ioThreads(), limits.ioThreads(), 0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(limits.maxQueuedOperations()), factory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    private final class WriteStream implements StreamObserver<WriteBatchRequest> {
        private final ServerCallStreamObserver<WriteBatchResponse> response;
        private final BatchStreamAssembler assembler = new BatchStreamAssembler(
                limits.maxChunkBytes(), limits.maxBatchBytes());
        private final Map<Integer, PartitionWriteAccumulator> acknowledgements = new HashMap<>();
        private final SerialExecutor serial;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int reservedBytes;
        private boolean inFlight;

        private WriteStream(ServerCallStreamObserver<WriteBatchResponse> response) {
            this.response = response;
            this.serial = new SerialExecutor(ioExecutor, this::fail);
            response.disableAutoRequest();
            response.setOnCancelHandler(this::cancel);
            response.request(1);
        }

        @Override
        public void onNext(WriteBatchRequest request) {
            if (closed.get()) {
                return;
            }
            int bytes = request.getChunk().getData().size();
            if (!reserveBytes(bytes)) {
                fail(Status.RESOURCE_EXHAUSTED.withDescription(
                        "shuffle write memory quota exhausted").asRuntimeException());
                return;
            }
            boolean admitted = serial.execute(() -> {
                markInFlight(true);
                try {
                    if (closed.get()) {
                        return;
                    }
                    CompleteWriteBatch complete = assembler.accept(request);
                    if (complete != null) {
                        StoredBatch stored = engine.acceptBatch(complete);
                        acknowledgements.computeIfAbsent(complete.outputPartitionId(),
                                ignored -> new PartitionWriteAccumulator()).accepted(
                                        stored.identity().batchSequence(), stored.sizeBytes());
                        releaseBytes(complete.canonicalBytes().length);
                    }
                    if (!closed.get()) {
                        response.request(1);
                    }
                } catch (RuntimeException failure) {
                    fail(failure);
                } finally {
                    markInFlight(false);
                }
            });
            if (!admitted) {
                fail(overloaded());
            }
        }

        @Override
        public void onError(Throwable failure) {
            cancel();
        }

        @Override
        public void onCompleted() {
            if (closed.get()) {
                return;
            }
            boolean admitted = serial.execute(() -> {
                try {
                    if (closed.get()) {
                        return;
                    }
                    assembler.requireComplete();
                    WriteBatchResponse.Builder result = WriteBatchResponse.newBuilder();
                    for (Map.Entry<Integer, PartitionWriteAccumulator> entry
                            : acknowledgements.entrySet()) {
                        result.addPartitions(entry.getValue().protocol(entry.getKey()));
                        result.setTotalBatchesAccepted(result.getTotalBatchesAccepted()
                            + entry.getValue().batches());
                        result.setTotalBytesAccepted(result.getTotalBytesAccepted()
                            + entry.getValue().bytes());
                    }
                    response.onNext(result.build());
                    response.onCompleted();
                    closeResources();
                } catch (RuntimeException failure) {
                    fail(failure);
                }
            });
            if (!admitted) {
                fail(overloaded());
            }
        }

        private void fail(RuntimeException failure) {
            if (closed.compareAndSet(false, true)) {
                writeStreams.release();
                response.onError(status(failure));
                releaseIfIdle();
            }
        }

        private void cancel() {
            if (closed.compareAndSet(false, true)) {
                writeStreams.release();
                releaseIfIdle();
            }
        }

        private void closeResources() {
            if (closed.compareAndSet(false, true)) {
                writeStreams.release();
                releaseIfIdle();
            }
        }

        private synchronized void releaseBytes(int bytes) {
            int permits = Math.multiplyExact(bytes, BUFFER_PEAK_MULTIPLIER);
            int released = Math.min(permits, reservedBytes);
            reservedBytes -= released;
            memoryBytes.release(released);
        }

        private synchronized boolean reserveBytes(int bytes) {
            int permits = Math.multiplyExact(bytes, BUFFER_PEAK_MULTIPLIER);
            if (!memoryBytes.tryAcquire(permits)) {
                return false;
            }
            reservedBytes = Math.addExact(reservedBytes, permits);
            return true;
        }

        private synchronized void releaseAllBytes() {
            memoryBytes.release(reservedBytes);
            reservedBytes = 0;
        }

        private synchronized void markInFlight(boolean value) {
            inFlight = value;
            if (!value && closed.get()) {
                releaseAllBytes();
            }
        }

        private synchronized void releaseIfIdle() {
            if (!inFlight) {
                releaseAllBytes();
            }
        }
    }

    private final class PendingRead {
        private final ReadPartitionRequest request;
        private final ServerCallStreamObserver<ReadBatchResponse> response;
        private final Context context;
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile ShuffleReadPump pump;

        private PendingRead(
                ReadPartitionRequest request,
                ServerCallStreamObserver<ReadBatchResponse> response,
                Context context) {
            this.request = request;
            this.response = response;
            this.context = context;
        }

        private void start() {
            if (closed.get() || context.isCancelled()) {
                return;
            }
            try {
                ShuffleReadPump created = new ShuffleReadPump(engine, engine.read(request),
                        response, ioExecutor, memoryBytes, limits);
                pump = created;
                if (closed.get() || context.isCancelled()) {
                    created.cancel();
                    return;
                }
                response.setOnReadyHandler(created::schedule);
                created.schedule();
            } catch (RuntimeException failure) {
                if (!closed.get() && !context.isCancelled()) {
                    response.onError(status(failure));
                }
            }
        }

        private void cancel() {
            closed.set(true);
            ShuffleReadPump current = pump;
            if (current != null) {
                current.cancel();
            }
        }
    }

    @Override
    public void close() {
        ioExecutor.shutdownNow();
    }

}
