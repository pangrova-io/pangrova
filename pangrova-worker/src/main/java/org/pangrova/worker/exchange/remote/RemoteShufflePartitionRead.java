package org.pangrova.worker.exchange.remote;

import java.util.ArrayDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;

/** Bounded, demand-driven, cancellable external-shuffle partition stream. */
@ThreadSafe
public final class RemoteShufflePartitionRead implements AutoCloseable {

    private static final int MAX_BUFFERED_FRAMES = 2;
    private static final int RPC_EXIT_TIMEOUT_SECONDS = 5;
    private final Object readState = new Object();
    @GuardedBy("readState")
    private final ArrayDeque<ReadBatchResponse> frames = new ArrayDeque<>(MAX_BUFFERED_FRAMES);
    private final CountDownLatch rpcExited = new CountDownLatch(1);
    private volatile ClientCallStreamObserver<ReadPartitionRequest> call;
    @GuardedBy("readState")
    private boolean closed;
    @GuardedBy("readState")
    private boolean completed;
    @GuardedBy("readState")
    private Throwable terminalError;

    RemoteShufflePartitionRead(
            ShuffleServiceGrpc.ShuffleServiceStub stub,
            ReadPartitionRequest request) {
        stub.readPartition(request, new ReaderObserver());
    }

    /** Returns the next frame, or {@code null} after clean RPC completion. */
    public ReadBatchResponse next() {
        final ReadBatchResponse frame;
        try {
            frame = awaitFrame();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            revoke();
            throw new RemoteShuffleClientException(
                    "External shuffle read interrupted", failure);
        }
        if (frame != null) {
            ClientCallStreamObserver<ReadPartitionRequest> activeCall = call;
            if (activeCall != null && acceptsFrames()) {
                activeCall.request(1);
            }
        }
        return frame;
    }

    private ReadBatchResponse awaitFrame() throws InterruptedException {
        synchronized (readState) {
            while (frames.isEmpty() && !closed && !completed) {
                readState.wait();
            }
            if (closed) {
                throw readFailure();
            }
            return frames.pollFirst();
        }
    }

    private boolean acceptsFrames() {
        synchronized (readState) {
            return !closed && !completed;
        }
    }

    @Override
    public void close() {
        revoke();
        awaitRpcExit();
        synchronized (readState) {
            frames.clear();
            readState.notifyAll();
        }
    }

    /** Revokes inbound ownership immediately; cancellation is never clean EOF. */
    public void revoke() {
        synchronized (readState) {
            if (completed) {
                return;
            }
            closed = true;
            frames.clear();
            readState.notifyAll();
        }
        cancelCall();
    }

    private void cancelCall() {
        ClientCallStreamObserver<ReadPartitionRequest> activeCall = call;
        if (activeCall != null) {
            activeCall.cancel("external shuffle consumer closed", null);
        }
    }

    private void awaitRpcExit() {
        try {
            if (!rpcExited.await(RPC_EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new RemoteShuffleClientException(
                        "External shuffle RPC did not reach its terminal callback");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RemoteShuffleClientException(
                    "Interrupted while draining external shuffle RPC", failure);
        }
    }

    @GuardedBy("readState")
    private RemoteShuffleClientException readFailure() {
        return new RemoteShuffleClientException("External shuffle read failed",
                terminalError == null
                        ? new CancellationException("External shuffle reader was revoked")
                        : terminalError);
    }

    private final class ReaderObserver
            implements ClientResponseObserver<ReadPartitionRequest, ReadBatchResponse> {

        @Override
        public void beforeStart(
                ClientCallStreamObserver<ReadPartitionRequest> requestStream) {
            requestStream.disableAutoRequestWithInitial(1);
            final boolean revoked;
            synchronized (readState) {
                call = requestStream;
                revoked = closed;
            }
            if (revoked) {
                requestStream.cancel("external shuffle consumer already closed", null);
            }
        }

        @Override
        public void onNext(ReadBatchResponse value) {
            synchronized (readState) {
                if (closed || completed) {
                    return;
                }
                if (frames.size() < MAX_BUFFERED_FRAMES) {
                    frames.addLast(value);
                    readState.notifyAll();
                    return;
                }
                failRead(new IllegalStateException(
                        "external shuffle server violated inbound flow control"));
            }
            cancelCall();
        }

        @Override
        public void onError(Throwable failure) {
            try {
                synchronized (readState) {
                    failRead(failure);
                }
            } finally {
                rpcExited.countDown();
            }
        }

        @Override
        public void onCompleted() {
            try {
                synchronized (readState) {
                    if (!closed) {
                        completed = true;
                    }
                    readState.notifyAll();
                }
            } finally {
                rpcExited.countDown();
            }
        }
    }

    @GuardedBy("readState")
    private void failRead(Throwable failure) {
        terminalError = failure;
        closed = true;
        frames.clear();
        readState.notifyAll();
    }
}
