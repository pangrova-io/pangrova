package org.pangrova.worker.exchange.remote;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CancellationException;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;

/** Bounded, demand-driven, cancellable external-shuffle partition stream. */
public final class RemoteShufflePartitionRead implements AutoCloseable {

    private static final Object COMPLETED = new Object();
    private static final Object REVOKED = new Object();
    private final BlockingQueue<Object> events = new ArrayBlockingQueue<>(2);
    private final CountDownLatch rpcExited = new CountDownLatch(1);
    private volatile ClientCallStreamObserver<ReadPartitionRequest> call;
    private volatile boolean closed;
    private volatile boolean completed;
    private volatile Throwable terminalError;

    RemoteShufflePartitionRead(
            ShuffleServiceGrpc.ShuffleServiceStub stub,
            ReadPartitionRequest request) {
        stub.readPartition(request, new ReaderObserver());
    }

    /** Returns the next frame, or {@code null} after clean RPC completion. */
    public ReadBatchResponse next() {
        if (closed && !completed) {
            throw readFailure();
        }
        if (events.isEmpty() && terminalError != null) {
            throw new RemoteShuffleClientException(
                    "External shuffle read failed", terminalError);
        }
        if (events.isEmpty() && completed) {
            return null;
        }
        final Object event;
        try {
            event = events.take();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            revoke();
            throw new RemoteShuffleClientException(
                    "External shuffle read interrupted", failure);
        }
        if (event == REVOKED || (closed && !completed)) {
            throw readFailure();
        }
        if (event == COMPLETED) {
            return null;
        }
        if (event instanceof Throwable failure) {
            throw new RemoteShuffleClientException(
                    "External shuffle read failed", failure);
        }
        ClientCallStreamObserver<ReadPartitionRequest> activeCall = call;
        if (activeCall != null && !closed) {
            activeCall.request(1);
        }
        return (ReadBatchResponse) event;
    }

    @Override
    public void close() {
        revoke();
        awaitRpcExit();
        events.clear();
    }

    /** Revokes inbound ownership immediately; cancellation is never clean EOF. */
    public void revoke() {
        if (completed) return;
        closed = true;
        ClientCallStreamObserver<ReadPartitionRequest> activeCall = call;
        if (activeCall != null) {
            activeCall.cancel("external shuffle consumer closed", null);
        }
        events.clear();
        events.offer(REVOKED);
    }

    private void awaitRpcExit() {
        try {
            if (!rpcExited.await(5, TimeUnit.SECONDS)) {
                throw new RemoteShuffleClientException(
                        "External shuffle RPC did not reach its terminal callback");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RemoteShuffleClientException(
                    "Interrupted while draining external shuffle RPC", failure);
        }
    }

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
            call = requestStream;
            requestStream.disableAutoRequestWithInitial(1);
            if (closed) {
                requestStream.cancel("external shuffle consumer already closed", null);
            }
        }

        @Override
        public void onNext(ReadBatchResponse value) {
            if (closed) return;
            if (!events.offer(value)) {
                closeWithFailure(new IllegalStateException(
                        "external shuffle server violated inbound flow control"));
                revoke();
            }
        }

        @Override
        public void onError(Throwable failure) {
            try {
                closeWithFailure(failure);
            } finally {
                rpcExited.countDown();
            }
        }

        @Override
        public void onCompleted() {
            try {
                if (!closed) {
                    completed = true;
                    events.offer(COMPLETED);
                }
            } finally {
                rpcExited.countDown();
            }
        }

        private void closeWithFailure(Throwable failure) {
            terminalError = failure;
            closed = true;
            events.offer(failure);
        }
    }
}
