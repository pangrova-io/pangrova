package org.pangrova.worker.exchange.remote;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;

/** Bounded, demand-driven, cancellable external-shuffle partition stream. */
public final class RemoteShufflePartitionRead implements AutoCloseable {

    private static final Object COMPLETED = new Object();
    private final BlockingQueue<Object> events = new ArrayBlockingQueue<>(2);
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
        if (events.isEmpty() && terminalError != null) {
            throw new RemoteShuffleClientException(
                    "External shuffle read failed", terminalError);
        }
        if (events.isEmpty() && (closed || completed)) {
            return null;
        }
        final Object event;
        try {
            event = events.take();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            close();
            throw new RemoteShuffleClientException(
                    "External shuffle read interrupted", failure);
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
        if (closed) {
            return;
        }
        closed = true;
        ClientCallStreamObserver<ReadPartitionRequest> activeCall = call;
        if (activeCall != null) {
            activeCall.cancel("external shuffle consumer closed", null);
        }
        events.offer(COMPLETED);
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
            if (!events.offer(value)) {
                closeWithFailure(new IllegalStateException(
                        "external shuffle server violated inbound flow control"));
            }
        }

        @Override
        public void onError(Throwable failure) {
            closeWithFailure(failure);
        }

        @Override
        public void onCompleted() {
            completed = true;
            events.offer(COMPLETED);
        }

        private void closeWithFailure(Throwable failure) {
            terminalError = failure;
            closed = true;
            events.offer(failure);
        }
    }
}
