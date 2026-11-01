package org.pangrova.shuffle.service.transport;

import io.grpc.stub.StreamObserver;

import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;

/** No-op request sink returned after the response side has rejected admission. */
public final class RejectedWriteStream implements StreamObserver<WriteBatchRequest> {
    @Override
    public void onNext(WriteBatchRequest value) {}

    @Override
    public void onError(Throwable failure) {}

    @Override
    public void onCompleted() {}
}
