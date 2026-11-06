package org.pangrova.ai.worker.context;

import org.pangrova.ai.spi.invocation.PhysicalAttempt;

import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Charges delivered entity buffers before the accumulating subscriber retains them. */
final class AiBoundedBodySubscriber implements HttpResponse.BodySubscriber<String> {
    private final PhysicalAttempt attempt;
    private final HttpResponse.BodySubscriber<String> delegate =
            HttpResponse.BodySubscribers.mapping(
                    HttpResponse.BodySubscribers.ofByteArray(),
                    bytes -> new String(bytes, StandardCharsets.UTF_8));
    private Flow.Subscription subscription;
    private boolean finished;
    private Thread delivering;

    AiBoundedBodySubscriber(PhysicalAttempt attempt) {
        this.attempt = attempt;
    }

    @Override
    public CompletionStage<String> getBody() {
        return delegate.getBody();
    }

    @Override
    public synchronized void onSubscribe(Flow.Subscription value) {
        if (subscription != null || finished) {
            value.cancel();
            return;
        }
        subscription = value;
        delegate.onSubscribe(value);
    }

    @Override
    public synchronized void onNext(List<ByteBuffer> buffers) {
        if (finished) {
            return;
        }
        delivering = Thread.currentThread();
        try {
            long bytes = 0;
            for (ByteBuffer buffer : buffers) {
                bytes = Math.addExact(bytes, buffer.remaining());
            }
            attempt.acceptResponseBytes(bytes);
            delegate.onNext(buffers);
        } catch (RuntimeException failure) {
            finished = true;
            subscription.cancel();
            delegate.onError(failure);
        } finally {
            delivering = null;
        }
    }

    synchronized boolean deliveringOnCurrentThread() {
        return delivering == Thread.currentThread();
    }

    @Override
    public synchronized void onError(Throwable failure) {
        if (!finished) {
            finished = true;
            delegate.onError(failure);
        }
    }

    /** Seals delivery before the sending thread releases the physical admission. */
    synchronized void abort(Throwable failure) {
        if (!finished) {
            finished = true;
            if (subscription != null) {
                subscription.cancel();
            }
            delegate.onError(failure);
        }
    }

    @Override
    public synchronized void onComplete() {
        if (!finished) {
            finished = true;
            delegate.onComplete();
        }
    }
}
