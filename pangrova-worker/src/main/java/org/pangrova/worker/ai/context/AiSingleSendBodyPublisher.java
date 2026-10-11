package org.pangrova.worker.ai.context;

import org.pangrova.spi.ai.AiInvocationException;

import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/** Prevents transparent HTTP entity replay after the only admitted provider send. */
final class AiSingleSendBodyPublisher implements HttpRequest.BodyPublisher {
    private final HttpRequest.BodyPublisher delegate;
    private final AtomicBoolean subscribed = new AtomicBoolean();

    AiSingleSendBodyPublisher(HttpRequest.BodyPublisher delegate) {
        this.delegate = delegate;
    }

    @Override
    public long contentLength() {
        return delegate.contentLength();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        if (subscribed.compareAndSet(false, true)) {
            delegate.subscribe(subscriber);
            return;
        }
        subscriber.onSubscribe(
                new Flow.Subscription() {
                    @Override
                    public void request(long count) {}

                    @Override
                    public void cancel() {}
                });
        subscriber.onError(
                new AiInvocationException(
                        AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                        false,
                        "HTTP transport attempted an ungoverned entity replay"));
    }
}
