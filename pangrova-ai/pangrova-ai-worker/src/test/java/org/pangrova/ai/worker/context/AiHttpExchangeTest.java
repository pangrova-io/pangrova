package org.pangrova.ai.worker.context;

import org.pangrova.ai.http.AiHttpExchange;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiHttpExchangeTest {
    @Test
    void revokeBetweenRegistrationAndSendWaitsForActualFinalizationAndPreventsSubmission() throws Exception {
        var registered = new CountDownLatch(1);
        var cancelled = new CountDownLatch(1);
        var finishAllowed = new CountDownLatch(1);
        var exchangeRef = new AtomicReference<AiHttpExchange>();
        var threads = Executors.newFixedThreadPool(2);
        try (var budget = AiTransportTestAdmission.embedding(3, 1, 1)) {
            var sender = threads.submit(() -> {
                try (var attempt = budget.openAttempt(1)) {
                    var exchange = new AiHttpExchange(attempt);
                    exchangeRef.set(exchange);
                    try (var registration = attempt.registerExchange(exchange)) {
                        registered.countDown();
                        try {
                            assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
                            assertThatThrownBy(() -> exchange.start(HttpClient.newHttpClient(),
                                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:1/never-sent"))
                                            .POST(HttpRequest.BodyPublishers.ofString("x")).build()))
                                    .isInstanceOf(IOException.class).hasMessageContaining("before transport submission");
                            assertThat(finishAllowed.await(5, TimeUnit.SECONDS)).isTrue();
                        } finally {
                            attempt.close();
                            exchange.finish();
                        }
                    }
                }
                return null;
            });
            assertThat(registered.await(5, TimeUnit.SECONDS)).isTrue();
            // The real cancellation method is idempotent; registry revoke must still join its owner.
            exchangeRef.get().cancel();
            cancelled.countDown();
            var revoker = threads.submit(budget::cancel);
            assertThatThrownBy(() -> revoker.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            finishAllowed.countDown();
            sender.get(5, TimeUnit.SECONDS);
            revoker.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> budget.openAttempt(1)).hasMessageContaining("revoked");
        } finally {
            cancelled.countDown();
            finishAllowed.countDown();
            threads.shutdownNow();
            assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void sameSenderCannotMistakeCancellationForItsOwnExit() {
        try (var budget = AiTransportTestAdmission.embedding(3, 1, 1);
                var attempt = budget.openAttempt(1)) {
            var exchange = new AiHttpExchange(attempt);
            try (var registration = attempt.registerExchange(exchange)) {
                assertThatThrownBy(budget::cancel).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("cannot join its own exchange");
                attempt.close();
                exchange.finish();
                budget.cancel();
            }
        }
    }
}
