package org.pangrova.worker.ai.context;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.embedding.EmbeddingBatchRequest;
import org.pangrova.spi.ai.embedding.EmbeddingPurpose;
import org.pangrova.worker.ai.embedding.provider.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.worker.ai.embedding.provider.OpenAiEmbeddingConfig;
import org.pangrova.worker.ai.invocation.AiTransportTestAdmission;

import java.net.InetSocketAddress;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual HTTP stream faults, without claiming a model-inference result. */
class AiHttpBodyDeadlineTest {
    @Test
    void stalledBodyAfterHeadersExpiresWithoutReplayAndReleasesThePhysicalPermit() throws Exception {
        assertStalledBodyTerminates(false, false);
    }

    @Test
    void interruptedBodySealsDeliveryAndReleasesOnlyTheClientPermit() throws Exception {
        assertStalledBodyTerminates(true, false);
    }

    @Test
    void taskRevocationJoinsAnAlreadySentBodyWithoutWaitingForItsDeadline() throws Exception {
        assertStalledBodyTerminates(false, true);
    }

    private static void assertStalledBodyTerminates(boolean interrupt, boolean revoke) throws Exception {
        var bodyStarted = new CountDownLatch(1);
        var releaseBody = new CountDownLatch(1);
        var sends = new AtomicInteger();
        var sendingThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var wasInterrupted = new java.util.concurrent.atomic.AtomicBoolean();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var serverThreads = Executors.newFixedThreadPool(2);
        var caller = Executors.newSingleThreadExecutor();
        server.setExecutor(serverThreads);
        byte[] body = "{\"data\":[{\"index\":0,\"embedding\":[1,2,3]}]}".getBytes(StandardCharsets.UTF_8);
        server.createContext("/embeddings", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                int ordinal = sends.incrementAndGet();
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body, 0, 1);
                exchange.getResponseBody().flush();
                bodyStarted.countDown();
                if (ordinal == 1 && !releaseBody.await(15, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("test did not release the stalled response");
                }
                exchange.getResponseBody().write(body, 1, body.length - 1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        server.start();
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/embeddings";
        var provider = new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                endpoint, "transport-contract", "", 3, 1, 100, "", "", interrupt || revoke ? 10 : 1));
        var defaults = AiTransportTestAdmission.Limits.defaults();
        var singleSlot = new AiTransportTestAdmission.Limits(1, defaults.items(), defaults.attempts(),
                defaults.requestBytes(), defaults.responseBytes(), defaults.mediaBytes(), defaults.constraints());
        try (var budget = new AiTransportTestAdmission(
                org.pangrova.spi.ai.AiCapabilityDescriptor.Operation.EMBEDDING, 3, 1, 1,
                singleSlot, java.time.Instant.now().plusSeconds(30))) {
            var failed = caller.submit(() -> {
                sendingThread.set(Thread.currentThread());
                try {
                    budget.call("stalled-body", () -> provider.embed(
                            EmbeddingBatchRequest.text(EmbeddingPurpose.RAW, List.of("first"))));
                    throw new AssertionError("stalled body returned success");
                } catch (AiInvocationException failure) {
                    wasInterrupted.set(Thread.currentThread().isInterrupted());
                    return failure;
                }
            });
            assertThat(bodyStarted.await(5, TimeUnit.SECONDS)).isTrue();
            if (interrupt) sendingThread.get().interrupt();
            if (revoke) {
                long started = System.nanoTime();
                budget.cancel();
                assertThat(System.nanoTime() - started).isLessThan(TimeUnit.SECONDS.toNanos(5));
            }
            var failure = failed.get(5, TimeUnit.SECONDS);
            assertThat(failure.category()).isEqualTo(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
            assertThat(failure.retryable()).isFalse();
            if (!revoke) {
                assertThat(failure).hasCauseInstanceOf(interrupt ? InterruptedException.class : HttpTimeoutException.class);
            }
            assertThat(wasInterrupted.get()).isEqualTo(interrupt);
            assertThat(sends).hasValue(1);
            if (revoke) {
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
                return;
            }
            // A new explicitly admitted call can acquire the released model/task permit.
            var recovered = budget.call("next-explicit-call", () -> provider.embed(
                    EmbeddingBatchRequest.text(EmbeddingPurpose.RAW, List.of("second"))));
            assertThat(recovered.vectors().get(0)).containsExactly(1, 2, 3);
            assertThat(sends).hasValue(2);
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(2);
            releaseBody.countDown();
        } finally {
            releaseBody.countDown();
            caller.shutdownNow();
            server.stop(0);
            serverThreads.shutdownNow();
            assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(serverThreads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
