package org.pangrova.ai.worker.inference.provider.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.invocation.ContentPart;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.ai.worker.context.AiProviderCallContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiResponsesInferenceProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void sendsExactImageAndDocumentPartsWithGovernedContent() throws Exception {
        byte[] image = {1, 2, 3, 4};
        byte[] document = "%PDF-test".getBytes(StandardCharsets.US_ASCII);
        BlobReference imageRef = reference(image, "image/png");
        BlobReference documentRef = reference(document, "application/pdf");
        AtomicReference<String> idempotencyKey = new AtomicReference<>();
        server = server(exchange -> {
            idempotencyKey.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            JsonNode request = MAPPER.readTree(exchange.getRequestBody());
            JsonNode content = request.path("input").get(0).path("content");
            assertThat(request.path("model").asText()).isEqualTo("gpt-responses-pinned");
            assertThat(content.get(0).path("type").asText()).isEqualTo("input_text");
            assertThat(content.get(1).path("type").asText()).isEqualTo("input_image");
            assertThat(content.get(1).path("image_url").asText())
                    .isEqualTo("data:image/png;base64,AQIDBA==");
            assertThat(content.get(2).path("type").asText()).isEqualTo("input_file");
            assertThat(content.get(2).path("filename").asText()).endsWith(".pdf");
            assertThat(content.get(2).path("file_data").asText())
                    .startsWith("data:application/pdf;base64,");
            respond(exchange, "{\"output\":[{\"type\":\"message\",\"content\":["
                    + "{\"type\":\"output_text\",\"text\":\"extracted\"}]}],"
                    + "\"usage\":{\"input_tokens\":11,\"output_tokens\":3,"
                    + "\"total_tokens\":14}}");
        });
        server.start();

        try (var provider = provider(reference -> {
            if (reference.equals(imageRef)) {
                return image.clone();
            }
            return document.clone();
        })) {
            var prompt = new MultimodalPrompt(java.util.List.of(
                    new ContentPart.Text("extract"),
                    new ContentPart.FileRef(imageRef),
                    new ContentPart.FileRef(documentRef)));
            var result = org.pangrova.ai.worker.invocation.AiTransportTestAdmission.inference(() -> AiProviderCallContext.callWith(
                    "stable-invocation", Instant.now().plusSeconds(10),
                    () -> provider.completeMultimodal(
                            prompt, GenerationParameters.DEFAULTS)));
            assertThat(result.text()).isEqualTo("extracted");
            assertThat(result.providerRequestId()).isEmpty();
        }
        assertThat(idempotencyKey).hasValue("stable-invocation:request-0");
    }

    @Test
    void rejectsUnverifiedDocumentBeforeProviderEgress() throws Exception {
        byte[] document = "%PDF-test".getBytes(StandardCharsets.US_ASCII);
        BlobReference reference = reference(document, "application/pdf");
        server = server(exchange -> {
            throw new AssertionError("provider endpoint must not be called");
        });
        server.start();

        try (var provider = provider(ignored -> new byte[] {9})) {
            assertThatThrownBy(() -> provider.completeMultimodal(
                    MultimodalPrompt.of("parse", new ContentPart.FileRef(reference)),
                    GenerationParameters.DEFAULTS))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiInvocationException.Category.AUTHORIZATION);
        }
    }

    @Test
    void rejectsUnknownApplicationMimeBeforeLoadingOrEgress() throws Exception {
        byte[] content = {1, 2, 3};
        BlobReference reference = reference(content, "application/octet-stream");
        AtomicBoolean loaded = new AtomicBoolean();
        server = server(exchange -> {
            throw new AssertionError("provider endpoint must not be called");
        });
        server.start();

        try (var provider = provider(ignored -> {
            loaded.set(true);
            return content.clone();
        })) {
            assertThatThrownBy(() -> provider.completeMultimodal(
                    MultimodalPrompt.of("parse", new ContentPart.FileRef(reference)),
                    GenerationParameters.DEFAULTS))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiInvocationException.Category.CAPABILITY_MISMATCH);
        }
        assertThat(loaded).isFalse();
    }

    @Test
    void doesNotReplayPostAfterProviderReceivesRequestAndDisconnects() throws Exception {
        AtomicInteger received = new AtomicInteger();
        server = server(exchange -> {
            exchange.getRequestBody().readAllBytes();
            received.incrementAndGet();
            exchange.close();
        });
        server.start();

        try (var provider = provider(ignored -> new byte[0])) {
            assertThatThrownBy(() -> org.pangrova.ai.worker.invocation.AiTransportTestAdmission
                    .inference(() -> AiProviderCallContext.callWith(
                            "disconnect-invocation", Instant.now().plusSeconds(10),
                            () -> provider.completeMultimodal(
                                    MultimodalPrompt.ofText("hello"), GenerationParameters.DEFAULTS))))
                    .isInstanceOf(AiInvocationException.class)
                    .satisfies(failure -> {
                        AiInvocationException invocation = (AiInvocationException) failure;
                        assertThat(invocation.category()).isEqualTo(
                                AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
                        assertThat(invocation.retryable()).isFalse();
                    });
        }
        assertThat(received).hasValue(1);
    }

    @Test
    void doesNotReplayPostAfterProviderReturnsServiceUnavailable() throws Exception {
        AtomicInteger received = new AtomicInteger();
        server = server(exchange -> {
            exchange.getRequestBody().readAllBytes();
            received.incrementAndGet();
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        server.start();

        try (var provider = provider(ignored -> new byte[0])) {
            assertThatThrownBy(() -> org.pangrova.ai.worker.invocation.AiTransportTestAdmission
                    .inference(() -> AiProviderCallContext.callWith(
                            "unavailable-invocation", Instant.now().plusSeconds(10),
                            () -> provider.completeMultimodal(
                                    MultimodalPrompt.ofText("hello"), GenerationParameters.DEFAULTS))))
                    .isInstanceOf(AiInvocationException.class)
                    .satisfies(failure -> {
                        AiInvocationException invocation = (AiInvocationException) failure;
                        assertThat(invocation.category()).isEqualTo(
                                AiInvocationException.Category.PROVIDER_UNAVAILABLE);
                        assertThat(invocation.retryable()).isFalse();
                    });
        }
        assertThat(received).hasValue(1);
    }

    private OpenAiResponsesInferenceProvider provider(
            java.util.function.Function<BlobReference, byte[]> loader) {
        return new OpenAiResponsesInferenceProvider(
                new OpenAiInferenceConfig(
                        "http://localhost:" + server.getAddress().getPort() + "/v1/responses",
                        "gpt-responses-pinned", "test-key", 4, 100.0, 2, 5),
                Optional.of(loader));
    }

    private HttpServer server(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        HttpServer result = HttpServer.create(new InetSocketAddress(0), 0);
        result.createContext("/v1/responses", handler);
        return result;
    }

    private static BlobReference reference(byte[] content, String mimeType) {
        String checksum = sha256(content);
        return new BlobReference(
                "pangrova-blob://v2/" + "a".repeat(64) + "/" + "c".repeat(64)
                        + "/" + checksum,
                mimeType, content.length, checksum);
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new AssertionError(failure);
        }
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }
}
