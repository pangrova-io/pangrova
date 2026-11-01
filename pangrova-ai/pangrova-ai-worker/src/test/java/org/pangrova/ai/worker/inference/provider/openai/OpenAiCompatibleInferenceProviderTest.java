/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.worker.inference.provider.openai;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.invocation.ContentPart;
import org.pangrova.spi.ai.invocation.GenerationParameters;
import org.pangrova.spi.ai.invocation.MultimodalPrompt;
import org.pangrova.ai.worker.context.AiExecutionContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiCompatibleInferenceProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void sendsExactProviderDeploymentAndPreservesResponse() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            var request = MAPPER.readTree(exchange.getRequestBody());
            assertThat(request.path("model").asText())
                    .isEqualTo("provider-deployment:immutable-v7");
            assertThat(request.path("messages").get(0).path("content").asText())
                    .isEqualTo("hello");
            assertThat(exchange.getRequestHeaders().getFirst("Authorization"))
                    .isEqualTo("Bearer test-key");
            respond(exchange,
                    "{\"choices\":[{\"message\":{\"content\":\"world\"}}],"
                            + "\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}");
        });
        server.start();

        try (var provider = provider("provider-deployment:immutable-v7", "test-key")) {
            var result = complete(provider, "hello");
            assertThat(result.text()).isEqualTo("world");
            assertThat(result.providerRequestId()).isEmpty();
        }
    }

    @Test
    void anonymousEndpointOmitsAuthorizationHeader() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isNull();
            respond(exchange,
                    "{\"choices\":[{\"message\":{\"content\":\"ok\"}}],"
                            + "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}");
        });
        server.start();

        try (var provider = provider("local-pinned", "")) {
            assertThat(complete(provider, "ping").text())
                    .isEqualTo("ok");
        }
    }

    @Test
    void optionalUsageStatisticsDoNotInvalidateTheCompletionContract() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/chat/completions", exchange -> respond(exchange,
                "{\"choices\":[{\"message\":{\"content\":\"world\"}}],"
                        + "\"usage\":{\"prompt_tokens\":3}}"));
        server.start();

        try (var provider = provider("provider-deployment", "test-key")) {
            assertThat(complete(provider, "hello").text()).isEqualTo("world");
        }
    }

    @Test
    void classifiesAuthenticationFailureWithoutRetryingIt() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            byte[] body = "denied".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(401, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();

        try (var provider = provider("provider-deployment", "bad-key")) {
            assertThatThrownBy(() -> complete(provider, "hello"))
                    .isInstanceOf(AiInvocationException.class)
                    .satisfies(failure -> {
                        AiInvocationException invocation =
                                (AiInvocationException) failure;
                        assertThat(invocation.category()).isEqualTo(
                                AiInvocationException.Category.AUTHENTICATION);
                        assertThat(invocation.retryable()).isFalse();
                    });
        }
    }

    @Test
    void loadsAndVerifiesGovernedImageBeforeProviderEgress() throws Exception {
        byte[] image = {1, 2, 3, 4};
        AtomicReference<BlobReference> loaded = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            var request = MAPPER.readTree(exchange.getRequestBody());
            assertThat(request.path("messages").get(0).path("content").get(1)
                    .path("image_url").path("url").asText())
                    .isEqualTo("data:image/png;base64,AQIDBA==");
            respond(exchange,
                    "{\"choices\":[{\"message\":{\"content\":\"described\"}}],"
                            + "\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}");
        });
        server.start();
        Function<BlobReference, byte[]> loader = reference -> {
            loaded.set(reference);
            return image.clone();
        };
        var reference = new BlobReference(
                "pangrova-blob://v2/"
                        + "a".repeat(64) + "/" + "c".repeat(64)
                        + "/" + sha256(image),
                "image/png", image.length, sha256(image));
        var context = org.pangrova.ai.worker.context.AiTestTaskAuthority.context(
                "tenant-a", "alice", "query-1", "task-1", "trace-1",
                Instant.now().plusSeconds(60).toEpochMilli(), 1024);

        try (var provider = provider(
                "provider-deployment", "test-key", Optional.of(loader))) {
            var result = org.pangrova.ai.worker.invocation.AiTransportTestAdmission.inference(() ->
                    AiExecutionContext.callWith(context, () ->
                    provider.completeMultimodal(
                            MultimodalPrompt.of(
                                    "describe",
                                    new ContentPart.FileRef(reference)),
                            GenerationParameters.DEFAULTS)));
            assertThat(result.text()).isEqualTo("described");
        }
        assertThat(loaded).hasValue(reference);
    }

    @Test
    void rejectsGovernedContentWhoseChecksumDoesNotMatch() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        var reference = new BlobReference(
                "pangrova-blob://v2/" + "a".repeat(64) + "/" + "c".repeat(64)
                        + "/" + "b".repeat(64),
                "image/png", 4, "b".repeat(64));

        try (var provider = provider(
                "provider-deployment", "test-key",
                Optional.of(ignored -> new byte[] {1, 2, 3, 4}))) {
            assertThatThrownBy(() -> provider.completeMultimodal(
                    MultimodalPrompt.of("describe", new ContentPart.FileRef(reference)),
                    GenerationParameters.DEFAULTS))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiInvocationException.Category.AUTHORIZATION);
        }
    }

    @Test
    void rejectsNonImageFileInsteadOfEncodingItAsImageUrl() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        byte[] document = {1, 2, 3, 4};
        var reference = new BlobReference(
                "pangrova-blob://v2/" + "a".repeat(64) + "/" + "c".repeat(64)
                        + "/" + sha256(document),
                "application/pdf", document.length, sha256(document));

        try (var provider = provider(
                "provider-deployment", "test-key",
                Optional.of(ignored -> document.clone()))) {
            assertThatThrownBy(() -> provider.completeMultimodal(
                    MultimodalPrompt.of("parse", new ContentPart.FileRef(reference)),
                    GenerationParameters.DEFAULTS))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiInvocationException.Category.CAPABILITY_MISMATCH);
        }
    }

    private OpenAiCompatibleInferenceProvider provider(String model, String apiKey) {
        return provider(model, apiKey, Optional.empty());
    }

    private static org.pangrova.spi.ai.invocation.InferenceResult complete(
            OpenAiCompatibleInferenceProvider provider, String prompt) {
        return org.pangrova.ai.worker.invocation.AiTransportTestAdmission.inference(
                () -> provider.complete(prompt, GenerationParameters.DEFAULTS));
    }

    private OpenAiCompatibleInferenceProvider provider(
            String model,
            String apiKey,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        return new OpenAiCompatibleInferenceProvider(
                new OpenAiInferenceConfig(
                        "http://localhost:" + server.getAddress().getPort()
                                + "/v1/chat/completions",
                        model, apiKey, 8, 100.0, 4, 10),
                blobLoader);
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
