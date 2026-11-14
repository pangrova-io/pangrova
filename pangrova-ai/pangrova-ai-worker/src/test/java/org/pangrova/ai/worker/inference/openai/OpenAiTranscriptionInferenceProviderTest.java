package org.pangrova.ai.worker.inference.openai;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.http.chat.OpenAiInferenceConfig;
import org.pangrova.ai.provider.openai.inference.OpenAiTranscriptionInferenceProvider;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.invocation.ContentPart;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.ai.api.invocation.AiProviderCallContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiTranscriptionInferenceProviderTest {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void uploadsVerifiedAudioAsMultipartAndPreservesText() throws Exception {
        byte[] audio = {0, 1, 2, 3, 4, 5};
        BlobReference reference = reference(audio, "audio/wav");
        AtomicReference<String> idempotencyKey = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/audio/transcriptions", exchange -> {
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            assertThat(contentType).startsWith("multipart/form-data; boundary=pangrova-");
            idempotencyKey.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            byte[] request = exchange.getRequestBody().readAllBytes();
            String envelope = new String(request, StandardCharsets.ISO_8859_1);
            assertThat(envelope).contains(
                    "name=\"model\"\r\n\r\nwhisper-pinned\r\n",
                    "name=\"prompt\"\r\n\r\nTranscribe accurately\r\n",
                    "name=\"response_format\"\r\n\r\njson\r\n",
                    "name=\"file\"; filename=\"audio-");
            assertThat(envelope).contains("Content-Type: audio/wav\r\n\r\n");
            assertThat(contains(request, audio)).isTrue();
            respond(exchange, "{\"text\":\"hello world\",\"usage\":{"
                    + "\"input_tokens\":20,\"output_tokens\":2,"
                    + "\"total_tokens\":22}}");
        });
        server.start();

        try (var provider = provider(ignored -> audio.clone())) {
            var result = org.pangrova.ai.worker.invocation.AiTransportTestAdmission.inference(() -> AiProviderCallContext.callWith(
                    "transcription-invocation", Instant.now().plusSeconds(10),
                    () -> provider.completeMultimodal(
                            MultimodalPrompt.of(
                                    "Transcribe accurately",
                                    new ContentPart.FileRef(reference)),
                            GenerationParameters.DEFAULTS)));
            assertThat(result.text()).isEqualTo("hello world");
            assertThat(result.providerRequestId()).isEmpty();
        }
        assertThat(idempotencyKey).hasValue("transcription-invocation:request-0");
    }

    @Test
    void rejectsNonAudioFileWithoutCallingProvider() throws Exception {
        byte[] document = {1, 2, 3};
        BlobReference reference = reference(document, "application/pdf");
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();

        try (var provider = provider(ignored -> document.clone())) {
            assertThatThrownBy(() -> provider.completeMultimodal(
                    MultimodalPrompt.of(
                            "transcribe", new ContentPart.FileRef(reference)),
                    GenerationParameters.DEFAULTS))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiFailureCategory.CAPABILITY_MISMATCH);
        }
    }

    @Test
    void rejectsUnsupportedGenerationParametersBeforeLoadingAudio() throws Exception {
        byte[] audio = {1, 2, 3};
        BlobReference reference = reference(audio, "audio/wav");
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        AtomicBoolean loaded = new AtomicBoolean();
        var parameters = new GenerationParameters(
                java.util.OptionalDouble.empty(),
                OptionalInt.of(10),
                Optional.empty());

        try (var provider = provider(ignored -> {
            loaded.set(true);
            return audio.clone();
        })) {
            assertThatThrownBy(() -> provider.completeMultimodal(
                    MultimodalPrompt.of(
                            "transcribe", new ContentPart.FileRef(reference)),
                    parameters))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiFailureCategory.CAPABILITY_MISMATCH);
        }
        assertThat(loaded).isFalse();
    }

    private OpenAiTranscriptionInferenceProvider provider(
            java.util.function.Function<BlobReference, byte[]> loader) {
        return new OpenAiTranscriptionInferenceProvider(
                new OpenAiInferenceConfig(
                        "http://localhost:" + server.getAddress().getPort()
                                + "/v1/audio/transcriptions",
                        "whisper-pinned", "test-key", 1, 100.0, 2, 5),
                loader);
    }

    private static boolean contains(byte[] outer, byte[] inner) {
        for (int index = 0; index <= outer.length - inner.length; index++) {
            boolean equal = true;
            for (int offset = 0; offset < inner.length; offset++) {
                equal &= outer[index + offset] == inner[offset];
            }
            if (equal) {
                return true;
            }
        }
        return false;
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
