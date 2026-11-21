package org.pangrova.ai.worker.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QwenProviderRegistryTest {
    private static final String PATH = "/compatible-mode/v1/chat/completions";

    @Test
    void configuredQwenUsesGovernedChatTransportAndDeploymentIdentity() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var request = new AtomicReference<String>();
        server.createContext(PATH, exchange -> {
            try (exchange) {
                request.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                var bytes = """
                        {"choices":[{"message":{"content":"answer"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}
                        """.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
        try {
            var config = deployment("http://127.0.0.1:" + server.getAddress().getPort() + PATH);
            config.put("models.chat.authentication", "none");
            config.remove("models.chat.secret-reference");
            try (var registry = new ConfiguredAiProviderRegistry(config, ref -> { throw new AssertionError(ref); })) {
                var model = registry.resolveInference("chat");
                assertThat(model.modelName()).isEqualTo("chat");
                var result = AiTransportTestAdmission.inference(() -> model.complete("question", GenerationParameters.DEFAULTS));
                assertThat(result.text()).isEqualTo("answer");
                var body = new ObjectMapper().readTree(request.get());
                assertThat(body.path("model").asText()).isEqualTo("qwen-plus");
                assertThat(body.path("messages").get(0).path("content").asText()).isEqualTo("question");
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsWrongEndpointsAndUnsupportedCapabilitiesBeforeAdvertising() {
        var config = deployment("https://models.example/v1/chat/completions");
        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(config)).hasMessageContaining(PATH);
        config.put("models.chat.endpoint", "https://models.example" + PATH);
        config.put("models.chat.capabilities", "embedding");
        config.put("models.chat.embedding-dimension", "512");
        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(config))
                .hasMessageContaining("requires TEXT inference");
    }

    private static Map<String, String> deployment(String endpoint) {
        var config = new HashMap<>(AiProviderTestConfigurations.inferenceModelConfig());
        config.put("models.chat.provider", "qwen");
        config.put("models.chat.provider-model", "qwen-plus");
        config.put("models.chat.endpoint", endpoint);
        return config;
    }
}
