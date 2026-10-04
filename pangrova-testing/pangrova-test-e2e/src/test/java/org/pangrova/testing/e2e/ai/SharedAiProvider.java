package org.pangrova.testing.e2e.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.worker.jni.NativeFunctionConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** Owns the real model deployment for the lifetime of the shared Native callbacks. */
public final class SharedAiProvider implements AutoCloseable {
    public static final String PROVIDER_MODEL = "tinyllama";
    public static final String LOGICAL_MODEL = "local-tinyllama";
    public static final String EMBEDDING_PROVIDER_MODEL = "all-minilm:latest";
    public static final String EMBEDDING_LOGICAL_MODEL = "local-all-minilm";
    public static final int EMBEDDING_DIMENSION = 384;
    public static final String QUERY_PREFIX = "Pangrova retrieval query: ";
    public static final String DOCUMENT_PREFIX = "Pangrova stored document: ";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10)).build();
    private GenericContainer<?> container;
    private NativeFunctionConfig config;

    /** Starts actual models and freezes their deployment identities before Native initialization. */
    public NativeFunctionConfig prepare() throws Exception {
        if (config != null) {
            return config;
        }
        if (container != null) {
            throw new IllegalStateException("A previous model deployment still requires cleanup");
        }
        container = new GenericContainer<>(DockerImageName.parse("ollama/ollama:0.6.2"))
                .withExposedPorts(11434)
                .waitingFor(Wait.forHttp("/api/tags").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        try {
            container.start();
            pull(PROVIDER_MODEL);
            pull(EMBEDDING_PROVIDER_MODEL);
            config = configuration(loadedModelDigest(PROVIDER_MODEL),
                    loadedModelDigest(EMBEDDING_PROVIDER_MODEL));
            return config;
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (Exception | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private void pull(String model) throws Exception {
        var pull = container.execInContainer("ollama", "pull", model);
        if (pull.getExitCode() != 0) {
            throw new IllegalStateException("Ollama model pull failed for " + model + ": " + pull.getStderr());
        }
    }

    private String loadedModelDigest(String providerModel) throws Exception {
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(endpoint("/api/tags")))
                .timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Ollama model inventory returned HTTP " + response.statusCode());
        }
        JsonNode models = MAPPER.readTree(response.body()).path("models");
        for (JsonNode model : models) {
            if (providerModel.equals(model.path("name").asText())
                    || providerModel.equals(model.path("model").asText())) {
                String digest = model.path("digest").asText();
                if (!digest.matches("(?:sha256:)?[a-fA-F0-9]{64}")) {
                    throw new IllegalStateException("Ollama did not provide an immutable digest for " + providerModel);
                }
                return digest;
            }
        }
        throw new IllegalStateException("Ollama did not list pulled model " + providerModel);
    }

    private NativeFunctionConfig configuration(String digest, String embeddingDigest) {
        Map<String, String> model = new LinkedHashMap<>();
        addModel(model, "local", LOGICAL_MODEL, PROVIDER_MODEL, digest,
                "/v1/chat/completions", "inference");
        model.put("models.local.max-output-tokens", "128");
        addModel(model, "embedding", EMBEDDING_LOGICAL_MODEL, EMBEDDING_PROVIDER_MODEL,
                embeddingDigest, "/v1/embeddings", "embedding");
        model.put("models.embedding.embedding-dimension", Integer.toString(EMBEDDING_DIMENSION));
        model.put("models.embedding.query-prefix", QUERY_PREFIX);
        model.put("models.embedding.document-prefix", DOCUMENT_PREFIX);
        model.put("authorization.mode", "authenticated");
        model.put("authorization.allowed-models", LOGICAL_MODEL + "," + EMBEDDING_LOGICAL_MODEL);
        model.put("authorization.allowed-capabilities",
                "inference:" + LOGICAL_MODEL + ",embedding:" + EMBEDDING_LOGICAL_MODEL);
        model.put("authorization.granted-permissions", "AI_MODEL_USAGE");
        model.put("authorization.max-batch-size", "8");
        model.put("authorization.max-input-bytes", "1048576");
        return new NativeFunctionConfig(true, Map.of("ai-inference", Map.copyOf(model)));
    }

    private void addModel(Map<String, String> config, String id, String logicalModel,
                          String providerModel, String digest, String path, String capability) {
        String prefix = "models." + id + ".";
        config.put(prefix + "name", logicalModel);
        config.put(prefix + "provider", "openai-compatible");
        config.put(prefix + "provider-model", providerModel);
        config.put(prefix + "version", digest);
        config.put(prefix + "endpoint", endpoint(path));
        config.put(prefix + "authentication", "none");
        config.put(prefix + "capabilities", capability);
        config.put(prefix + "max-batch-size", "8");
        config.put(prefix + "max-requests-per-second", "32");
        config.put(prefix + "timeout-seconds", "120");
        config.put(prefix + "max-input-tokens", "2048");
        config.put(prefix + "max-concurrency", "2");
        config.put(prefix + "max-provider-items-per-task", "16");
        config.put(prefix + "max-provider-attempts-per-task", "16");
        config.put(prefix + "max-request-bytes", "1048576");
        config.put(prefix + "max-response-bytes", "1048576");
    }

    /** Returns the real mapped endpoint, never a constant deployment address. */
    public String endpoint(String path) {
        requirePreparedContainer();
        return "http://" + container.getHost() + ":" + container.getMappedPort(11434) + path;
    }

    /** Counts actual provider HTTP access log entries. */
    public long requestCount(String path) {
        requirePreparedContainer();
        return container.getLogs().lines().filter(line -> line.contains(path)).count();
    }

    private void requirePreparedContainer() {
        if (container == null || !container.isRunning()) {
            throw new IllegalStateException("The shared real model deployment is not running");
        }
    }

    /** Stops only after the caller has retired all Native and Java callback owners. */
    @Override
    public void close() {
        if (container != null) {
            container.stop();
            container = null;
            config = null;
        }
    }
}
