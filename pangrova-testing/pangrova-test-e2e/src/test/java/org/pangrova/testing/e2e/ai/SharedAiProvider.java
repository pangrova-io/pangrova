package org.pangrova.testing.e2e.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.exception.NotFoundException;
import org.pangrova.worker.jni.NativeFunctionConfig;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Owns the real model deployment for the lifetime of the shared Native callbacks. */
public final class SharedAiProvider implements AutoCloseable {
    public static final String PROVIDER_MODEL = "tinyllama:latest";
    public static final String LOGICAL_MODEL = "local-tinyllama";
    public static final String EMBEDDING_PROVIDER_MODEL = "all-minilm:latest";
    public static final String EMBEDDING_LOGICAL_MODEL = "local-all-minilm";
    public static final int EMBEDDING_DIMENSION = 384;
    public static final String QUERY_PREFIX = "Pangrova retrieval query: ";
    public static final String DOCUMENT_PREFIX = "Pangrova stored document: ";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration DEPLOYMENT_TIMEOUT = Duration.ofMinutes(3);
    private static final int MAX_RESPONSE_BYTES = 64 * 1024;
    private static final int MAX_DIAGNOSTIC_CHARACTERS = 16 * 1024;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10)).build();
    private final GenericContainer<?> deployment;
    private final Map<String, String> expectedModels;
    private final Duration deploymentTimeout;
    private final StringBuilder diagnosticTail = new StringBuilder();
    private GenericContainer<?> container;
    private NativeFunctionConfig config;

    /** Creates one isolated deployment with a single three-minute setup budget. */
    public SharedAiProvider() {
        this(AiProviderDeployment.configured(), DEPLOYMENT_TIMEOUT);
    }

    /** Owns the actual test deployment; fault tests may shorten, but never enlarge, the budget. */
    SharedAiProvider(GenericContainer<?> deployment, Duration timeout) {
        this(new AiProviderDeployment(Objects.requireNonNull(deployment, "deployment"), Map.of()), timeout);
    }

    SharedAiProvider(AiProviderDeployment selected, Duration timeout) {
        this.deployment = selected.container();
        this.expectedModels = selected.expectedModels();
        this.deploymentTimeout = Objects.requireNonNull(timeout, "timeout");
        if (deployment.getContainerId() != null) {
            throw new IllegalArgumentException("Model deployment must not borrow an already-created container");
        }
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(DEPLOYMENT_TIMEOUT) > 0) {
            throw new IllegalArgumentException("Model deployment budget must be positive and at most three minutes");
        }
    }

    /** Starts actual models and freezes their deployment identities before Native initialization. */
    public NativeFunctionConfig prepare() throws Exception {
        if (config != null) {
            return config;
        }
        if (container != null) {
            throw new IllegalStateException("A previous model deployment still requires cleanup");
        }
        long deadline = System.nanoTime() + deploymentTimeout.toNanos();
        container = deployment;
        String phase = "container startup";
        try {
            container.withStartupTimeout(remaining(deadline));
            container.withLogConsumer(frame -> retainDiagnostic(frame.getUtf8String()));
            container.start();
            phase = "immutable model inventory";
            String digest = loadedModelDigest(PROVIDER_MODEL, deadline);
            String embeddingDigest = loadedModelDigest(EMBEDDING_PROVIDER_MODEL, deadline);
            for (var actual : Map.of(PROVIDER_MODEL, digest, EMBEDDING_PROVIDER_MODEL, embeddingDigest).entrySet()) {
                String expected = expectedModels.get(actual.getKey());
                if (expected != null && !expected.equals(actual.getValue())) {
                    throw new IllegalStateException("Prepared AI model identity changed: " + actual.getKey());
                }
            }
            remaining(deadline);
            config = configuration(digest, embeddingDigest);
            return config;
        } catch (Exception | Error failure) {
            failure.addSuppressed(new IllegalStateException(
                    "Actual Ollama deployment failed during " + phase + "; container diagnostics: " + diagnostics()));
            try {
                close();
            } catch (Exception | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private String loadedModelDigest(String providerModel, long deadline) throws Exception {
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(endpoint("/api/tags")))
                .timeout(remaining(deadline)).GET().build();
        HttpResponse<byte[]> response = send(request, deadline);
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Ollama model inventory returned HTTP " + response.statusCode()
                    + ": " + new String(response.body(), StandardCharsets.UTF_8));
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
        throw new IllegalStateException("Prepared Ollama fixture is missing model " + providerModel);
    }

    private HttpResponse<byte[]> send(HttpRequest request, long deadline) throws Exception {
        var response = http.sendAsync(request, info -> new BoundedResponse());
        try {
            return response.get(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException failure) {
            response.cancel(true);
            throw failure;
        } catch (InterruptedException failure) {
            response.cancel(true);
            Thread.currentThread().interrupt();
            throw failure;
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof Exception cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw failure;
        }
    }

    private static Duration remaining(long deadline) throws TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) throw new TimeoutException("The shared Ollama deployment deadline expired");
        return Duration.ofNanos(nanos);
    }

    private synchronized void retainDiagnostic(String message) {
        diagnosticTail.append(message);
        if (diagnosticTail.length() > MAX_DIAGNOSTIC_CHARACTERS) {
            diagnosticTail.delete(0, diagnosticTail.length() - MAX_DIAGNOSTIC_CHARACTERS);
        }
    }

    private synchronized String diagnostics() {
        return diagnosticTail.toString();
    }

    /** Retains bounded actual deployment evidence on inference failures before owned cleanup. */
    public String deploymentDiagnostics() {
        requirePreparedContainer();
        return diagnostics();
    }

    /** Bounds the complete HTTP entity before retaining it, including error responses. */
    private static final class BoundedResponse implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> delegate = HttpResponse.BodySubscribers.ofByteArray();
        private Flow.Subscription subscription;
        private int received;
        private boolean terminal;

        @Override
        public CompletionStage<byte[]> getBody() { return delegate.getBody(); }

        @Override
        public void onSubscribe(Flow.Subscription subscribed) {
            subscription = subscribed;
            delegate.onSubscribe(subscribed);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (terminal) return;
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > MAX_RESPONSE_BYTES - received) {
                    terminal = true;
                    subscription.cancel();
                    delegate.onError(new IOException("Ollama deployment response exceeds " + MAX_RESPONSE_BYTES
                            + " bytes after receiving " + received + " bytes"));
                    return;
                }
                received += buffer.remaining();
            }
            delegate.onNext(buffers);
        }

        @Override
        public void onError(Throwable failure) {
            if (!terminal) {
                terminal = true;
                delegate.onError(failure);
            }
        }

        @Override
        public void onComplete() {
            if (!terminal) {
                terminal = true;
                delegate.onComplete();
            }
        }
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
        config.put(prefix + "max-input-tokens", Integer.toString(AiProviderDeployment.MAX_INPUT_TOKENS));
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
        boolean interrupted = Thread.interrupted();
        try {
            if (container != null) {
                String ownedId = container.getContainerId();
                if (ownedId != null) {
                    removeOwnedContainer(ownedId);
                }
                container.stop();
                container = null;
                config = null;
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private static void removeOwnedContainer(String ownedId) {
        var docker = DockerClientFactory.instance().client();
        try {
            docker.removeContainerCmd(ownedId).withForce(true).withRemoveVolumes(true).exec();
        } catch (NotFoundException alreadyRemoved) {
            // Absence of the exact owned container is the same terminal cleanup state.
        }
        try {
            docker.inspectContainerCmd(ownedId).exec();
            throw new IllegalStateException("Owned Ollama container still exists after deletion: " + ownedId);
        } catch (NotFoundException removed) {
            // Confirm actual deletion before retiring the handle or clearing its configuration.
        }
    }
}
