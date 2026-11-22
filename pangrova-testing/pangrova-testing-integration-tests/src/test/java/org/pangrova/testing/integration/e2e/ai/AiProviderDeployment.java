package org.pangrova.testing.integration.e2e.ai;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/** Selects a real isolated model deployment; prepared artifacts retain exact identities. */
record AiProviderDeployment(GenericContainer<?> container, Map<String, String> expectedModels) {
    static final String IMAGE_PROPERTY = "pangrova.e2e.ai.image-id";
    static final String LABEL_PREFIX = "org.pangrova.ai-fixture.";
    static final int INFERENCE_THREADS = 2;
    static final int CONTEXT_LENGTH = 512;
    static final int MAX_INPUT_TOKENS = 256;

    AiProviderDeployment {
        expectedModels = Map.copyOf(expectedModels);
    }

    static AiProviderDeployment configured() {
        return select(System.getProperty(IMAGE_PROPERTY));
    }

    static AiProviderDeployment select(String image) {
        if (image == null) {
            throw new IllegalArgumentException("Real model fixture is required: run "
                    + ".pangrova/scripts/prepare-e2e-ai-image.py and set -D" + IMAGE_PROPERTY);
        }
        if (!image.matches("sha256:[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Prepared AI image must be an immutable SHA-256 image ID");
        }
        var actual = DockerClientFactory.instance().client().inspectImageCmd(image).exec();
        if (!image.equals(actual.getId()) || actual.getConfig() == null) {
            throw new IllegalStateException("Prepared AI image differs from its immutable identity");
        }
        Map<String, String> expected = expectedModels(actual.getConfig().getLabels());
        var container = new GenericContainer<>(DockerImageName.parse(image))
                .withEnv("OLLAMA_NUM_PARALLEL", "1")
                .withEnv("OLLAMA_MAX_LOADED_MODELS", "1")
                .withEnv("OLLAMA_CONTEXT_LENGTH", Integer.toString(CONTEXT_LENGTH))
                .withExposedPorts(11434)
                .waitingFor(Wait.forHttp("/api/tags").forStatusCode(200));
        return new AiProviderDeployment(container, expected);
    }

    static Map<String, String> expectedModels(Map<String, String> labels) {
        if (labels == null || !"0.6.2".equals(labels.get(LABEL_PREFIX + "ollama-version"))
                || !validImage(labels.get(LABEL_PREFIX + "base-image"))) {
            throw new IllegalStateException("Prepared AI image has no exact base deployment receipt");
        }
        if (!Integer.toString(INFERENCE_THREADS).equals(labels.get(LABEL_PREFIX + "inference-threads"))) {
            throw new IllegalStateException("Prepared AI image lacks the fixed inference thread receipt");
        }
        return Map.of(SharedAiProvider.PROVIDER_MODEL, digest(labels, "completion-digest"),
                SharedAiProvider.EMBEDDING_PROVIDER_MODEL, digest(labels, "embedding-digest"));
    }

    private static boolean validImage(String identity) {
        return identity != null && identity.matches("sha256:[0-9a-f]{64}");
    }

    private static String digest(Map<String, String> labels, String key) {
        String value = labels.get(LABEL_PREFIX + key);
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalStateException("Prepared AI image lacks exact " + key);
        }
        return value;
    }
}
