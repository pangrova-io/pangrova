package org.pangrova.ai.control.config;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.model.AiModelDescriptor;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Blob-capable models need explicit tenant consent and an endpoint host allowlist. */
class AiEgressGovernanceTest {

    private static Map<String, String> visionModel(String endpoint, String egressHosts) {
        var properties = new LinkedHashMap<String, String>();
        properties.put("models.vision.name", "vision");
        properties.put("models.vision.version", "2026-01-01");
        properties.put("models.vision.provider", "openai-responses");
        properties.put("models.vision.provider-model", "gpt-4o");
        properties.put("models.vision.endpoint", endpoint);
        properties.put("models.vision.capabilities", "inference,multimodal");
        properties.put("models.vision.modalities", "TEXT,IMAGE");
        properties.put("models.vision.authentication", "none");
        properties.put("models.vision.max-batch-size", "1");
        properties.put("models.vision.max-input-tokens", "8192");
        properties.put("models.vision.max-output-tokens", "1024");
        properties.put("models.vision.max-media-bytes", "1048576");
        properties.put("models.vision.max-concurrency", "1");
        properties.put("models.vision.max-provider-items-per-task", "16");
        properties.put("models.vision.max-provider-attempts-per-task", "32");
        properties.put("models.vision.max-request-bytes", "262144");
        properties.put("models.vision.max-response-bytes", "1048576");
        if (egressHosts != null) {
            properties.put("models.vision.authorization.egress-hosts", egressHosts);
        }
        return properties;
    }

    private static AiModelDescriptor parse(Map<String, String> properties) {
        return AiModelConfigParser.parse(properties, true).get("vision");
    }

    @Test
    void blobCapableModelWithoutEgressHostsFailsClosed() {
        assertThatThrownBy(() -> parse(visionModel("https://api.example.com/v1/responses", null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authorization.egress-hosts");
    }

    @Test
    void blobCapableModelRejectsAnUnlistedHost() {
        assertThatThrownBy(() -> parse(visionModel(
                "https://api.example.com/v1/responses", "other.example.com")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api.example.com");
    }

    @Test
    void exactHostGrantsMediaEgressPermission() {
        AiModelDescriptor model = parse(visionModel(
                "https://api.example.com/v1/responses", "api.example.com"));

        assertThat(model.capabilities())
                .allSatisfy(capability -> assertThat(capability.requiredPermissions())
                        .contains("AI_MODEL_USAGE"));
        assertThat(model.capabilities())
                .anySatisfy(capability -> assertThat(capability.requiredPermissions())
                        .contains("AI_MEDIA_EGRESS"));
    }

    @Test
    void wildcardHostAllowsSubdomains() {
        AiModelDescriptor model = parse(visionModel(
                "https://vision.example.com/v1/responses", "*.example.com"));

        assertThat(model.capabilities()).isNotEmpty();
    }

    @Test
    void loopbackMediaEndpointNeedsNoEgressHosts() {
        AiModelDescriptor model = parse(visionModel(
                "https://localhost/v1/responses", null));

        assertThat(model.capabilities()).isNotEmpty();
    }

    @Test
    void textOnlyModelKeepsOnlyTheUsagePermission() {
        var properties = new LinkedHashMap<>(visionModel(
                "https://api.example.com/v1/responses", "api.example.com"));
        properties.put("models.vision.capabilities", "inference");
        properties.remove("models.vision.modalities");
        properties.put("models.vision.max-media-bytes", "0");

        AiModelDescriptor model = parse(properties);

        assertThat(model.capabilities())
                .allSatisfy(capability -> assertThat(capability.requiredPermissions())
                        .containsExactly("AI_MODEL_USAGE"));
    }
}
