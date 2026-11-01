package org.pangrova.spi.ai.model;

import org.pangrova.spi.ai.capability.AiRequestConstraints;
import org.pangrova.spi.ai.capability.Modality;
import org.pangrova.spi.ai.capability.AiCapabilityDescriptor;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiModelDescriptorTest {
    private static final URI ENDPOINT = URI.create("https://api.example.test/v1/embeddings");

    @Test
    void freezesPropertiesAndCapabilitiesWithoutRetainingMutableInputs() {
        var properties = new HashMap<>(Map.of("authentication", "bearer"));
        var capabilities = new ArrayList<>(List.of(capability()));
        var descriptor = model(ENDPOINT, "vault://model", properties, capabilities);
        properties.put("authentication", "none");
        capabilities.clear();
        assertThat(descriptor.properties()).containsEntry("authentication", "bearer");
        assertThat(descriptor.capabilities()).containsExactly(capability());
        assertThatThrownBy(() -> descriptor.properties().put("endpoint", "other"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> descriptor.capabilities().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/relative", "urn:model:opaque", "file:/tmp/model", "https://user:secret@api.test/embed"})
    void rejectsEndpointsWithoutNetworkIdentityOrWithUserInfo(String endpoint) {
        assertThatThrownBy(() -> model(URI.create(endpoint), "vault://model", Map.of(), List.of(capability())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("absolute network URI");
    }

    @Test
    void genericNetworkRouteAndCanonicalBearerRetainTheirExistingContract() {
        var descriptor = model(URI.create("http://localhost:11434/api"), "vault://model", null, List.of(capability()));
        assertThat(descriptor.properties()).isEmpty();
        assertThat(descriptor.secretReference()).isEqualTo("vault://model");
        assertThatThrownBy(() -> model(null, "vault://model", Map.of(), List.of(capability())))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("endpoint");
        for (String secret : Arrays.asList(null, "", " ")) {
            assertThatThrownBy(() -> model(ENDPOINT, secret, Map.of(), List.of(capability())))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("secretReference");
        }
    }

    @Test
    void anonymousAuthenticationCanonicalizesOnlyAbsentCredentialsAndRejectsConflicts() {
        for (String secret : Arrays.asList(null, "", " ")) {
            assertThat(model(ENDPOINT, secret, Map.of("authentication", "none"), List.of(capability())).secretReference())
                    .isEmpty();
        }
        assertThatThrownBy(() -> model(ENDPOINT, "vault://model", Map.of("authentication", "none"), List.of(capability())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must not declare");
        for (String authentication : List.of("NONE", "Bearer", "basic", "")) {
            assertThatThrownBy(() -> model(ENDPOINT, "vault://model", Map.of("authentication", authentication),
                    List.of(capability()))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical");
        }
    }

    @Test
    void rejectsMissingOrCorruptCapabilitiesAndProperties() {
        assertThatThrownBy(() -> model(ENDPOINT, "vault://model", Map.of(), null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("capabilities");
        assertThatThrownBy(() -> model(ENDPOINT, "vault://model", Map.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must not be empty");
        assertThatThrownBy(() -> model(ENDPOINT, "vault://model", Map.of(), Arrays.asList(capability(), null)))
                .isInstanceOf(NullPointerException.class);
        var invalidProperties = new HashMap<String, String>();
        invalidProperties.put("authentication", null);
        assertThatThrownBy(() -> model(ENDPOINT, "vault://model", invalidProperties, List.of(capability())))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void validatesIdentityNamesAndRequiresTheExactCapabilityVersion() {
        for (int index = 0; index < 3; index++) {
            for (String invalid : Arrays.asList(null, "", " ")) {
                var names = new ArrayList<>(List.of("model", "v1", "provider"));
                names.set(index, invalid);
                assertThatThrownBy(() -> new AiModelDescriptor(names.get(0), names.get(1), names.get(2),
                        ENDPOINT, "vault://model", List.of(capability()), true, Map.of()))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must not be blank");
            }
        }
        var descriptor = model(ENDPOINT, "vault://model", Map.of(), List.of(capability()));
        assertThat(descriptor.requireCapability(AiCapabilityDescriptor.Operation.EMBEDDING, "1")).isEqualTo(capability());
        assertThatThrownBy(() -> descriptor.requireCapability(AiCapabilityDescriptor.Operation.EMBEDDING, "2"))
                .isInstanceOf(AiInvocationException.class).hasMessageContaining("does not provide");
    }

    private static AiModelDescriptor model(URI endpoint, String secret, Map<String, String> properties,
            List<AiCapabilityDescriptor> capabilities) {
        return new AiModelDescriptor("model", "v1", "provider", endpoint, secret, capabilities, true, properties);
    }

    private static AiCapabilityDescriptor capability() {
        return new AiCapabilityDescriptor("embedding", "1", AiCapabilityDescriptor.Operation.EMBEDDING,
                List.of(new AiCapabilityDescriptor.Signature("VECTOR(3)", List.of("VARCHAR"))),
                Set.of(AiCapabilityDescriptor.ExecutionMode.SCALAR), Set.of(Modality.TEXT), 1,
                new AiCapabilityDescriptor.ResourceLimits(new AiRequestConstraints(1, 0, 1024, 4096),
                        0, 1, 1, 1), true,
                AiCapabilityDescriptor.RetryPolicy.noRetry(), Set.of());
    }
}
