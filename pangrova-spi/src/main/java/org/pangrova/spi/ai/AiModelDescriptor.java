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
package org.pangrova.spi.ai;

import org.pangrova.spi.annotation.Experimental;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Canonical, versioned model object used by both inference and embedding. */
@Experimental
public record AiModelDescriptor(
        String name,
        String version,
        String provider,
        URI endpoint,
        String secretReference,
        List<AiCapabilityDescriptor> capabilities,
        boolean enabled,
        Map<String, String> properties) {

    public AiModelDescriptor {
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        requireText(name, "name");
        requireText(version, "version");
        requireText(provider, "provider");
        requireEndpoint(endpoint);
        secretReference = canonicalSecretReference(secretReference, properties);
        capabilities = copyCapabilities(capabilities);
    }

    private static void requireEndpoint(URI endpoint) {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        if (endpoint.getScheme() == null || endpoint.getHost() == null || endpoint.getUserInfo() != null) {
            throw new IllegalArgumentException("endpoint must be an absolute network URI without user-info");
        }
    }

    private static String canonicalSecretReference(String secretReference, Map<String, String> properties) {
        String authentication = properties.getOrDefault("authentication", "bearer");
        if (!authentication.equals("bearer") && !authentication.equals("none")) {
            throw new IllegalArgumentException(
                    "authentication must be canonical 'bearer' or 'none'");
        }
        if (!authentication.equals("none")) {
            requireText(secretReference, "secretReference");
            return secretReference;
        }
        if (secretReference != null && !secretReference.isBlank()) {
            throw new IllegalArgumentException("authentication=none must not declare a secretReference");
        }
        return "";
    }

    private static List<AiCapabilityDescriptor> copyCapabilities(List<AiCapabilityDescriptor> capabilities) {
        List<AiCapabilityDescriptor> copy = List.copyOf(Objects.requireNonNull(
                capabilities, "capabilities must not be null"));
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("capabilities must not be empty");
        }
        return copy;
    }

    /**
     * Resolves the exact capability required by a planned invocation.
     *
     * @param operation required operation
     * @param capabilityVersion required capability version
     * @return the matching capability descriptor
     * @throws AiInvocationException if the model does not advertise the capability
     */
    public AiCapabilityDescriptor requireCapability(
            AiCapabilityDescriptor.Operation operation, String capabilityVersion) {
        return capabilities.stream()
                .filter(capability -> capability.operation() == operation)
                .filter(capability -> capability.version().equals(capabilityVersion))
                .findFirst()
                .orElseThrow(() -> new AiInvocationException(
                        AiInvocationException.Category.CAPABILITY_MISMATCH,
                        false,
                        "Model '" + name + "' version '" + version
                                + "' does not provide " + operation
                                + " capability version '" + capabilityVersion + "'"));
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
