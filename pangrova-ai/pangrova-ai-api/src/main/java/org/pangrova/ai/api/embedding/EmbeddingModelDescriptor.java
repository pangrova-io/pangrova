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
package org.pangrova.ai.api.embedding;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;

import java.net.URI;

/**
 * Immutable descriptor for a registered embedding model configuration.
 *
 * <p>Stored in the catalog's embedding model registry. The {@code apiKeySecret}
 * field holds an opaque reference resolved at runtime through the engine secret
 * resolution port; actual credentials are never persisted in catalog metadata.</p>
 *
 * @param name                 unique model name (must not be null or blank)
 * @param route                provider route and opaque credential reference
 * @param contract             exact vector output and text-prefix contract
 * @param limits               provider batching and rate limits
 * @param version              configuration version, incremented on ALTER (must be non-negative)
 * @param deployment           exact Worker deployment fence selected at catalog registration
 */
@Experimental
public record EmbeddingModelDescriptor(
        String name,
        ProviderRoute route,
        EmbeddingContract contract,
        ServingLimits limits,
        int version,
        DeploymentFence deployment) {

    public EmbeddingModelDescriptor {
        requireText(name, "name");
        requireNonNull(route, "route");
        requireNonNull(contract, "contract");
        requireNonNull(limits, "limits");
        requireNonNull(deployment, "deployment");
        if (version < 0) {
            throw new IllegalArgumentException("version must be non-negative");
        }
    }

    /** Returns whether this catalog revision is fenced to one exact Worker deployment. */
    public boolean isDeploymentBound() {
        return deployment.isBound();
    }

    public String provider() { return route.provider(); }
    public String endpoint() { return route.endpoint(); }
    public String apiKeySecret() { return route.apiKeySecret(); }
    public int dimension() { return contract.dimension(); }
    public String queryPrefix() { return contract.queryPrefix(); }
    public String documentPrefix() { return contract.documentPrefix(); }
    public int maxBatchSize() { return limits.maxBatchSize(); }
    public int maxRequestsPerSecond() { return limits.maxRequestsPerSecond(); }
    public String deploymentVersion() { return deployment.version(); }
    public String deploymentSignatureSha256() { return deployment.signatureSha256(); }

    public record ProviderRoute(String provider, String endpoint, String apiKeySecret) {
        public ProviderRoute {
            requireText(provider, "provider");
            requireText(endpoint, "endpoint");
            requireCredential(apiKeySecret);
            URI uri = URI.create(endpoint);
            requireEndpointAddress(uri);
            requireEndpointTransport(uri, apiKeySecret);
        }

        private static void requireCredential(String apiKeySecret) {
            requireNonNull(apiKeySecret, "apiKeySecret");
            if (!apiKeySecret.isEmpty() && apiKeySecret.isBlank()) {
                throw new IllegalArgumentException(
                        "apiKeySecret must be a non-blank reference or canonical empty value");
            }
        }

        private static void requireEndpointAddress(URI uri) {
            if (!uri.isAbsolute() || uri.getHost() == null || uri.getUserInfo() != null
                    || uri.getQuery() != null || uri.getFragment() != null) {
                throw new IllegalArgumentException(
                        "endpoint must be an absolute URI without user-info, query or fragment");
            }
        }

        private static void requireEndpointTransport(URI uri, String apiKeySecret) {
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    && !(apiKeySecret.isEmpty() && "http".equalsIgnoreCase(uri.getScheme())
                            && isLoopback(uri.getHost()))) {
                throw new IllegalArgumentException(
                        "endpoint must use HTTPS; HTTP is allowed only for an anonymous loopback deployment");
            }
        }

        private static boolean isLoopback(String host) {
            return "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host)
                    || "::1".equals(host) || "[::1]".equals(host);
        }

        /** A canonical empty credential reference denotes explicit no-auth deployment. */
        public boolean unauthenticated() {
            return apiKeySecret.isEmpty();
        }
    }

    public record EmbeddingContract(
            int dimension, String queryPrefix, String documentPrefix) {
        public EmbeddingContract {
            requirePositive(dimension, "dimension");
            requireNonNull(queryPrefix, "queryPrefix");
            requireNonNull(documentPrefix, "documentPrefix");
        }
    }

    public record ServingLimits(int maxBatchSize, int maxRequestsPerSecond) {
        public ServingLimits {
            requirePositive(maxBatchSize, "maxBatchSize");
            requirePositive(maxRequestsPerSecond, "maxRequestsPerSecond");
        }
    }

    public record DeploymentFence(String version, String signatureSha256) {
        public DeploymentFence {
            requireNonNull(version, "deploymentVersion");
            requireNonNull(signatureSha256, "deploymentSignatureSha256");
            boolean hasVersion = !version.isBlank();
            boolean hasSignature = !signatureSha256.isBlank();
            if (hasVersion != hasSignature) {
                throw new IllegalArgumentException(
                        "deployment version and signature must be present together");
            }
            if (hasSignature && !DigestUtils.isSha256Hex(signatureSha256)) {
                throw new IllegalArgumentException(
                        "deploymentSignatureSha256 must be 64 lowercase hexadecimal characters");
            }
        }

        public static DeploymentFence unbound() { return new DeploymentFence("", ""); }
        public boolean isBound() { return !version.isBlank(); }
    }

    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " required");
        }
    }

    private static void requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
    }
}
