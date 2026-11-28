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

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.common.api.annotation.Experimental;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;

import org.pangrova.ai.api.model.AiModelDescriptor;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Computes the canonical public binding identity for an embedding deployment. */
@Experimental
public final class EmbeddingModelBindingIdentity {

    private static final String CONTRACT_VERSION = "pangrova.ai.embedding.binding.v1";

    private EmbeddingModelBindingIdentity() {}

    /** Computes the catalog-side identity without exposing credential material. */
    public static byte[] sha256(EmbeddingModelDescriptor model) {
        if (model == null) {
            throw new IllegalArgumentException("model must not be null");
        }
        return sha256(
                new ModelRoute(
                        model.provider(), model.endpoint(), model.apiKeySecret()),
                new EmbeddingServingContract(
                        model.dimension(), model.maxBatchSize(),
                        Integer.toString(model.maxRequestsPerSecond()),
                        model.queryPrefix(), model.documentPrefix()));
    }

    /** Computes the Worker-side identity from one canonical configured model. */
    public static byte[] sha256(AiModelDescriptor model) {
        if (model == null) {
            throw new IllegalArgumentException("model must not be null");
        }
        return sha256(
                new ModelRoute(
                        model.provider(), model.endpoint().toString(),
                        model.secretReference()),
                new EmbeddingServingContract(
                        positiveInt(model, "embedding-dimension"),
                        positiveInt(model, "max-batch-size"),
                        required(model, "max-requests-per-second"),
                        model.properties().getOrDefault("query-prefix", ""),
                        model.properties().getOrDefault("document-prefix", "")));
    }

    private static byte[] sha256(
            ModelRoute route, EmbeddingServingContract serving) {
        MessageDigest digest = Sha256Digests.sha256();
        update(digest, CONTRACT_VERSION);
        update(digest, requireText(route.provider(), "provider")
                .toLowerCase(Locale.ROOT));
        update(digest, canonicalEndpoint(route.endpoint()));
        update(digest, secretReferenceDigest(route.secretReference()));
        update(digest, Integer.toString(
                requirePositive(serving.dimension(), "dimension")));
        update(digest, Integer.toString(
                requirePositive(serving.maxBatchSize(), "maxBatchSize")));
        update(digest, canonicalPositiveDecimal(
                serving.maxRequestsPerSecond()));
        update(digest, requireNonNull(serving.queryPrefix(), "queryPrefix"));
        update(digest, requireNonNull(
                serving.documentPrefix(), "documentPrefix"));
        return digest.digest();
    }

    private static String canonicalEndpoint(String value) {
        URI endpoint = URI.create(requireText(value, "endpoint")).normalize();
        if (endpoint.getScheme() == null || endpoint.getHost() == null
                || endpoint.getUserInfo() != null) {
            throw new IllegalArgumentException(
                    "endpoint must be an absolute network URI without user-info");
        }
        return endpoint.toASCIIString();
    }

    private static String secretReferenceDigest(String value) {
        MessageDigest digest = Sha256Digests.sha256();
        // An explicitly unauthenticated Worker deployment has no secret
        // reference. Hash the canonical empty value instead of refusing to
        // advertise an otherwise valid model capability.
        digest.update(requireNonNull(value, "secretReference")
                .getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static int positiveInt(AiModelDescriptor model, String property) {
        try {
            return requirePositive(Integer.parseInt(required(model, property)), property);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "model property must be an integer: " + property, failure);
        }
    }

    private static String required(AiModelDescriptor model, String property) {
        return requireText(model.properties().get(property), property);
    }

    private static String canonicalPositiveDecimal(String value) {
        try {
            BigDecimal decimal = new BigDecimal(requireText(
                    value, "maxRequestsPerSecond")).stripTrailingZeros();
            if (decimal.signum() <= 0) {
                throw new IllegalArgumentException(
                        "maxRequestsPerSecond must be positive");
            }
            return decimal.toPlainString();
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "maxRequestsPerSecond must be numeric", failure);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static String requireNonNull(String value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private record ModelRoute(
            String provider,
            String endpoint,
            String secretReference) {}

    private record EmbeddingServingContract(
            int dimension,
            int maxBatchSize,
            String maxRequestsPerSecond,
            String queryPrefix,
            String documentPrefix) {}
}
