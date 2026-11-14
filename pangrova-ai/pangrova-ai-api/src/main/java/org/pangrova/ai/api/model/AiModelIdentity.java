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
package org.pangrova.ai.api.model;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/** Computes the stable public identity of an exact AI model execution descriptor. */
@Experimental
public final class AiModelIdentity {

    private static final Set<String> DECLARATIVE_OR_SECRET_PROPERTIES = Set.of(
            "name",
            "provider",
            "version",
            "endpoint",
            "secret-reference",
            "capabilities",
            "modalities",
            "max-batch-size",
            "enabled");

    private AiModelIdentity() {}

    /**
     * Returns a SHA-256 signature over routing and execution semantics. Secret
     * references and secret values are deliberately excluded from the public
     * identity carried in Worker capability inventories.
     */
    public static byte[] signatureSha256(AiModelDescriptor model) {
        if (model == null) {
            throw new IllegalArgumentException("model must not be null");
        }
        MessageDigest digest = DigestUtils.sha256();
        updateModelIdentity(digest, model);
        model.capabilities().stream()
                .sorted(Comparator
                        .comparing(AiCapabilityDescriptor::capabilityId)
                        .thenComparing(AiCapabilityDescriptor::version)
                        .thenComparing(value -> value.operation().name()))
                .forEach(capability -> updateCapability(digest, capability));
        updateProperties(digest, model.properties());
        return digest.digest();
    }

    private static void updateModelIdentity(MessageDigest digest, AiModelDescriptor model) {
        update(digest, "pangrova.ai.model.v2");
        update(digest, model.name());
        update(digest, model.version());
        update(digest, model.provider());
        update(digest, model.endpoint().normalize().toASCIIString());
    }

    private static void updateCapability(
            MessageDigest digest, AiCapabilityDescriptor capability) {
        update(digest, capability.capabilityId());
        update(digest, capability.version());
        update(digest, capability.operation().name());
        update(digest, Integer.toString(capability.maxBatchSize()));
        updateResourceLimits(digest, capability.resourceLimits());
        updateRetryPolicy(digest, capability.retryPolicy());
        update(digest, Boolean.toString(capability.deterministic()));
        capability.executionModes().stream().map(Enum::name).sorted()
                .forEach(value -> update(digest, value));
        capability.modalities().stream().map(Enum::name).sorted()
                .forEach(value -> update(digest, value));
        capability.requiredPermissions().stream().sorted()
                .forEach(value -> update(digest, value));
        capability.signatures().stream()
                .map(signature -> signature.returnType() + "<-("
                        + String.join(",", signature.argumentTypes()) + ")")
                .sorted()
                .forEach(value -> update(digest, value));
    }

    private static void updateResourceLimits(
            MessageDigest digest, AiCapabilityDescriptor.ResourceLimits limits) {
        var request = limits.requestConstraints();
        update(digest, Long.toString(request.maxInputTokens()));
        update(digest, Long.toString(request.maxOutputTokens()));
        update(digest, Long.toString(request.maxRequestBytes()));
        update(digest, Long.toString(request.maxResponseBytes()));
        update(digest, Long.toString(limits.maxMediaBytes()));
        update(digest, Integer.toString(limits.maxConcurrency()));
        update(digest, Long.toString(limits.maxProviderItemsPerTask()));
        update(digest, Long.toString(limits.maxProviderAttemptsPerTask()));
    }

    private static void updateRetryPolicy(
            MessageDigest digest, AiCapabilityDescriptor.RetryPolicy policy) {
        update(digest, Integer.toString(policy.maxAttempts()));
        updateDuration(digest, policy.initialBackoff());
        updateDuration(digest, policy.maxBackoff());
        policy.retryableCategories().stream().map(Enum::name).sorted()
                .forEach(value -> update(digest, value));
    }

    private static void updateDuration(MessageDigest digest, java.time.Duration duration) {
        update(digest, Long.toString(duration.getSeconds()));
        update(digest, Integer.toString(duration.getNano()));
    }

    private static void updateProperties(MessageDigest digest, Map<String, String> properties) {
        properties.entrySet().stream()
                .filter(entry -> !DECLARATIVE_OR_SECRET_PROPERTIES.contains(
                        entry.getKey()))
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    update(digest, entry.getKey());
                    update(digest, entry.getValue());
                });
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
