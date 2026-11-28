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
package org.pangrova.ai.api.capability;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.common.api.annotation.Experimental;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Canonical description of an AI capability shared by planning, Worker dispatch
 * and provider implementations.
 *
 * <p>The descriptor is deliberately execution-oriented: it declares the exact
 * signatures, version, supported modes and modalities, batching limit, retry
 * policy and authorization requirements. A component must reject a request it
 * cannot satisfy instead of silently selecting a nearby capability.</p>
 */
@Experimental
public record AiCapabilityDescriptor(
        String capabilityId,
        String version,
        AiOperation operation,
        List<Signature> signatures,
        Set<AiExecutionMode> executionModes,
        Set<Modality> modalities,
        int maxBatchSize,
        ResourceLimits resourceLimits,
        boolean deterministic,
        RetryPolicy retryPolicy,
        Set<String> requiredPermissions) {

    public record Signature(String returnType, List<String> argumentTypes) {
        public Signature {
            requireText(returnType, "returnType");
            argumentTypes = argumentTypes == null ? List.of() : List.copyOf(argumentTypes);
            if (argumentTypes.stream().anyMatch(type -> type == null || type.isBlank())) {
                throw new IllegalArgumentException("argumentTypes must not contain blank values");
            }
        }
    }

    /**
     * Hard serving envelope for an exact model deployment. These limits
     * are part of the signed model capability and therefore cannot drift
     * independently between planning, admission and execution.
     *
     * @param requestConstraints exact per-request limits; token limits are not usage statistics
     * @param maxMediaBytes   maximum referenced or inline media bytes per
     *                        request; zero for text-only operations
     * @param maxConcurrency  maximum simultaneous upstream requests served by
     *                        this model deployment on one Worker
     * @param maxProviderItemsPerTask maximum logical input items admitted to one physical task
     * @param maxProviderAttemptsPerTask maximum physical HTTP sends, including failed retries
     */
    public record ResourceLimits(
            AiRequestConstraints requestConstraints,
            long maxMediaBytes,
            int maxConcurrency,
            long maxProviderItemsPerTask,
            long maxProviderAttemptsPerTask) {
        public ResourceLimits {
            Objects.requireNonNull(requestConstraints, "requestConstraints required");
            if (maxMediaBytes < 0) {
                throw new IllegalArgumentException(
                        "media limit must not be negative");
            }
            if (maxConcurrency <= 0) {
                throw new IllegalArgumentException(
                        "maxConcurrency must be positive");
            }
            if (maxProviderItemsPerTask <= 0 || maxProviderAttemptsPerTask <= 0) {
                throw new IllegalArgumentException(
                        "provider item and physical-attempt limits must be positive");
            }
        }
    }

    public record RetryPolicy(
            int maxAttempts,
            Duration initialBackoff,
            Duration maxBackoff,
            Set<AiFailureCategory> retryableCategories) {
        public RetryPolicy {
            if (maxAttempts <= 0) {
                throw new IllegalArgumentException("maxAttempts must be positive");
            }
            Objects.requireNonNull(initialBackoff, "initialBackoff must not be null");
            Objects.requireNonNull(maxBackoff, "maxBackoff must not be null");
            if (initialBackoff.isNegative() || maxBackoff.isNegative()
                    || maxBackoff.compareTo(initialBackoff) < 0) {
                throw new IllegalArgumentException(
                        "retry backoff must be non-negative and maxBackoff >= initialBackoff");
            }
            retryableCategories = retryableCategories == null
                    ? Set.of()
                    : Set.copyOf(retryableCategories);
        }

        public static RetryPolicy noRetry() {
            return new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Set.of());
        }
    }

    public AiCapabilityDescriptor {
        requireText(capabilityId, "capabilityId");
        requireText(version, "version");
        Objects.requireNonNull(operation, "operation must not be null");
        signatures = List.copyOf(Objects.requireNonNull(signatures, "signatures must not be null"));
        if (signatures.isEmpty()) {
            throw new IllegalArgumentException("signatures must not be empty");
        }
        executionModes = Set.copyOf(Objects.requireNonNull(
                executionModes, "executionModes must not be null"));
        modalities = Set.copyOf(Objects.requireNonNull(modalities, "modalities must not be null"));
        if (executionModes.isEmpty()) {
            throw new IllegalArgumentException("executionModes must not be empty");
        }
        if (modalities.isEmpty()) {
            throw new IllegalArgumentException("modalities must not be empty");
        }
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        Objects.requireNonNull(resourceLimits, "resourceLimits must not be null");
        Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        requiredPermissions = requiredPermissions == null ? Set.of() : Set.copyOf(requiredPermissions);
        if (requiredPermissions.stream().anyMatch(value -> value == null || value.isBlank())) {
            throw new IllegalArgumentException("requiredPermissions must not contain blank values");
        }
    }
}
