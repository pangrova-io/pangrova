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
package org.pangrova.ai.worker.governance;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.ContentPart;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Built-in, fail-closed worker admission policy for installations without a
 * deployment-specific {@link AiInvocationAuthorizer} plugin.
 *
 * <p>The default mode is {@code deny}. Enabling {@code authenticated} requires
 * explicit model, capability and permission allowlists and still relies on the
 * engine-owned tenant/principal context propagated by the authenticated task
 * channel. Role-aware authorization belongs in a dedicated authorizer plugin;
 * physical resource admission remains owned by the Worker task authority.</p>
 */
public final class ConfigurableAiInvocationAuthorizer implements AiInvocationAuthorizer {

    static final String MODE = "authorization.mode";
    static final String ALLOWED_MODELS = "authorization.allowed-models";
    static final String ALLOWED_CAPABILITIES = "authorization.allowed-capabilities";
    static final String GRANTED_PERMISSIONS = "authorization.granted-permissions";
    static final String MAX_BATCH_SIZE = "authorization.max-batch-size";
    static final String MAX_INPUT_BYTES = "authorization.max-input-bytes";

    private static final int DEFAULT_MAX_BATCH_SIZE = 128;
    private static final long DEFAULT_MAX_INPUT_BYTES = 8L * 1024 * 1024;

    private final boolean enabled;
    private final Set<String> allowedModels;
    private final Set<String> allowedCapabilities;
    private final Set<String> grantedPermissions;
    private final int maxBatchSize;
    private final long maxInputBytes;

    private ConfigurableAiInvocationAuthorizer(
            boolean enabled,
            Set<String> allowedModels,
            Set<String> allowedCapabilities,
            Set<String> grantedPermissions,
            int maxBatchSize,
            long maxInputBytes) {
        this.enabled = enabled;
        this.allowedModels = allowedModels;
        this.allowedCapabilities = allowedCapabilities;
        this.grantedPermissions = grantedPermissions;
        this.maxBatchSize = maxBatchSize;
        this.maxInputBytes = maxInputBytes;
    }

    /** Builds an authorizer from the worker's immutable AI authorization settings. */
    public static ConfigurableAiInvocationAuthorizer from(Map<String, String> config) {
        Map<String, String> values = config == null ? Map.of() : config;
        String mode = values.getOrDefault(MODE, "deny").trim().toLowerCase(java.util.Locale.ROOT);
        if (!mode.equals("deny") && !mode.equals("authenticated")) {
            throw new IllegalArgumentException(
                    MODE + " must be 'deny' or 'authenticated', got: " + mode);
        }
        Set<String> models = csv(values.get(ALLOWED_MODELS));
        Set<String> capabilities = csv(values.get(ALLOWED_CAPABILITIES));
        Set<String> permissions = csv(values.get(GRANTED_PERMISSIONS));
        int batchSize = parsePositiveInt(
                values.get(MAX_BATCH_SIZE), DEFAULT_MAX_BATCH_SIZE, MAX_BATCH_SIZE);
        long inputBytes = parsePositiveLong(
                values.get(MAX_INPUT_BYTES), DEFAULT_MAX_INPUT_BYTES, MAX_INPUT_BYTES);
        if (mode.equals("authenticated")
                && (models.isEmpty() || capabilities.isEmpty() || permissions.isEmpty())) {
            throw new IllegalArgumentException(
                    "authenticated AI authorization requires explicit model, capability "
                            + "and permission allowlists");
        }
        return new ConfigurableAiInvocationAuthorizer(
                mode.equals("authenticated"),
                models,
                capabilities,
                permissions,
                batchSize,
                inputBytes);
    }

    /** Selects the sole installed policy or the built-in fail-closed policy. */
    public static AiInvocationAuthorizer discover(Map<String, String> config) {
        List<AiInvocationAuthorizer> implementations = ServiceLoader
                .load(AiInvocationAuthorizer.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();
        if (implementations.size() > 1) {
            throw new IllegalStateException(
                    "Multiple AiInvocationAuthorizer implementations are installed: "
                            + implementations.stream()
                            .map(value -> value.getClass().getName())
                            .toList());
        }
        if (implementations.isEmpty()) {
            return from(config);
        }
        return implementations.get(0);
    }

    @Override
    public void authorize(
            AiInvocationRequest request, AiCapabilityDescriptor capability) {
        if (!enabled) {
            deny("AI invocation authorization mode is deny");
        }
        if (!matches(allowedModels, request.modelName())) {
            deny("Model is not allowed: " + request.modelName());
        }
        if (!matches(allowedCapabilities, capability.capabilityId())) {
            deny("Capability is not allowed: " + capability.capabilityId());
        }
        if (!grantedPermissions.containsAll(capability.requiredPermissions())) {
            deny("Missing required AI permissions: "
                    + difference(capability.requiredPermissions(), grantedPermissions));
        }
        if (request.inputs().size() > maxBatchSize) {
            budget("AI batch size " + request.inputs().size()
                    + " exceeds configured maximum " + maxBatchSize);
        }
        long inputBytes;
        try {
            inputBytes = request.inputs().stream()
                    .mapToLong(ConfigurableAiInvocationAuthorizer::inputBytes)
                    .reduce(0, Math::addExact);
        } catch (ArithmeticException e) {
            budget("AI input size exceeds supported accounting range");
            return;
        }
        if (inputBytes > maxInputBytes) {
            budget("AI input bytes " + inputBytes
                    + " exceeds configured maximum " + maxInputBytes);
        }
    }

    private static long inputBytes(AiInvocationRequest.Input input) {
        if (input instanceof AiInvocationRequest.TextInput text) {
            return utf8Length(text.text());
        }
        if (input instanceof AiInvocationRequest.BlobInput blob) {
            return blob.reference().size();
        }
        if (input instanceof AiInvocationRequest.VectorInput vector) {
            return Math.multiplyExact((long) vector.vector().length, Float.BYTES);
        }
        if (input instanceof AiInvocationRequest.RerankInput rerank) {
            return Math.addExact(
                    utf8Length(rerank.query()),
                    utf8Length(rerank.document()));
        }
        var prompt = ((AiInvocationRequest.MultimodalInput) input).prompt();
        long bytes = 0;
        for (ContentPart part : prompt.parts()) {
            long partBytes;
            if (part instanceof ContentPart.Text text) {
                partBytes = utf8Length(text.text());
            } else if (part instanceof ContentPart.FileRef file) {
                partBytes = file.reference().size();
            } else {
                partBytes = ((ContentPart.InlineImage) part).data().length;
            }
            bytes = Math.addExact(bytes, partBytes);
        }
        return bytes;
    }

    private static long utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static boolean matches(Set<String> allowlist, String value) {
        return allowlist.contains("*") || allowlist.contains(value);
    }

    private static Set<String> difference(Set<String> required, Set<String> granted) {
        return required.stream()
                .filter(permission -> !granted.contains(permission))
                .collect(Collectors.toUnmodifiableSet());
    }

    private static Set<String> csv(String value) {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static int parsePositiveInt(String value, int fallback, String name) {
        long parsed = parsePositiveLong(value, fallback, name);
        if (parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds integer range");
        }
        return (int) parsed;
    }

    private static long parsePositiveLong(String value, long fallback, String name) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
    }

    private static void deny(String message) {
        throw new AiInvocationException(
                AiInvocationException.Category.AUTHORIZATION, false, message);
    }

    private static void budget(String message) {
        throw new AiInvocationException(
                AiInvocationException.Category.BUDGET_EXCEEDED, false, message);
    }
}
