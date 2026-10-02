package org.pangrova.worker.ai.provider;

import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.AiModelDescriptor;
import org.pangrova.spi.ai.AiRequestConstraints;
import org.pangrova.spi.ai.Modality;
import java.net.URI;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Parses and canonicalizes configured AI model descriptors. */
final class AiModelConfigParser {

    private static final String MODEL_PREFIX = "models.";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final Set<String> KNOWN_PROPERTIES = Set.of(
            "name", "provider", "provider-model", "version", "endpoint", "secret-reference",
            "capabilities", "modalities", "max-batch-size", "max-requests-per-second",
            "timeout-seconds", "max-input-tokens", "max-output-tokens", "max-media-bytes",
            "max-concurrency", "max-provider-items-per-task", "max-provider-attempts-per-task",
            "max-request-bytes", "max-response-bytes", "embedding-dimension",
            "query-prefix", "document-prefix", "authentication", "enabled");

    private AiModelConfigParser() {}

    static Map<String, AiModelDescriptor> parse(
            Map<String, String> config,
            boolean governedBlobLoaderAvailable) {
        Map<String, AiModelDescriptor> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : group(config).entrySet()) {
            AiModelDescriptor descriptor = parseModel(
                    entry.getKey(), Map.copyOf(entry.getValue()), governedBlobLoaderAvailable);
            if (descriptor == null) {
                continue;
            }
            if (result.putIfAbsent(descriptor.name(), descriptor) != null) {
                throw configurationFailure(
                        "Duplicate SQL-visible AI model name: " + descriptor.name());
            }
        }
        return Map.copyOf(result);
    }

    private static Map<String, Map<String, String>> group(Map<String, String> config) {
        Map<String, Map<String, String>> grouped = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().startsWith(MODEL_PREFIX)) {
                addProperty(grouped, entry);
            }
        }
        return grouped;
    }

    private static void addProperty(
            Map<String, Map<String, String>> grouped, Map.Entry<String, String> entry) {
        String remainder = entry.getKey().substring(MODEL_PREFIX.length());
        int separator = remainder.indexOf('.');
        if (separator <= 0 || separator == remainder.length() - 1) {
            throw configurationFailure("Invalid AI model property key: " + entry.getKey());
        }
        String modelName = remainder.substring(0, separator);
        String property = remainder.substring(separator + 1);
        requireConfigId(modelName);
        if (!KNOWN_PROPERTIES.contains(property)) {
            throw configurationFailure(
                    "Unknown property '" + property + "' for AI model '" + modelName + "'");
        }
        grouped.computeIfAbsent(modelName, ignored -> new LinkedHashMap<>())
                .put(property, entry.getValue());
    }

    private static void requireConfigId(String modelName) {
        if (!modelName.matches("[A-Za-z0-9_-]+")) {
            throw configurationFailure(
                    "Configured AI model names may contain only letters, digits, '_' and '-': "
                            + modelName);
        }
    }

    private static AiModelDescriptor parseModel(
            String configId,
            Map<String, String> properties,
            boolean governedBlobLoaderAvailable) {
        if (!Boolean.parseBoolean(properties.getOrDefault("enabled", "true"))) {
            return null;
        }
        ModelSettings settings = settings(
                configId, properties, governedBlobLoaderAvailable);
        return new AiModelDescriptor(
                settings.modelName(),
                settings.version(),
                settings.provider(),
                URI.create(settings.endpoint()),
                settings.secretReference(),
                settings.capabilities(),
                true,
                canonicalProperties(properties, settings));
    }

    private static ModelSettings settings(
            String configId,
            Map<String, String> properties,
            boolean governedBlobLoaderAvailable) {
        String modelName = properties.getOrDefault("name", configId).trim();
        if (modelName.isEmpty()) {
            throw configurationFailure("AI model config '" + configId + "' has a blank name");
        }
        String version = required(properties, "version", modelName);
        String provider = required(properties, "provider", modelName);
        String providerModel = required(properties, "provider-model", modelName);
        String endpoint = required(properties, "endpoint", modelName);
        Authentication authentication = authentication(properties, modelName);
        requireSecureEndpoint(endpoint, authentication, modelName);
        int maxBatchSize = positiveInt(properties, "max-batch-size", 16, modelName);
        Set<Modality> modalities = parseModalities(properties.get("modalities"), modelName);
        String rawCapabilities = required(properties, "capabilities", modelName);
        Set<String> capabilityNames = capabilityNames(rawCapabilities);
        AiCapabilityDescriptor.ResourceLimits limits = resourceLimits(
                properties, modelName, capabilityNames);
        boolean embedding = capabilityNames.contains("embedding");
        AiProviderDialectValidator.validate(
                provider, endpoint, capabilityNames, modalities, maxBatchSize, modelName);
        List<AiCapabilityDescriptor> capabilities = AiCapabilityConfigParser.parse(
                rawCapabilities, modelName, version, maxBatchSize, modalities, limits,
                governedBlobLoaderAvailable);
        return new ModelSettings(
                modelName, version, provider, providerModel, endpoint,
                authentication.secretReference(), authentication.type(), maxBatchSize,
                limits, modalities, capabilities, embedding);
    }

    private static void requireSecureEndpoint(
            String endpoint, Authentication authentication, String modelName) {
        URI uri = parseEndpoint(endpoint, modelName);
        if (!isCanonicalEndpoint(uri)) {
            throw configurationFailure(
                    "AI model '" + modelName
                            + "' endpoint must be an absolute URI without user-info, query or fragment");
        }
        if ("https".equalsIgnoreCase(uri.getScheme())) {
            return;
        }
        if (!isAnonymousLoopback(uri, authentication)) {
            throw configurationFailure(
                    "AI model '" + modelName
                            + "' endpoint must use HTTPS; HTTP is allowed only for an "
                            + "anonymous loopback deployment");
        }
    }

    private static URI parseEndpoint(String endpoint, String modelName) {
        final URI uri;
        try {
            uri = URI.create(endpoint);
        } catch (IllegalArgumentException failure) {
            throw configurationFailure(
                    "AI model '" + modelName + "' endpoint is not a valid URI", failure);
        }
        return uri;
    }

    private static boolean isCanonicalEndpoint(URI uri) {
        return uri.isAbsolute()
                && uri.getHost() != null
                && uri.getUserInfo() == null
                && uri.getQuery() == null
                && uri.getFragment() == null;
    }

    private static boolean isAnonymousLoopback(
            URI uri, Authentication authentication) {
        return "none".equals(authentication.type())
                && "http".equalsIgnoreCase(uri.getScheme())
                && isLoopbackHost(uri.getHost());
    }

    private static boolean isLoopbackHost(String host) {
        return "localhost".equalsIgnoreCase(host)
                || "127.0.0.1".equals(host)
                || "::1".equals(host)
                || "[::1]".equals(host);
    }

    private static Authentication authentication(
            Map<String, String> properties, String modelName) {
        String type = properties.getOrDefault(
                "authentication", "bearer").trim().toLowerCase(Locale.ROOT);
        if (!type.equals("bearer") && !type.equals("none")) {
            throw configurationFailure(
                    "AI model '" + modelName + "' authentication must be 'bearer' or 'none'");
        }
        String configuredSecret = properties.get("secret-reference");
        if (type.equals("none") && configuredSecret != null && !configuredSecret.isBlank()) {
            throw configurationFailure(
                    "AI model '" + modelName
                            + "' with authentication=none must not configure a secret-reference");
        }
        String secretReference = type.equals("none")
                ? "" : required(properties, "secret-reference", modelName);
        return new Authentication(type, secretReference);
    }

    private static Set<String> capabilityNames(String rawCapabilities) {
        return AiCapabilityConfigParser.csv(rawCapabilities).stream()
                .map(value -> value.toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static AiCapabilityDescriptor.ResourceLimits resourceLimits(
            Map<String, String> properties, String modelName, Set<String> capabilityNames) {
        long maxInputTokens = positiveLong(properties, "max-input-tokens", modelName);
        int maxConcurrency = positiveInt(properties, "max-concurrency", -1, modelName);
        long maxItems = positiveLong(properties, "max-provider-items-per-task", modelName);
        long maxAttempts = positiveLong(properties, "max-provider-attempts-per-task", modelName);
        long maxRequestBytes = positiveLong(properties, "max-request-bytes", modelName);
        long maxResponseBytes = positiveLong(properties, "max-response-bytes", modelName);
        boolean generates = capabilityNames.contains("inference")
                || capabilityNames.contains("multimodal");
        long maxOutputTokens = generates
                ? positiveLong(properties, "max-output-tokens", modelName)
                : optionalNonNegativeLong(properties, "max-output-tokens", modelName);
        long maxMediaBytes = capabilityNames.contains("multimodal")
                ? positiveLong(properties, "max-media-bytes", modelName)
                : optionalNonNegativeLong(properties, "max-media-bytes", modelName);
        return new AiCapabilityDescriptor.ResourceLimits(
                new AiRequestConstraints(maxInputTokens, maxOutputTokens, maxRequestBytes, maxResponseBytes),
                maxMediaBytes, maxConcurrency, maxItems, maxAttempts);
    }

    private static Map<String, String> canonicalProperties(
            Map<String, String> properties, ModelSettings settings) {
        var canonical = new LinkedHashMap<String, String>();
        putIdentity(canonical, settings);
        putLimits(canonical, settings);
        putTuning(canonical, properties, settings.modelName());
        putEmbedding(canonical, properties, settings.modelName(), settings.embedding());
        canonical.put("enabled", "true");
        return Map.copyOf(canonical);
    }

    private static void putIdentity(
            Map<String, String> canonical, ModelSettings settings) {
        canonical.put("name", settings.modelName());
        canonical.put("provider", settings.provider());
        canonical.put("provider-model", settings.providerModel());
        canonical.put("version", settings.version());
        canonical.put("endpoint", URI.create(settings.endpoint()).normalize().toASCIIString());
        canonical.put("secret-reference", settings.secretReference());
        canonical.put("authentication", settings.authentication());
        canonical.put("capabilities", settings.capabilities().stream()
                .map(capability -> capability.operation().name()).sorted()
                .collect(java.util.stream.Collectors.joining(",")));
        canonical.put("modalities", settings.modalities().stream()
                .map(Modality::name).sorted()
                .collect(java.util.stream.Collectors.joining(",")));
    }

    private static void putLimits(
            Map<String, String> canonical, ModelSettings settings) {
        AiCapabilityDescriptor.ResourceLimits limits = settings.limits();
        canonical.put("max-batch-size", Integer.toString(settings.maxBatchSize()));
        canonical.put("max-input-tokens", Long.toString(limits.requestConstraints().maxInputTokens()));
        canonical.put("max-output-tokens", Long.toString(limits.requestConstraints().maxOutputTokens()));
        canonical.put("max-request-bytes", Long.toString(limits.requestConstraints().maxRequestBytes()));
        canonical.put("max-response-bytes", Long.toString(limits.requestConstraints().maxResponseBytes()));
        canonical.put("max-media-bytes", Long.toString(limits.maxMediaBytes()));
        canonical.put("max-concurrency", Integer.toString(limits.maxConcurrency()));
        canonical.put("max-provider-items-per-task", Long.toString(limits.maxProviderItemsPerTask()));
        canonical.put("max-provider-attempts-per-task", Long.toString(limits.maxProviderAttemptsPerTask()));
    }

    private static void putTuning(
            Map<String, String> canonical,
            Map<String, String> properties,
            String modelName) {
        canonical.put("max-requests-per-second", Double.toString(positiveDouble(
                properties, "max-requests-per-second", 10.0, modelName)));
        canonical.put("timeout-seconds", Integer.toString(positiveInt(
                properties, "timeout-seconds", DEFAULT_TIMEOUT_SECONDS, modelName)));
        canonical.put("query-prefix", properties.getOrDefault("query-prefix", ""));
        canonical.put("document-prefix", properties.getOrDefault("document-prefix", ""));
    }

    private static void putEmbedding(
            Map<String, String> canonical,
            Map<String, String> properties,
            String modelName,
            boolean embedding) {
        if (embedding && isBlank(properties.get("embedding-dimension"))) {
            throw configurationFailure(
                    "AI model '" + modelName + "' requires property 'embedding-dimension'");
        }
        if (embedding || properties.containsKey("embedding-dimension")) {
            canonical.put("embedding-dimension", Integer.toString(positiveInt(
                    properties, "embedding-dimension", -1, modelName)));
        }
    }

    private static Set<Modality> parseModalities(String raw, String modelName) {
        if (isBlank(raw)) {
            return Set.of(Modality.TEXT);
        }
        EnumSet<Modality> result = EnumSet.noneOf(Modality.class);
        for (String value : AiCapabilityConfigParser.csv(raw)) {
            try {
                result.add(Modality.valueOf(value.toUpperCase(Locale.ROOT)));
            } catch (IllegalArgumentException failure) {
                throw configurationFailure(
                        "Unknown modality '" + value + "' for AI model '" + modelName + "'");
            }
        }
        if (!result.contains(Modality.TEXT)) {
            throw configurationFailure(
                    "AI model '" + modelName + "' modalities must include TEXT");
        }
        return Set.copyOf(result);
    }

    private static String required(
            Map<String, String> properties, String property, String modelName) {
        String value = properties.get(property);
        if (isBlank(value)) {
            throw configurationFailure(
                    "AI model '" + modelName + "' requires property '" + property + "'");
        }
        return value.trim();
    }

    private static int positiveInt(
            Map<String, String> properties, String property, int fallback, String modelName) {
        int value = parseInt(properties.get(property), fallback, property, modelName);
        if (value <= 0) {
            throw configurationFailure(
                    "AI model '" + modelName + "' property '" + property + "' must be positive");
        }
        return value;
    }

    private static int parseInt(
            String raw, int fallback, String property, String modelName) {
        if (isBlank(raw)) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException failure) {
            throw configurationFailure(
                    "AI model '" + modelName + "' property '" + property
                            + "' must be an integer", failure);
        }
    }

    private static long positiveLong(
            Map<String, String> properties, String property, String modelName) {
        long value = parseLong(required(properties, property, modelName), property, modelName);
        if (value <= 0) {
            throw configurationFailure(
                    "AI model '" + modelName + "' property '" + property + "' must be positive");
        }
        return value;
    }

    private static long optionalNonNegativeLong(
            Map<String, String> properties, String property, String modelName) {
        String raw = properties.get(property);
        if (isBlank(raw)) {
            return 0;
        }
        long value = parseLong(raw, property, modelName);
        if (value < 0) {
            throw configurationFailure(
                    "AI model '" + modelName + "' property '" + property
                            + "' must not be negative");
        }
        return value;
    }

    private static long parseLong(String raw, String property, String modelName) {
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException failure) {
            throw configurationFailure(
                    "AI model '" + modelName + "' property '" + property
                            + "' must be an integer", failure);
        }
    }

    private static double positiveDouble(
            Map<String, String> properties, String property, double fallback, String modelName) {
        String raw = properties.get(property);
        if (isBlank(raw)) {
            return fallback;
        }
        try {
            double value = Double.parseDouble(raw);
            if (!Double.isFinite(value) || value <= 0) {
                throw new NumberFormatException("not positive and finite");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw configurationFailure(
                    "AI model '" + modelName + "' property '" + property
                            + "' must be positive and finite", failure);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static IllegalArgumentException configurationFailure(String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException configurationFailure(
            String message, RuntimeException cause) {
        return new IllegalArgumentException(message, cause);
    }

    private record Authentication(String type, String secretReference) {}

    private record ModelSettings(
            String modelName,
            String version,
            String provider,
            String providerModel,
            String endpoint,
            String secretReference,
            String authentication,
            int maxBatchSize,
            AiCapabilityDescriptor.ResourceLimits limits,
            Set<Modality> modalities,
            List<AiCapabilityDescriptor> capabilities,
            boolean embedding) {}
}
