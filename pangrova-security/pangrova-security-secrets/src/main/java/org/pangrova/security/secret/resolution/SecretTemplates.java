package org.pangrova.security.secret.resolution;

import org.pangrova.security.api.secret.SecretReference;
import org.pangrova.security.api.secret.SecretResolver;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.enums.PangrovaErrorCode;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves {@code ${PREFIX:reference}} tokens inside configuration values.
 */
@Experimental
public final class SecretTemplates {

    private static final Pattern SECRET_PATTERN = Pattern.compile("\\$\\{([A-Z]+):([^{}]+)\\}");

    private SecretTemplates() {
    }

    /**
     * Expands every reference exactly once. Ordinary text and provider-returned bytes
     * (including a literal template marker in a credential) pass through unchanged.
     * Any malformed template in the input is rejected before it reaches a connector.
     *
     * @param resolver the resolver that owns the provider registry
     * @param value    the configuration value, possibly null
     * @param tenantId optional tenant scope; may be null
     */
    public static String resolve(SecretResolver resolver, String value, String tenantId) {
        Objects.requireNonNull(resolver, "resolver must not be null");
        int start = value == null ? -1 : value.indexOf("${");
        if (start < 0) {
            return value;
        }
        Matcher matcher = SECRET_PATTERN.matcher(value);
        StringBuilder result = new StringBuilder(value.length());
        int position = 0;
        while (start >= 0) {
            result.append(value, position, start);
            if (!matcher.region(start, value.length()).lookingAt()) {
                throw new ConfigurationException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "Malformed connector secret reference");
            }
            String secret = resolver.resolve(new SecretReference(matcher.group(1), matcher.group(2), tenantId));
            if (secret == null) {
                throw new ConfigurationException(PangrovaErrorCode.CONFIG_MISSING_KEY,
                        "Secret resolver returned null");
            }
            result.append(secret);
            position = matcher.end();
            start = value.indexOf("${", position);
        }
        return result.append(value, position, value.length()).toString();
    }
}
