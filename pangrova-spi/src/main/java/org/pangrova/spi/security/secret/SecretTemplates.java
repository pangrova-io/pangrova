package org.pangrova.spi.security.secret;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves {@code ${PREFIX:reference}} tokens inside configuration values.
 */
@Experimental
public final class SecretTemplates {

    private static final Pattern SECRET_PATTERN = Pattern.compile("\\$\\{([A-Z]+):(.+?)\\}");

    private SecretTemplates() {
    }

    /**
     * Resolves every secret token in {@code value}; plain values and unmatched
     * tokens are returned unchanged.
     *
     * @param resolver the resolver that owns the provider registry
     * @param value    the configuration value, possibly null
     * @param tenantId optional tenant scope; may be null
     */
    public static String resolve(SecretResolver resolver, String value, String tenantId) {
        Objects.requireNonNull(resolver, "resolver must not be null");
        if (value == null || !value.contains("${")) {
            return value;
        }
        Matcher matcher = SECRET_PATTERN.matcher(value);
        StringBuilder resolved = new StringBuilder();
        while (matcher.find()) {
            String secret = resolver.resolve(
                    new SecretReference(matcher.group(1), matcher.group(2), tenantId));
            matcher.appendReplacement(resolved, Matcher.quoteReplacement(secret));
        }
        matcher.appendTail(resolved);
        return resolved.toString();
    }
}
