package org.pangrova.spi.security.secret;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Locale;
import java.util.Objects;

/**
 * One secret lookup: a provider prefix, an opaque provider-specific key, and an
 * optional tenant scope.
 *
 * @param prefix   provider prefix such as {@code ENV} or {@code FILE}
 * @param key      provider-specific reference
 * @param tenantId optional tenant scope; may be null
 */
@Experimental
public record SecretReference(String prefix, String key, String tenantId) {

    public SecretReference {
        requireNonBlank(prefix, "prefix");
        requireNonBlank(key, "key");
    }

    /** Creates a tenant-neutral reference. */
    public static SecretReference of(String prefix, String key) {
        return new SecretReference(prefix, key, null);
    }

    /** Creates the same reference scoped to another tenant. */
    public SecretReference withTenant(String scopedTenantId) {
        return new SecretReference(prefix, key, scopedTenantId);
    }

    /**
     * Parses the {@code scheme://key} form used by AI provider configuration.
     *
     * @throws IllegalArgumentException if the reference does not use that form
     */
    public static SecretReference parse(String reference) {
        Objects.requireNonNull(reference, "reference");
        int separator = reference.indexOf("://");
        if (separator <= 0 || separator + 3 >= reference.length()) {
            throw new IllegalArgumentException(
                    "Secret reference must use scheme://key syntax");
        }
        return new SecretReference(
                reference.substring(0, separator).toUpperCase(Locale.ROOT),
                reference.substring(separator + 3),
                null);
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
