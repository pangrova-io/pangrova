package org.pangrova.materialization.api.domain;

/**
 * Tenant-scoped identity of a Dynamic Table or Materialized View.
 *
 * @param tenant owning tenant
 * @param name qualified object name
 */
public record MaterializationId(TenantId tenant, QualifiedName name) {

    public MaterializationId {
        if (tenant == null) {
            throw new IllegalArgumentException("tenant must not be null");
        }
        if (name == null) {
            throw new IllegalArgumentException("name must not be null");
        }
    }

    /**
     * Returns the canonical identity used in hashes and logs.
     *
     * @return canonical identity
     */
    public String canonicalName() {
        return tenant.value() + "/" + name;
    }

    /**
     * Parses a canonical identity produced by {@link #canonicalName()}.
     *
     * @param canonicalName canonical identity
     * @return parsed identity
     */
    public static MaterializationId parse(String canonicalName) {
        if (canonicalName == null) {
            throw new IllegalArgumentException("canonicalName must not be null");
        }
        int separator = canonicalName.indexOf('/');
        if (separator <= 0 || separator == canonicalName.length() - 1) {
            throw new IllegalArgumentException(
                    "materialization identity must be tenant/qualified-name: " + canonicalName);
        }
        return new MaterializationId(new TenantId(canonicalName.substring(0, separator)),
                QualifiedName.parse(canonicalName.substring(separator + 1)));
    }
}
