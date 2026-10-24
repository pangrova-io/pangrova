package org.pangrova.materialization.domain;

/**
 * Tenant owning a materialization object.
 *
 * @param value tenant identifier
 */
public record TenantId(String value) {

    public TenantId {
        value = Require.text(value, "tenantId");
    }
}
