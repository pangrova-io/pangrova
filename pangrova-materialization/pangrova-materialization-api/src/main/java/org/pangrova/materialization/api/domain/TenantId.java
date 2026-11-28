package org.pangrova.materialization.api.domain;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Tenant owning a materialization object.
 *
 * @param value tenant identifier
 */
public record TenantId(String value) {

    public TenantId {
        value = requireText(value, "tenantId");
    }
}
