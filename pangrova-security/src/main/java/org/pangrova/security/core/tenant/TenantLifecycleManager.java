package org.pangrova.security.core.tenant;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.api.tenant.TenantStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Manages tenant onboarding and offboarding workflows.
 */
public class TenantLifecycleManager {

    private static final Logger LOG = LoggerFactory.getLogger(TenantLifecycleManager.class);

    private final TenantStore store;

    /**
     * Creates a tenant lifecycle manager.
     *
     * @param store tenant persistence store
     */
    public TenantLifecycleManager(TenantStore store) {
        this.store = store;
    }

    /**
     * Creates and activates a tenant with the default resource quota.
     *
     * @param tenantId tenant identifier
     * @param name tenant display name
     * @param createdBy creating principal
     */
    public void onboard(String tenantId, String name, String createdBy) {
        Tenant tenant = new Tenant(tenantId, name, TenantStatus.ACTIVE,
                TenantDefaults.resourceQuota(), List.of(), List.of(), null,
                Map.of(), Instant.now(), createdBy);
        store.createTenant(tenant);
    }

    /**
     * Suspends and deletes a tenant.
     *
     * @param tenantId tenant identifier
     */
    public void offboard(String tenantId) {
        Tenant tenant = loadTenant(tenantId);
        updateStatus(tenant, TenantStatus.SUSPENDED);
        updateStatus(tenant, TenantStatus.DELETED);
    }

    /**
     * Suspends an active tenant.
     *
     * @param tenantId tenant identifier
     */
    public void suspend(String tenantId) {
        Tenant tenant = loadTenant(tenantId);
        if (tenant.status() == TenantStatus.SUSPENDED) {
            LOG.info("event=tenant_already_suspended tenantId={}", tenantId);
            return;
        }
        if (tenant.status() == TenantStatus.DELETED) {
            throw new PangrovaException(PangrovaErrorCode.TENANT_INVALID_STATE,
                    "Cannot suspend deleted tenant: " + tenantId);
        }
        updateStatus(tenant, TenantStatus.SUSPENDED);
        LOG.info("event=tenant_suspended tenantId={}", tenantId);
    }

    /**
     * Activates a tenant that has not been deleted.
     *
     * @param tenantId tenant identifier
     */
    public void activate(String tenantId) {
        Tenant tenant = loadTenant(tenantId);
        if (tenant.status() == TenantStatus.ACTIVE) {
            return;
        }
        if (tenant.status() == TenantStatus.DELETED) {
            throw new PangrovaException(PangrovaErrorCode.TENANT_INVALID_STATE,
                    "Cannot activate deleted tenant: " + tenantId);
        }
        updateStatus(tenant, TenantStatus.ACTIVE);
        LOG.info("event=tenant_activated tenantId={}", tenantId);
    }

    /**
     * Checks if a tenant is suspended. Callers should reject operations for suspended tenants.
     */
    public boolean isSuspended(String tenantId) {
        return store.getTenant(tenantId)
                .map(t -> t.status() == TenantStatus.SUSPENDED)
                .orElse(false);
    }

    /**
     * Grants a principal access to a tenant.
     */
    public void grantTenant(String tenantId, String principal) {
        Tenant tenant = loadTenant(tenantId);
        List<String> updatedSchemas = new java.util.ArrayList<>(tenant.allowedSchemas());
        if (!updatedSchemas.contains(principal)) {
            updatedSchemas.add(principal);
        }
        Tenant updated = new Tenant(tenant.tenantId(), tenant.name(), tenant.status(),
                tenant.quota(), tenant.allowedCatalogs(), List.copyOf(updatedSchemas),
                tenant.resourcePool(), tenant.properties(),
                tenant.createdAt(), tenant.createdBy());
        store.updateTenant(updated);
    }

    /**
     * Revokes a principal's access to a tenant.
     */
    public void revokeTenant(String tenantId, String principal) {
        Tenant tenant = loadTenant(tenantId);
        List<String> updatedSchemas = new java.util.ArrayList<>(tenant.allowedSchemas());
        updatedSchemas.remove(principal);
        Tenant updated = new Tenant(tenant.tenantId(), tenant.name(), tenant.status(),
                tenant.quota(), tenant.allowedCatalogs(), List.copyOf(updatedSchemas),
                tenant.resourcePool(), tenant.properties(),
                tenant.createdAt(), tenant.createdBy());
        store.updateTenant(updated);
    }

    private Tenant loadTenant(String tenantId) {
        return store.getTenant(tenantId)
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.TENANT_NOT_FOUND,
                        "Tenant not found: " + tenantId));
    }

    private void updateStatus(Tenant tenant, TenantStatus status) {
        Tenant updated = new Tenant(tenant.tenantId(), tenant.name(), status,
                tenant.quota(), tenant.allowedCatalogs(), tenant.allowedSchemas(),
                tenant.resourcePool(), tenant.properties(),
                tenant.createdAt(), tenant.createdBy());
        store.updateTenant(updated);
    }

}
