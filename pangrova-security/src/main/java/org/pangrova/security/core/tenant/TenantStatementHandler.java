package org.pangrova.security.core.tenant;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.persistence.api.security.tenant.TenantStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Executes tenant management statements against the tenant store.
 */
public class TenantStatementHandler {

    private final TenantStore store;

    /**
     * Creates a tenant statement handler.
     *
     * @param store tenant persistence store
     */
    public TenantStatementHandler(TenantStore store) {
        this.store = store;
    }

    /**
     * Executes a tenant management statement.
     *
     * @param statement statement to execute
     */
    public void execute(TenantStatement statement) {
        if (statement instanceof TenantStatement.CreateTenant ct) {
            handleCreate(ct);
        } else if (statement instanceof TenantStatement.AlterTenant at) {
            handleAlter(at);
        } else if (statement instanceof TenantStatement.DropTenant dt) {
            handleDrop(dt);
        } else if (statement instanceof TenantStatement.GrantTenant gt) {
            handleGrant(gt);
        } else if (statement instanceof TenantStatement.RevokeTenant rt) {
            handleRevoke(rt);
        }
    }

    private void handleCreate(TenantStatement.CreateTenant ct) {
        Tenant tenant = buildTenant(ct);
        store.createTenant(tenant);
    }

    private void handleAlter(TenantStatement.AlterTenant at) {
        Tenant existing = loadTenant(at.tenantId());
        Tenant updated = applyProperties(existing, at.properties());
        store.updateTenant(updated);
    }

    private void handleDrop(TenantStatement.DropTenant dt) {
        Tenant existing = loadTenant(dt.tenantId());
        store.updateTenant(withStatus(existing, TenantStatus.DELETED));
    }

    private void handleGrant(TenantStatement.GrantTenant gt) {
        store.addUserToTenant(gt.tenantId(), gt.userPrincipal(), gt.role());
    }

    private void handleRevoke(TenantStatement.RevokeTenant rt) {
        store.removeUserFromTenant(rt.tenantId(), rt.userPrincipal());
    }

    private Tenant loadTenant(String tenantId) {
        return store.getTenant(tenantId)
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.TENANT_QUOTA_EXCEEDED,
                        "Tenant not found: " + tenantId));
    }

    private Tenant buildTenant(TenantStatement.CreateTenant ct) {
        return new Tenant(ct.tenantId(), ct.name(), TenantStatus.ACTIVE,
                TenantDefaults.resourceQuota(), List.of(), List.of(), null,
                ct.properties(), Instant.now(), "system");
    }

    private Tenant applyProperties(Tenant existing, Map<String, String> props) {
        TenantStatus status = props.containsKey("status")
                ? TenantStatus.valueOf(props.get("status"))
                : existing.status();
        return new Tenant(existing.tenantId(), existing.name(), status,
                existing.quota(), existing.allowedCatalogs(), existing.allowedSchemas(),
                existing.resourcePool(), props, existing.createdAt(), existing.createdBy());
    }

    private Tenant withStatus(Tenant tenant, TenantStatus status) {
        return new Tenant(tenant.tenantId(), tenant.name(), status,
                tenant.quota(), tenant.allowedCatalogs(), tenant.allowedSchemas(),
                tenant.resourcePool(), tenant.properties(),
                tenant.createdAt(), tenant.createdBy());
    }

}
