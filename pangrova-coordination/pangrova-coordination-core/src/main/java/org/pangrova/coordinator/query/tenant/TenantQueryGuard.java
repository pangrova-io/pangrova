package org.pangrova.coordinator.query.tenant;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.security.core.tenant.QuotaCheckResult;
import org.pangrova.security.core.tenant.QuotaChecker;
import org.pangrova.security.core.tenant.TenantQuotaExceededException;
import org.pangrova.security.core.tenant.TenantUsage;
import org.pangrova.security.core.tenant.TenantResolver;
import org.pangrova.spi.security.tenant.ResolvedTenantContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Pre-submit gate that resolves tenant membership and enforces quota limits.
 * Implements fail-close semantics: unmapped users are denied by default.
 *
 * @throws PangrovaException with TENANT_NOT_FOUND if user has no tenant mapping (fail-close)
 * @throws TenantQuotaExceededException if quota exceeded or tenant suspended
 */
public final class TenantQueryGuard {

    private static final Logger LOG = LoggerFactory.getLogger(TenantQueryGuard.class);

    private final TenantResolver resolver;
    private final QuotaChecker quotaChecker;
    private final TenantUsageProvider usageProvider;

    public TenantQueryGuard(TenantResolver resolver, QuotaChecker quotaChecker,
                            TenantUsageProvider usageProvider) {
        this.resolver = checkNotNull(resolver, "resolver must not be null");
        this.quotaChecker = checkNotNull(quotaChecker, "quotaChecker must not be null");
        this.usageProvider = checkNotNull(usageProvider, "usageProvider must not be null");
    }

    /**
     * Resolves tenant and checks quota. Returns tenantId on success.
     * Fail-close: denies access when identity is null or has no tenant mapping.
     *
     * @throws PangrovaException with TENANT_NOT_FOUND if user has no tenant mapping
     * @throws TenantQuotaExceededException if quota exceeded or tenant suspended
     */
    public String checkAndResolve(UserIdentity identity) {
        if (identity == null) {
            LOG.warn("event=tenant_guard_deny reason=null_identity");
            throw new PangrovaException(PangrovaErrorCode.TENANT_NOT_FOUND,
                    "Access denied: no user identity provided (fail-close)");
        }
        Optional<ResolvedTenantContext> ctx = resolver.resolve(identity);
        if (ctx.isEmpty()) {
            LOG.warn("event=tenant_guard_deny reason=unmapped_tenant user={}",
                    identity.principal());
            throw new PangrovaException(PangrovaErrorCode.TENANT_NOT_FOUND,
                    "Access denied: user '%s' has no tenant mapping (fail-close)"
                            .formatted(identity.principal()));
        }
        ResolvedTenantContext tenant = ctx.get();
        rejectIfSuspended(tenant);
        enforceQuota(tenant);
        LOG.debug("Tenant resolved: tenantId={}, user={}", tenant.tenant().tenantId(), identity.principal());
        return tenant.tenant().tenantId();
    }

    private void rejectIfSuspended(ResolvedTenantContext ctx) {
        if (ctx.tenant().status() == TenantStatus.SUSPENDED) {
            throw new TenantQuotaExceededException(
                    "Tenant '%s' is suspended".formatted(ctx.tenant().tenantId()));
        }
    }

    private void enforceQuota(ResolvedTenantContext ctx) {
        TenantUsage usage = usageProvider.getUsage(ctx.tenant().tenantId());
        QuotaCheckResult result = quotaChecker.checkBeforeSubmit(ctx.tenant().quota(), usage);
        if (!result.passed()) {
            throw new TenantQuotaExceededException(
                    "Tenant '%s' quota exceeded: %s".formatted(
                            ctx.tenant().tenantId(), String.join(", ", result.violations())));
        }
    }

    /**
     * SPI for retrieving current tenant resource usage snapshot.
     */
    @FunctionalInterface
    public interface TenantUsageProvider {
        TenantUsage getUsage(String tenantId);
    }
}
