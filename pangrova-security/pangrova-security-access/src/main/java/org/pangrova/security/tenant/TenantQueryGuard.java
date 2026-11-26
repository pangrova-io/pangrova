package org.pangrova.security.tenant;

import org.pangrova.security.exception.TenantQuotaExceededException;

import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Pre-submit gate that resolves tenant membership and enforces quota limits.
 * Implements fail-close semantics: unmapped users are denied by default.
 *
 * @throws AuthorizationDeniedException if there is no authenticated tenant membership (fail-close)
 * @throws TenantQuotaExceededException if quota exceeded or tenant inactive
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
     * Resolves tenant and checks quota. Returns the membership and tenant from that authority read.
     * Fail-close: denies access when identity is null or has no tenant mapping.
     *
     * @throws AuthorizationDeniedException if there is no authenticated tenant membership
     * @throws TenantQuotaExceededException if quota exceeded or tenant inactive
     */
    public ResolvedTenantContext checkAndResolve(UserIdentity identity) {
        if (identity == null) {
            LOG.warn("event=tenant_guard_deny reason=null_identity");
            throw new AuthorizationDeniedException(
                    "Access denied: no user identity provided (fail-close)");
        }
        Optional<ResolvedTenantContext> ctx = resolver.resolve(identity);
        if (ctx.isEmpty()) {
            LOG.warn("event=tenant_guard_deny reason=unmapped_tenant user={}",
                    identity.principal());
            throw new AuthorizationDeniedException(
                    "Access denied: user '%s' has no tenant mapping (fail-close)"
                            .formatted(identity.principal()));
        }
        ResolvedTenantContext tenant = ctx.get();
        requireActiveTenant(tenant);
        enforceQuota(tenant);
        LOG.debug("Tenant resolved: tenantId={}, user={}", tenant.tenant().tenantId(), identity.principal());
        return tenant;
    }

    private void requireActiveTenant(ResolvedTenantContext ctx) {
        if (!ctx.isActive()) {
            throw new TenantQuotaExceededException(
                    "Tenant '%s' is %s".formatted(ctx.tenantId(), ctx.tenant().status()));
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
