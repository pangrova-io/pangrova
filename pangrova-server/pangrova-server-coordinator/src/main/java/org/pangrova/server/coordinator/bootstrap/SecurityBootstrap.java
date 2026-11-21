package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.exception.BootstrapFailedException;
import org.pangrova.security.authentication.AuthenticationManager;
import org.pangrova.security.api.authentication.Authenticator;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.persistence.coordination.api.admission.ClusterConcurrencySlots;
import org.pangrova.persistence.coordination.postgresql.admission.JdbcClusterConcurrencySlots;
import org.pangrova.scheduler.admission.TenantQuotaAdmissionController;
import org.pangrova.persistence.security.api.tenant.TenantStore;
import org.pangrova.scheduler.api.quota.GlobalTenantUsageView;
import org.pangrova.security.api.authorization.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

/**
 * Bootstraps the security subsystem (authentication, authorization, access control).
 * Discovers the configured authenticator through the security API.
 */
final class SecurityBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityBootstrap.class);

    private final CoordinatorBootstrapConfig config;
    private final DataSource dataSource;
    private final java.util.function.Function<DataSource,
            org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore> maskStores;

    private final java.util.function.Function<DataSource,
            org.pangrova.persistence.security.api.rowfilter.RowPolicyStore> rowPolicyStores;

    private AuthenticationManager authenticationManager;
    private AccessControl accessControl;

    SecurityBootstrap(CoordinatorBootstrapConfig config, DataSource dataSource,
            java.util.function.Function<DataSource,
                    org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore> maskStores,
            java.util.function.Function<DataSource,
                    org.pangrova.persistence.security.api.rowfilter.RowPolicyStore> rowPolicyStores) {
        this.config = config;
        this.dataSource = dataSource;
        this.maskStores = java.util.Objects.requireNonNull(maskStores, "maskStores");
        this.rowPolicyStores = java.util.Objects.requireNonNull(rowPolicyStores, "rowPolicyStores");
    }

    void start() {
        if (!config.securityEnabled()) {
            LOG.info("Phase skipped: security not configured");
            return;
        }
        try {
            authenticationManager = startAuthentication(config.securityConfig());
            accessControl = buildAccessControl();
            LOG.info("Phase: security — initialized (authenticator={})",
                    config.securityConfig().requireAuthenticator().providerId());
        } catch (Exception e) {
            try {
                shutdown();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw new BootstrapFailedException("Security", e);
        }
    }

    void shutdown() {
        AuthenticationManager previous = authenticationManager;
        authenticationManager = null;
        accessControl = null;
        if (previous != null) {
            previous.close();
        }
    }

    static AuthenticationManager startAuthentication(CoordinatorBootstrapConfig.SecurityConfig config) {
        if (config == null) {
            throw new IllegalStateException("Security enabled but no authenticator configured");
        }
        Authenticator authenticator = java.util.Objects.requireNonNull(config.requireAuthenticator().create(),
                "authenticator");
        try {
            boolean bypass = config.bypassesAuthentication();
            if (bypass != authenticator.bypassesAuthentication()) {
                throw new IllegalStateException("Authenticator bypass capability conflicts with configured provider "
                        + config.requireAuthenticator().providerId());
            }
            if (bypass) {
                LOG.error("event=security_alarm severity=CRITICAL component=authentication "
                        + "message=\"Authentication is DISABLED (authenticator=NOOP/bypass): ALL "
                        + "requests are accepted without authentication. This MUST NOT be used in production.\"");
            }
            authenticator.init(bypass ? Map.of("bypass.enabled", "true") : config.authenticatorProperties());
            return new AuthenticationManager(List.of(authenticator));
        } catch (RuntimeException | Error failure) {
            try {
                authenticator.close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    private AccessControl buildAccessControl() {
        if (dataSource == null) {
            throw new IllegalStateException("Security requires the authoritative PostgreSQL datasource");
        }
        JdbcRoleStore roles = new JdbcRoleStore(dataSource);
        JdbcGrantStore grants = new JdbcGrantStore(dataSource);
        // A missing or unavailable authority prevents the process from serving secured traffic.
        roles.listRoles();
        grants.listGrants();
        var maskStore = maskStores.apply(dataSource);
        return new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(rowPolicyStores.apply(dataSource)),
                new ColumnMaskingProcessor(maskStore));
    }

    AuthenticationManager authenticationManager() {
        return authenticationManager;
    }

    AccessControl accessControl() {
        return accessControl;
    }

    /**
     * Returns true if the security subsystem is healthy and operational.
     * Checks that authentication and authorization components are initialized.
     */
    boolean isHealthy() {
        if (!config.securityEnabled()) {
            return true; // security disabled = healthy by definition
        }
        return authenticationManager != null && accessControl != null;
    }

    /**
     * P1-5b: builds the cluster-wide tenant admission controller that enforces per-tenant budget
     * limits on the query submission path, using the heartbeat-fed global usage view aggregated by
     * {@code ClusterTenantUsageAggregator}.
     *
     * <p>A-ADMISSION-01: with a shared metadata store, cluster-wide concurrency is enforced via
     * atomic slot reservation, which eliminates the oversell the lagged heartbeat view permits.
     * Without a shared database (dev / in-memory) the slot store stays null and the controller
     * falls back to the view alone.</p>
     *
     * @return {@code null} when tenant awareness is not configured, meaning no admission control
     */
    static TenantQuotaAdmissionController createClusterAdmissionController(
            TenantStore tenantStore, GlobalTenantUsageView usageView, DataSource metadataDataSource) {
        if (tenantStore == null || usageView == null) {
            return null;
        }
        ClusterConcurrencySlots clusterSlots = metadataDataSource != null
                ? new JdbcClusterConcurrencySlots(metadataDataSource) : null;
        return new TenantQuotaAdmissionController(
                tenantStore, usageView, clusterSlots);
    }
}
