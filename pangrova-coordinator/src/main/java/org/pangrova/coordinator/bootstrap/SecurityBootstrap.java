package org.pangrova.coordinator.bootstrap;

import org.pangrova.operations.bootstrap.BootstrapFailedException;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.Authenticator;
import org.pangrova.security.authn.LdapAuthenticator;
import org.pangrova.security.authn.BypassAuthenticator;
import org.pangrova.security.authn.OAuth2Authenticator;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.mask.EnrichedColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicyStore;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.security.tenant.quota.ClusterConcurrencySlots;
import org.pangrova.security.tenant.quota.JdbcClusterConcurrencySlots;
import org.pangrova.security.tenant.quota.QuotaChecker;
import org.pangrova.security.tenant.quota.TenantQuotaAdmissionController;
import org.pangrova.security.tenant.store.TenantStore;
import org.pangrova.security.event.SecurityPolicyEventPublisher;
import org.pangrova.spi.resource.tenant.GlobalTenantUsageView;
import org.pangrova.spi.security.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.sql.DataSource;

/**
 * Bootstraps the security subsystem (authentication, authorization, access control).
 * Supports NoOp, OAuth2, and LDAP authenticator types.
 */
final class SecurityBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityBootstrap.class);

    private final CoordinatorBootstrapConfig config;
    private final DataSource dataSource;

    private AuthenticationManager authenticationManager;
    private AccessControl accessControl;

    SecurityBootstrap(CoordinatorBootstrapConfig config, DataSource dataSource) {
        this.config = config;
        this.dataSource = dataSource;
    }

    void start() {
        if (!config.securityEnabled()) {
            LOG.info("Phase skipped: security not configured");
            return;
        }
        try {
            Authenticator authenticator = buildAuthenticator();
            Map<String, String> authnConfig;
            if (authenticator instanceof BypassAuthenticator) {
                // F-SEC-04: NOOP maps to BypassAuthenticator, which DISABLES authentication.
                // This injection is scoped strictly to the explicit AuthenticatorType.NOOP
                // path — any other accidental wiring of BypassAuthenticator receives an empty
                // config and is stopped by the authenticator's own opt-in guard (preserving
                // defense-in-depth). Because a coordinator with security ENABLED but auth
                // bypassed is a dangerous posture, emit an unmissable CRITICAL alarm so it can
                // never happen silently in production.
                LOG.error("event=security_alarm severity=CRITICAL component=authentication "
                        + "message=\"Authentication is DISABLED (authenticator=NOOP/bypass): ALL "
                        + "requests are accepted without authentication. This MUST NOT be used in "
                        + "production. Configure OAUTH2/LDAP, or disable the security feature flag "
                        + "for an explicitly unsecured deployment.\"");
                authnConfig = Map.of("bypass.enabled", "true");
            } else {
                authnConfig = Map.of();
            }
            authenticator.init(authnConfig);
            authenticationManager = new AuthenticationManager(List.of(authenticator));
            accessControl = buildAccessControl();
            LOG.info("Phase: security — initialized (authenticator={})", authenticator.getName());
        } catch (Exception e) {
            throw new BootstrapFailedException("Security", e);
        }
    }

    void shutdown() {
        // Security components have no explicit shutdown lifecycle
    }

    private Authenticator buildAuthenticator() {
        CoordinatorBootstrapConfig.SecurityConfig secCfg = config.securityConfig();
        if (secCfg == null) {
            throw new BootstrapFailedException(
                    "Security enabled but no authenticator configured",
                    new IllegalStateException("securityConfig is null while security is enabled"));
        }
        Authenticator authenticator = switch (secCfg.authenticatorType()) {
            case OAUTH2 -> new OAuth2Authenticator();
            case LDAP -> new LdapAuthenticator();
            case NOOP -> new BypassAuthenticator();
        };
        return authenticator;
    }

    private AccessControl buildAccessControl() {
        RowPolicyStore rowPolicyStore;
        EnrichedColumnMaskStore columnMaskStore;
        if (dataSource != null) {
            rowPolicyStore = new PersistentRowPolicyStore(dataSource);
            columnMaskStore = new PersistentColumnMaskStore(dataSource);
            LOG.info("Phase: security — using persistent RLS/masking stores (JDBC-backed)");
        } else {
            // F-SEC-02: with security ENABLED but no datasource, RLS/column-masking policy
            // stores are empty, so every row filter and column mask is silently disabled.
            // Raise this to a CRITICAL security alarm (not a mere warning) so an operator who
            // believes policies are enforced cannot miss that they are not.
            LOG.error("event=security_alarm severity=CRITICAL component=access_control "
                    + "message=\"No datasource configured; row-level security and column-masking "
                    + "stores are EMPTY. ALL RLS filters and column masks are DISABLED and policies "
                    + "will NOT be enforced. Configure a JDBC datasource to enable policy "
                    + "enforcement before serving production traffic.\"");
            rowPolicyStore = new EmptyRowPolicyStore();
            columnMaskStore = new EmptyColumnMaskStore();
        }
        RowLevelSecurityFilter rlsFilter = new RowLevelSecurityFilter(rowPolicyStore);
        ColumnMaskingProcessor maskProcessor = new ColumnMaskingProcessor(columnMaskStore);
        List<PrivilegeGrant> grants = loadGrants();
        if (!grants.isEmpty()) {
            LOG.info("Phase: security — loaded {} initial grants from config", grants.size());
        }
        // F-SEC-05: Use DynamicRbacAccessControl for runtime grant refresh support.
        // DynamicRbacAccessControl accepts a GrantStore and publishes policy events,
        // enabling GRANT/REVOKE statements to take effect without coordinator restart.
        StaticGrantStore grantStore = new StaticGrantStore(grants);
        SecurityPolicyEventPublisher eventPublisher = new SecurityPolicyEventPublisher();
        return new DynamicRbacAccessControl(grantStore, rlsFilter, maskProcessor, eventPublisher);
    }

    private List<PrivilegeGrant> loadGrants() {
        CoordinatorBootstrapConfig.SecurityConfig secCfg = config.securityConfig();
        if (secCfg == null || secCfg.grants().isEmpty()) {
            return List.of();
        }
        return secCfg.grants().stream()
                .map(SecurityBootstrap::toPrivilegeGrant)
                .toList();
    }

    private static PrivilegeGrant toPrivilegeGrant(CoordinatorBootstrapConfig.SecurityConfig.GrantEntry entry) {
        Privilege privilege = Privilege.valueOf(entry.privilege().toUpperCase(Locale.ROOT));
        return PrivilegeGrant.of(
                entry.role(),
                privilege,
                entry.objectType(),
                entry.catalog(),
                entry.schema(),
                entry.table(),
                entry.column());
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
                tenantStore, new QuotaChecker(), usageView, clusterSlots);
    }
}
