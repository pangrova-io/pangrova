package org.pangrova.coordinator.testinfra;

import org.junit.jupiter.api.AfterEach;
import org.pangrova.testing.testkit.ContainerTestBase;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantRole;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.spi.security.tenant.ResourceQuota;
import org.postgresql.ds.PGSimpleDataSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Owns a real OIDC issuer only for tests that configure secured coordinator assembly. */
public abstract class SignedOidcCoordinatorTestBase extends ContainerTestBase {
    private SignedOidcIssuer issuer;

    /** Starts the test-owned issuer lazily and returns its real discovery URI. */
    protected String issuerUri() {
        if (issuer == null) {
            try {
                issuer = new SignedOidcIssuer();
            } catch (Exception failure) {
                throw new IllegalStateException("Cannot start signed OIDC test issuer", failure);
            }
        }
        return issuer.uri();
    }

    /** Configures real HTTP control authentication without granting query permissions or enabling tenancy. */
    protected CoordinatorBootstrapConfig withControlAuthentication(CoordinatorBootstrapConfig original) {
        var flags = original.featureFlags();
        var securedFlags = new CoordinatorBootstrapConfig.FeatureFlags(flags.observability(), true,
                flags.metadata() && original.metadataJdbcConfig() != null,
                flags.streaming(), flags.governance(), flags.resilience(), flags.tenant(), flags.operations(),
                flags.speculation(), flags.pullModeSplitAssignment(), flags.cacheWriteback(), flags.indexing(),
                flags.embedding(), flags.ddlLeaderRouting(), flags.shuffleMode(), flags.shuffleEndpoints());
        var security = new CoordinatorBootstrapConfig.SecurityConfig(
                CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.OAUTH2,
                "bootstrap-test-internal-secret", Map.of("issuerUri", issuerUri(), "audience", "bootstrap-tests"));
        return SecuredCoordinatorFixtures.withAuthority(new CoordinatorBootstrapConfig(
                loopbackCore(original.core()), securedFlags, original.query(), original.transport(), original.workerHealth(),
                original.metadataJdbcConfig(), security, original.observabilityBootstrapConfig()));
    }

    /** Starts the real secured pipeline with an explicit active tenant and signed subject. */
    protected CoordinatorBootstrap startAuthorizedCoordinator(CoordinatorBootstrapConfig original) {
        return startAuthorizedCoordinator(original, new ResourceQuota(
                1_073_741_824L, 10, 10, 60_000, 1_073_741_824L,
                1_000, 1_099_511_627_776L, 1_099_511_627_776L));
    }

    /** Configures explicit admission limits without changing authentication or policy enforcement. */
    protected CoordinatorBootstrap startAuthorizedCoordinator(
            CoordinatorBootstrapConfig original, ResourceQuota quota) {
        var flags = original.featureFlags();
        var securedFlags = new CoordinatorBootstrapConfig.FeatureFlags(flags.observability(), true,
                flags.metadata() && original.metadataJdbcConfig() != null,
                flags.streaming(), flags.governance(), flags.resilience(), true, flags.operations(),
                flags.speculation(), flags.pullModeSplitAssignment(), flags.cacheWriteback(), flags.indexing(),
                flags.embedding(), flags.ddlLeaderRouting(), flags.shuffleMode(), flags.shuffleEndpoints());
        var security = new CoordinatorBootstrapConfig.SecurityConfig(
                CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.OAUTH2,
                "bootstrap-test-internal-secret", Map.of("issuerUri", issuerUri(), "audience", "bootstrap-tests"));
        var config = SecuredCoordinatorFixtures.withAuthority(new CoordinatorBootstrapConfig(
                loopbackCore(original.core()), securedFlags, original.query(), original.transport(), original.workerHealth(),
                original.metadataJdbcConfig(), security, original.observabilityBootstrapConfig()));
        var bootstrap = new CoordinatorBootstrap(config);
        try {
            bootstrap.start();
            var jdbc = config.metadataJdbcConfig();
            var source = new PGSimpleDataSource();
            source.setURL(jdbc.jdbcUrl());
            source.setUser(jdbc.username());
            source.setPassword(jdbc.password());
            var tenants = new JdbcTenantStore(source);
            tenants.createTenant(new Tenant("bootstrap-fixture", "bootstrap-fixture", TenantStatus.ACTIVE,
                    quota, List.of(), List.of(), null, Map.of(), Instant.EPOCH, "alice"));
            tenants.addUserToTenant("bootstrap-fixture", "alice", TenantRole.MEMBER);
            return bootstrap;
        } catch (RuntimeException failure) {
            try {
                bootstrap.shutdown();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static CoordinatorBootstrapConfig.CoreConfig loopbackCore(CoordinatorBootstrapConfig.CoreConfig core) {
        return new CoordinatorBootstrapConfig.CoreConfig(core.coordinatorId(), core.httpPort(), core.shutdownTimeoutMs(),
                core.role(), "127.0.0.1");
    }

    /** Signs a request for the exact issuer/audience configured above. */
    protected String authorizationHeader() throws Exception {
        issuerUri();
        return "Bearer " + issuer.token("bootstrap-tests", true);
    }

    /** Stops the issuer after the subclass has closed its coordinator. */
    @AfterEach
    @AfterProperty
    protected void closeOidcIssuer() throws Exception {
        if (issuer != null) {
            issuer.close();
            issuer = null;
        }
    }
}
