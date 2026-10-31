package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;

/** Supplies real PostgreSQL authority and test-only internal authentication to live bootstraps. */
public final class SecuredCoordinatorFixtures {
    private SecuredCoordinatorFixtures() {}

    /** Preserves feature intent while adding the mandatory live-bootstrap security dependencies. */
    public static CoordinatorBootstrapConfig withAuthority(CoordinatorBootstrapConfig config) {
        var flags = config.featureFlags();
        boolean secureQuery = config.role().isQuery();
        var persistent = new CoordinatorBootstrapConfig.FeatureFlags(
                flags.observability(), flags.security() || secureQuery,
                flags.metadata() || secureQuery,
                flags.streaming(), flags.governance(), flags.resilience(), flags.tenant(), flags.operations(),
                flags.speculation(), flags.pullModeSplitAssignment(), flags.cacheWriteback(), flags.indexing(),
                flags.embedding(), flags.ddlLeaderRouting(), flags.shuffleMode(), flags.shuffleEndpoints());
        var security = config.securityConfig();
        if (secureQuery && security == null) {
            security = new CoordinatorBootstrapConfig.SecurityConfig(
                    CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.NOOP,
                    "test-internal-service-secret", java.util.Map.of());
        } else if (secureQuery && (security.internalAuthSecret() == null
                || security.internalAuthSecret().isBlank())) {
            security = new CoordinatorBootstrapConfig.SecurityConfig(
                    security.authenticatorType(), "test-internal-service-secret",
                    security.authenticatorProperties());
        }
        if (persistent.equals(flags) && security == config.securityConfig()) {
            return config;
        }
        try {
            var jdbc = config.metadataJdbcConfig();
            if (persistent.metadata() && jdbc == null) {
                var source = (PGSimpleDataSource) SharedPostgresContainer.isolatedDataSource(
                        "secured_fixture");
                jdbc = new CoordinatorBootstrapConfig.MetadataJdbcConfig(
                        source.getURL(), source.getUser(), source.getPassword());
            }
            return new CoordinatorBootstrapConfig(config.core(), persistent, config.query(), config.transport(),
                    config.workerHealth(), jdbc, security, config.observabilityBootstrapConfig());
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot initialize secured fixture authority", failure);
        }
    }
}
