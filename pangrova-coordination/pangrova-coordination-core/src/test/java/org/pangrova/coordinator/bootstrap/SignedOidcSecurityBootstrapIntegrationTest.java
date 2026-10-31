package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.security.api.authn.AuthenticationRequest;
import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.store.jdbc.PersistentGrantStore;
import org.pangrova.security.store.jdbc.PersistentRoleStore;
import org.pangrova.security.store.jdbc.PersistentRowPolicyStore;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production configuration and bootstrap with real signed JWKS and PostgreSQL policy storage. */
class SignedOidcSecurityBootstrapIntegrationTest extends ContainerTestBase {
    @Test
    void signedIdentityAndNamedGrantAreEffectiveAndShutdownFailsClosed() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("signed_oidc");
        new PersistentRowPolicyStore(dataSource).initSchema();
        var roles = new PersistentRoleStore(dataSource);
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        var grants = new PersistentGrantStore(dataSource);
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "lake", "public", "facts", null));
        try (var issuer = new org.pangrova.coordinator.testinfra.SignedOidcIssuer()) {
            var security = SecurityConfigurationLoader.load(PangrovaConfiguration.fromYaml(Map.of(
                    "security.enabled", "true", "security.auth.type", "OAUTH2",
                    "security.auth.properties.issuer-uri", issuer.uri(),
                    "security.auth.properties.audience", "pangrova")));
            var bootstrap = new SecurityBootstrap(configWith(security), dataSource, org.pangrova.security.store.jdbc.PersistentColumnMaskStore::new,
                    org.pangrova.security.store.jdbc.PersistentRowPolicyStore::new);
            try {
                bootstrap.start();
                assertThat(bootstrap.isHealthy()).isTrue();
                var manager = bootstrap.authenticationManager();
                var result = manager.authenticate(request(issuer.token("pangrova", true)));
                assertThat(result.success()).isTrue();
                assertThat(result.userIdentity().principal()).isEqualTo("alice");
                assertThat(result.userIdentity().groups()).containsExactly("directory-analysts");
                assertThatCode(() -> bootstrap.accessControl().checkCanSelectFromTable(
                        result.userIdentity(), "lake", "public", "facts")).doesNotThrowAnyException();
                assertThatThrownBy(() -> bootstrap.accessControl().checkCanInsertIntoTable(
                        result.userIdentity(), "lake", "public", "facts"))
                        .isInstanceOf(AuthorizationDeniedException.class);
                grants.revokeGrants("reader", Privilege.SELECT, "lake", "public", "facts");
                assertThatThrownBy(() -> bootstrap.accessControl().checkCanSelectFromTable(
                        result.userIdentity(), "lake", "public", "facts"))
                        .isInstanceOf(AuthorizationDeniedException.class);
                assertThat(manager.authenticate(request(issuer.token("another-audience", true))).success()).isFalse();
                for (int attempt = 0; attempt < 8; attempt++) {
                    assertThat(manager.authenticate(request(issuer.token("pangrova", false))).success()).isFalse();
                }
                assertThat(manager.authenticate(request(issuer.token("pangrova", true))).success()).isTrue();
                bootstrap.shutdown();
                assertThat(bootstrap.isHealthy()).isFalse();
                assertThat(manager.authenticate(request(issuer.token("pangrova", true))).success()).isFalse();
            } finally {
                bootstrap.shutdown();
            }
        }
    }

    private static AuthenticationRequest request(String token) {
        return new AuthenticationRequest(null, null, token, null, Map.of(), "127.0.0.1");
    }

    private static CoordinatorBootstrapConfig configWith(CoordinatorBootstrapConfig.SecurityConfig security) {
        return new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("signed-oidc", 0, 30_000, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(), CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), null, security, null);
    }

}
