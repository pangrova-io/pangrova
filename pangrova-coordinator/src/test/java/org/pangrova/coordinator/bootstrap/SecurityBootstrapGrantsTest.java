package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig.GrantEntry;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that SecurityBootstrap loads initial grants from config
 * and wires them into AccessControl so permissions are effective.
 */
class SecurityBootstrapGrantsTest {

    @Test
    void grantsFromConfig_areEffectiveAfterStart() {
        List<GrantEntry> grants = List.of(
                new GrantEntry("admin", "SELECT", "TABLE", "*", "*", "*", null),
                new GrantEntry("admin", "INSERT", "TABLE", "*", "*", "*", null),
                new GrantEntry("admin", "DELETE", "TABLE", "*", "*", "*", null),
                new GrantEntry("analyst", "SELECT", "TABLE", "default", "*", "*", null));

        SecurityConfig secCfg = new SecurityConfig(AuthenticatorType.NOOP, grants);
        CoordinatorBootstrapConfig config = configWith(secCfg);
        SecurityBootstrap bootstrap = new SecurityBootstrap(config, null);
        bootstrap.start();

        AccessControl ac = bootstrap.accessControl();
        assertThat(ac).isNotNull();

        UserIdentity admin = new UserIdentity("admin-user", Set.of("admin"), Map.of(), Instant.now());
        UserIdentity analyst = new UserIdentity("analyst-user", Set.of("analyst"), Map.of(), Instant.now());

        // admin can SELECT, INSERT, DELETE on any table
        assertThatCode(() -> ac.checkCanSelectFromTable(admin, "prod", "public", "orders"))
                .doesNotThrowAnyException();
        assertThatCode(() -> ac.checkCanInsertIntoTable(admin, "prod", "public", "orders"))
                .doesNotThrowAnyException();
        assertThatCode(() -> ac.checkCanDeleteFromTable(admin, "prod", "public", "orders"))
                .doesNotThrowAnyException();

        // analyst can SELECT on default catalog only
        assertThatCode(() -> ac.checkCanSelectFromTable(analyst, "default", "sales", "revenue"))
                .doesNotThrowAnyException();

        // analyst cannot INSERT
        assertThatThrownBy(() -> ac.checkCanInsertIntoTable(analyst, "default", "sales", "revenue"))
                .isInstanceOf(AuthorizationDeniedException.class);

        // analyst cannot SELECT on non-default catalog
        assertThatThrownBy(() -> ac.checkCanSelectFromTable(analyst, "prod", "public", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void noGrants_securityStartsWithEmptyPermissions() {
        SecurityConfig secCfg = SecurityConfig.noop();
        CoordinatorBootstrapConfig config = configWith(secCfg);
        SecurityBootstrap bootstrap = new SecurityBootstrap(config, null);
        bootstrap.start();

        AccessControl ac = bootstrap.accessControl();
        assertThat(ac).isNotNull();

        UserIdentity user = new UserIdentity("someone", Set.of("viewer"), Map.of(), Instant.now());
        assertThatThrownBy(() -> ac.checkCanSelectFromTable(user, "cat", "sch", "tbl"))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void securityDisabled_accessControlIsNull() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("coord-1", 0);
        SecurityBootstrap bootstrap = new SecurityBootstrap(config, null);
        bootstrap.start();

        assertThat(bootstrap.accessControl()).isNull();
    }

    private static CoordinatorBootstrapConfig configWith(SecurityConfig secCfg) {
        return new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("coord-test", 0, 30_000),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, secCfg, null);
    }
}
