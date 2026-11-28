package org.pangrova.persistence.security.postgresql.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Verifies policy role encoding; canonical schema behavior is covered by real PostgreSQL tests.
 */
class JdbcPolicyStoreBaseTest {

    @Test
    void rolesRoundTripThroughCommaSeparatedForm() {
        assertThat(JdbcPolicyStoreBase.encodeRoles(List.of())).isEmpty();
        assertThat(JdbcPolicyStoreBase.encodeRoles(List.of("a", "b"))).isEqualTo("a,b");
        assertThat(JdbcPolicyStoreBase.decodeRoles("")).isEmpty();
        assertThat(JdbcPolicyStoreBase.decodeRoles("a,b")).containsExactly("a", "b");
    }
}
