package org.pangrova.persistence.security.postgresql.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Verifies policy role encoding; canonical schema behavior is covered by real PostgreSQL tests.
 */
class JdbcPolicyStoreSupportTest {

    @Test
    void rolesRoundTripThroughCommaSeparatedForm() {
        assertThat(JdbcPolicyStoreSupport.encodeRoles(List.of())).isEmpty();
        assertThat(JdbcPolicyStoreSupport.encodeRoles(List.of("a", "b"))).isEqualTo("a,b");
        assertThat(JdbcPolicyStoreSupport.decodeRoles("")).isEmpty();
        assertThat(JdbcPolicyStoreSupport.decodeRoles("a,b")).containsExactly("a", "b");
    }
}
