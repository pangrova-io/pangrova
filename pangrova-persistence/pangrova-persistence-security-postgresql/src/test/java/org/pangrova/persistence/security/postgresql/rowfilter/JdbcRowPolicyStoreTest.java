package org.pangrova.persistence.security.postgresql.rowfilter;

import org.junit.jupiter.api.Test;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcRowPolicyStoreTest extends ContainerTestBase {
    @Test
    void stableIdentitySurvivesReopeningAndNameUpdatesWithoutDuplicatingThePolicy() throws Exception {
        var source = MigratedPostgres.create();
        var store = new JdbcRowPolicyStore(source);
        UUID tableId = UUID.randomUUID();
        store.savePolicy(policy(tableId, "orders", "id > 0", true));
        var original = store.getPolicies(tableId).get(0);
        store.savePolicy(policy(tableId, "renamed", "id > 10", true));
        var reopened = new JdbcRowPolicyStore(source);
        assertThat(reopened.getPolicies(tableId)).singleElement().satisfies(value -> {
            assertThat(value.policyId()).isEqualTo(original.policyId());
            assertThat(value.tableName()).isEqualTo("renamed");
            assertThat(value.filterExpression()).isEqualTo("id > 10");
        });
        assertThat(reopened.getPolicies("cat", "db", "orders")).isEmpty();
    }

    @Test
    void sameNameUnderAnotherIdentityCannotReplaceOrDeleteTheOriginalPolicy() throws Exception {
        var store = new JdbcRowPolicyStore(MigratedPostgres.create());
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        store.savePolicy(policy(first, "orders", "id > 0", true));
        store.savePolicy(policy(second, "orders", "id > 10", true));
        var original = store.getPolicies(first).get(0);
        store.deletePolicy(original.policyId());
        assertThat(store.getPolicies(first)).isEmpty();
        assertThat(store.getPolicies(second)).singleElement()
                .satisfies(value -> assertThat(value.filterExpression()).isEqualTo("id > 10"));
        store.savePolicy(policy(second, "orders", "id > 10", false));
        assertThat(store.getPolicies(second)).isEmpty();
    }

    @Test
    void unboundSecurityOnlyPoliciesKeepNameIsolationAndCannotBeReadByStableIdentity() throws Exception {
        var store = new JdbcRowPolicyStore(MigratedPostgres.create());
        store.savePolicy(policy(null, "orders", "id > 0", true));
        assertThat(store.getPolicies("cat", "db", "orders")).hasSize(1);
        assertThat(store.getPolicies(UUID.randomUUID())).isEmpty();
        assertThatThrownBy(() -> store.savePolicy(policy(null, "orders", "id > 10", true)))
                .isInstanceOf(IllegalStateException.class);
    }

    private static RowPolicy policy(UUID tableId, String table, String expression, boolean enabled) {
        return new RowPolicy(0, "rows", "cat", "db", table, expression, List.of("reader"), enabled, 1, tableId);
    }
}
