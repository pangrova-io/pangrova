package org.pangrova.persistence.security.postgresql.rowfilter;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcReadPolicyStore;
import org.pangrova.security.api.mask.ColumnMask;
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

    @Test
    void currentTableReadIncludesRenamedBindingAndUnboundNameOnly() throws Exception {
        var store = new JdbcRowPolicyStore(MigratedPostgres.create());
        UUID current = UUID.randomUUID();
        store.savePolicy(policy(current, "old_name", "id > 0", true));
        store.savePolicy(policy(UUID.randomUUID(), "orders", "id > 10", true));
        store.savePolicy(policy(null, "orders", "id > 20", true));
        store.savePolicy(policy(null, "other_table", "id > 30", true));

        assertThat(store.getPoliciesForCurrentTable("cat", "db", "orders", current))
                .extracting(RowPolicy::filterExpression).containsExactly("id > 0", "id > 20");
        assertThat(store.getPoliciesForCurrentTable("cat", "db", "orders", null))
                .extracting(RowPolicy::filterExpression).containsExactly("id > 20");
    }

    @Test
    void combinedReadKeepsRowIdentityScopeAndTheExactNamedMasks() throws Exception {
        var source = MigratedPostgres.create();
        var rows = new JdbcRowPolicyStore(source);
        var masks = new JdbcColumnMaskStore(source);
        var readPolicies = new JdbcReadPolicyStore(source);
        UUID current = UUID.randomUUID();
        rows.savePolicy(policy(current, "old_name", "id > 0", true));
        rows.savePolicy(policy(UUID.randomUUID(), "orders", "id > 10", true));
        rows.savePolicy(policy(null, "orders", "id > 20", true));
        rows.savePolicy(policy(null, "other_table", "id > 30", true));
        rows.savePolicy(new RowPolicy(0, "disabled", "cat", "db", "orders", "id > 40",
                List.of("reader"), false, 50, current));
        masks.saveColumnMask(new ColumnMask(0, "hide_payload", "cat", "db", "orders", "payload",
                "'masked'", List.of("reader"), List.of("admin"), true, current));
        masks.saveColumnMask(new ColumnMask(0, "disabled", "cat", "db", "orders", "id",
                "0", List.of("reader"), List.of(), false, current));
        masks.saveColumnMask(new ColumnMask(0, "other_table", "cat", "db", "other_table", "payload",
                "'hidden'", List.of("reader"), List.of(), true, null));

        var managed = readPolicies.read("cat", "db", "orders", current);
        assertThat(managed.rows()).containsExactlyElementsOf(
                rows.getPoliciesForCurrentTable("cat", "db", "orders", current));
        assertThat(managed.rows()).hasSize(2);
        assertThat(managed.masks()).containsExactlyElementsOf(masks.getTableMasks("cat", "db", "orders"));
        assertThat(managed.masks()).hasSize(1);

        var external = readPolicies.read("cat", "db", "orders", null);
        assertThat(external.rows()).extracting(RowPolicy::filterExpression).containsExactly("id > 20");
        assertThat(external.masks()).isEqualTo(managed.masks());
        var absent = readPolicies.read("cat", "db", "absent", null);
        assertThat(absent.rows()).isEmpty();
        assertThat(absent.masks()).isEmpty();
    }

    private static RowPolicy policy(UUID tableId, String table, String expression, boolean enabled) {
        return new RowPolicy(0, "rows", "cat", "db", table, expression, List.of("reader"), enabled, 1, tableId);
    }
}
