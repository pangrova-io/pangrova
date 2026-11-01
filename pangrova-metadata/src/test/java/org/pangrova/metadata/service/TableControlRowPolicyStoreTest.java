package org.pangrova.metadata.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.table.api.control.CatalogDeploymentIdentity;
import org.pangrova.table.store.jdbc.control.JdbcTableControlStore;
import org.pangrova.table.core.control.TableControlService;
import org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.persistence.jdbc.security.rowfilter.JdbcRowPolicyStore;
import org.pangrova.spi.connector.metadata.table.TableIdentity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlRowPolicyStoreTest {
    private javax.sql.DataSource source;
    private CatalogRegistry catalogs;
    private JdbcMetadataStore metadata;
    private TableControlService control;
    private JdbcRowPolicyStore persisted;
    private TableControlRowPolicyStore policies;

    @BeforeEach
    void setUp() throws Exception {
        source = SharedPostgresContainer.migratedDataSource("stable_rows");
        metadata = new JdbcMetadataStore(source);
        metadata.saveCatalogDescriptor(new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        catalogs = new CatalogRegistry(metadata);
        control = new TableControlService(new JdbcTableControlStore(source), name -> {
            var descriptor = metadata.getCatalogDescriptor(name).orElseThrow();
            return new CatalogDeploymentIdentity(descriptor.incarnationId(), descriptor.revision());
        });
        catalogs.setTableControlService(control);
        persisted = new JdbcRowPolicyStore(source);
        persisted.initSchema();
        policies = new TableControlRowPolicyStore(new RegistryTablePolicyAuthority(catalogs), persisted);
        create("orders", "original");
    }

    @Test
    void renameKeepsTheCanonicalPolicyAndAReusedNameStartsWithAnotherIdentity() {
        policies.savePolicy(policy("orders", "id > 0"));
        var original = control.find("cat", "db", "orders").orElseThrow();
        var bound = policies.getPolicies("cat", "db", "orders").get(0);
        var pending = control.rename(original, "db", "renamed");
        assertThatThrownBy(() -> policies.getPolicies("cat", "db", "orders"))
                .hasMessageContaining("ACTIVE");
        assertThatThrownBy(() -> policies.getPolicies("cat", "db", "renamed"))
                .hasMessageContaining("ACTIVE");
        control.completeRename(pending, Optional.of(identity("original")), Optional.empty());
        assertThat(policies.getPolicies("cat", "db", "renamed")).singleElement().satisfies(value -> {
            assertThat(value.tableId()).isEqualTo(original.tableId());
            assertThat(value.policyId()).isEqualTo(bound.policyId());
            assertThat(value.tableName()).isEqualTo("renamed");
        });
        create("orders", "replacement");
        assertThat(policies.getPolicies("cat", "db", "orders")).isEmpty();
        policies.savePolicy(policy("orders", "id > 10"));
        policies.deletePolicy(bound.policyId());
        assertThat(policies.getPolicies("cat", "db", "orders")).singleElement()
                .satisfies(value -> assertThat(value.filterExpression()).isEqualTo("id > 10"));
    }

    @Test
    void aCapturedPolicyCannotWriteIntoAReplacementAndUnprovenPoliciesRemainBlocked() {
        policies.savePolicy(policy("orders", "id > 0"));
        var captured = policies.getPolicies("cat", "db", "orders").get(0);
        control.markDropped(control.find("cat", "db", "orders").orElseThrow());
        create("orders", "replacement");
        assertThatThrownBy(() -> policies.savePolicy(captured)).hasMessageContaining("replaced");
        assertThat(policies.getPolicies("cat", "db", "orders")).isEmpty();
        persisted.savePolicy(policy("orders", "id > 10"));
        assertThatThrownBy(() -> policies.getPolicies("cat", "db", "orders"))
                .hasMessageContaining("no proven stable table binding");
    }

    @Test
    void catalogReplacementCannotReuseAPolicyEvenIfThePhysicalIdentityIsUnchanged() {
        policies.savePolicy(policy("orders", "id > 0"));
        metadata.deleteCatalogDescriptor("cat");
        metadata.saveCatalogDescriptor(new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        assertThatThrownBy(() -> policies.getPolicies("cat", "db", "orders"))
                .hasMessageContaining("catalog deployment");
    }

    @Test
    void deferredAuthorizationReadsTheSamePolicyByIdentityAfterRename() throws Exception {
        var roles = new org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore(source);
        roles.initSchema();
        roles.createRole(org.pangrova.spi.security.policy.Role.of("reader"));
        roles.grantRoleToUser(org.pangrova.spi.security.policy.UserRoleAssignment.of("alice", "reader"));
        new org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore(source).addGrant(
                org.pangrova.spi.security.policy.PrivilegeGrant.of("reader", org.pangrova.spi.security.policy.Privilege.ALL,
                        "TABLE", "cat", "db", "*", null));
        policies.savePolicy(policy("orders", "id > 0"));
        var pending = control.rename(control.find("cat", "db", "orders").orElseThrow(), "db", "renamed");
        control.completeRename(pending, Optional.of(identity("original")), Optional.empty());
        var table = new org.pangrova.spi.connector.metadata.table.QualifiedTableName("cat", "db", "renamed");
        var raw = org.pangrova.spi.security.authz.DeferredAuthorizationScope.raw(table, Set.of("id"), table);
        var transformed = new org.pangrova.spi.security.authz.DeferredAuthorizationScope(List.of(
                new org.pangrova.spi.security.authz.DeferredAuthorizationScope.TablePermission(table, Set.of("id"),
                        org.pangrova.spi.security.policy.Privilege.SELECT,
                        org.pangrova.spi.security.authz.DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                        new org.pangrova.spi.security.authz.DeferredAuthorizationScope.PolicySnapshot(
                                Optional.of("(id > 0)"), Map.of("id", Optional.empty()))),
                new org.pangrova.spi.security.authz.DeferredAuthorizationScope.TablePermission(table, Set.of(),
                        org.pangrova.spi.security.policy.Privilege.ALTER,
                        org.pangrova.spi.security.authz.DeferredAuthorizationScope.ReadMode.NONE,
                        org.pangrova.spi.security.authz.DeferredAuthorizationScope.PolicySnapshot.empty())), Optional.empty());
        try (var transaction = source.getConnection()) {
            transaction.setAutoCommit(false);
            assertThatThrownBy(() -> TableControlDeferredPolicy.requireCurrent(transaction, "alice", raw))
                    .hasMessageContaining("Raw artifact reads");
            transaction.rollback();
            TableControlDeferredPolicy.requireCurrent(transaction, "alice", transformed);
            transaction.rollback();
        }
    }

    private void create(String table, String physicalId) {
        control.registerCreated("cat", "db", table, "alice", Optional.of(identity(physicalId)), Optional.empty());
    }

    private static TableIdentity identity(String id) {
        return new TableIdentity("TEST", id, "file:/" + id, "schema");
    }

    private static RowPolicy policy(String table, String expression) {
        return new RowPolicy(0, "rows", "cat", "db", table, expression, List.of("reader"), true, 1, null);
    }
}
