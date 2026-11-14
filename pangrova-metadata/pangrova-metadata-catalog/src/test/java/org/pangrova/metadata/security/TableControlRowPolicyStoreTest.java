package org.pangrova.metadata.security;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.metadata.testinfra.TestCatalogRegistry;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.table.ownership.TableControlService;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.connector.api.metadata.table.TableIdentity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlRowPolicyStoreTest {
    private javax.sql.DataSource source;
    private TestCatalogRegistry catalogs;
    private PostgresCatalogDeploymentRepository metadata;
    private TableControlService control;
    private JdbcRowPolicyStore persisted;
    private TableControlRowPolicyStore policies;

    @BeforeEach
    void setUp() throws Exception {
        source = SharedPostgresContainer.migratedDataSource("stable_rows");
        metadata = new PostgresCatalogDeploymentRepository(source);
        metadata.saveCatalogDescriptor(new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        catalogs = new TestCatalogRegistry(memoryTableStores(), metadata);
        control = new TableControlService(new JdbcTableControlStore(source), name -> {
            var descriptor = metadata.getCatalogDescriptor(name).orElseThrow();
            return new CatalogDeploymentIdentity(descriptor.incarnationId(), descriptor.revision());
        });
        catalogs.setTableControlService(control);
        persisted = new JdbcRowPolicyStore(source);
        policies = new TableControlRowPolicyStore(catalogs.tableGovernance(), persisted);
        create("orders", "original");
    }

    @Test
    void renameKeepsTheCanonicalPolicyAndAReusedNameStartsWithAnotherIdentity() {
        policies.savePolicy(policy("orders", "id > 0"));
        var original = control.find("cat", "db", "orders").orElseThrow();
        var bound = policies.getPolicies("cat", "db", "orders").get(0);
        var pending = control.renames().begin(original, "db", "renamed");
        assertThatThrownBy(() -> policies.getPolicies("cat", "db", "orders"))
                .hasMessageContaining("ACTIVE");
        assertThatThrownBy(() -> policies.getPolicies("cat", "db", "renamed"))
                .hasMessageContaining("ACTIVE");
        control.renames().complete(pending, Optional.of(identity("original")), Optional.empty());
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
        control.registrations().markDropped(control.find("cat", "db", "orders").orElseThrow());
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
        metadata.deleteCatalogDescriptor(metadata.getCatalogDescriptor("cat").orElseThrow().identity());
        metadata.saveCatalogDescriptor(new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        assertThatThrownBy(() -> policies.getPolicies("cat", "db", "orders"))
                .hasMessageContaining("catalog deployment");
    }

    @Test
    void deferredAuthorizationReadsTheSamePolicyByIdentityAfterRename() throws Exception {
        var roles = new org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore(source);
        roles.createRole(org.pangrova.security.api.policy.Role.of("reader"));
        roles.grantRoleToUser(org.pangrova.security.api.policy.UserRoleAssignment.of("alice", "reader"));
        new org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore(source).addGrant(
                org.pangrova.security.api.policy.PrivilegeGrant.of("reader", org.pangrova.security.api.enums.Privilege.ALL,
                        "TABLE", "cat", "db", "*", null));
        policies.savePolicy(policy("orders", "id > 0"));
        var pending = control.renames().begin(control.find("cat", "db", "orders").orElseThrow(), "db", "renamed");
        control.renames().complete(pending, Optional.of(identity("original")), Optional.empty());
        var table = new org.pangrova.connector.api.metadata.table.QualifiedTableName("cat", "db", "renamed");
        var raw = org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(table, Set.of("id"), table);
        var transformed = new org.pangrova.security.api.authorization.DeferredAuthorizationScope(List.of(
                new org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission(table, Set.of("id"),
                        org.pangrova.security.api.enums.Privilege.SELECT,
                        org.pangrova.security.api.authorization.DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                        new org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot(
                                Optional.of("(id > 0)"), Map.of("id", Optional.empty()))),
                new org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission(table, Set.of(),
                        org.pangrova.security.api.enums.Privilege.ALTER,
                        org.pangrova.security.api.authorization.DeferredAuthorizationScope.ReadMode.NONE,
                        org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot.empty())), Optional.empty());
        try (var transaction = source.getConnection()) {
            transaction.setAutoCommit(false);
            assertThatThrownBy(() -> TableWriteAuthorityFixture.create().requireCurrent(transaction, "alice", raw))
                    .hasMessageContaining("Raw artifact reads");
            transaction.rollback();
            TableWriteAuthorityFixture.create().requireCurrent(transaction, "alice", transformed);
            transaction.rollback();
        }
    }

    private void create(String table, String physicalId) {
        control.registrations().registerCreated("cat", "db", table, "alice", Optional.of(identity(physicalId)), Optional.empty());
    }

    private static TableIdentity identity(String id) {
        return new TableIdentity("TEST", id, "file:/" + id, "schema");
    }

    private static RowPolicy policy(String table, String expression) {
        return new RowPolicy(0, "rows", "cat", "db", table, expression, List.of("reader"), true, 1, null);
    }
}
