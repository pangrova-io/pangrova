package org.pangrova.metadata.service;

import org.pangrova.governance.runtime.classification.ColumnTagBindings;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.metadata.testinfra.TestCatalogRegistry;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.metadata.security.TableControlColumnMaskStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.persistence.table.postgresql.governancebinding.JdbcTableControlGovernanceBindingStore;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.table.ownership.TableControlService;
import org.pangrova.table.config.TableControlSettings;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.connector.api.metadata.table.TableIdentity;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataTagOperationsTest {
    private TestCatalogRegistry catalogs;
    private javax.sql.DataSource source;
    private TableControlService control;
    private ColumnTagBindings tags;
    private final QualifiedTableRef orders = new QualifiedTableRef("cat", "db", "orders");

    @BeforeEach
    void setUp() throws Exception {
        source = SharedPostgresContainer.migratedDataSource("stable_tags");
        var metadata = new PostgresCatalogDeploymentRepository(source);
        var descriptor = new org.pangrova.metadata.api.catalog.CatalogDescriptor("cat",
                org.pangrova.metadata.api.enums.CatalogType.CONNECTOR, "test",
                org.pangrova.metadata.api.enums.CatalogState.ACTIVE, Map.of(), "", Set.of());
        metadata.saveCatalogDescriptor(descriptor);
        catalogs = new TestCatalogRegistry(memoryTableStores(), metadata);
        control = new TableControlService(new JdbcTableControlStore(source),
                name -> new CatalogDeploymentIdentity(descriptor.incarnationId(), descriptor.revision()),
                new TableControlSettings(true, true, Set.of()));
        catalogs.setTableControlService(control);
        catalogs.setGovernanceBindingStore(new JdbcTableControlGovernanceBindingStore(source));
        tags = new ColumnTagBindings(catalogs.tableGovernance());
        create("orders");
        create("customers");
    }

    @Test
    void keepsTagsIsolatedByStableTableAndColumn() {
        tags.setTag(current(orders), "id", "classification", "sensitive", "alice");
        assertThat(tags.listTags(current(orders), "id")).containsEntry("classification", "sensitive");
        assertThat(tags.listTags(current(orders), "other")).isEmpty();
        assertThat(tags.listTags(current(new QualifiedTableRef("cat", "db", "customers")), "id")).isEmpty();
        tags.unsetTag(current(orders), "id", "classification");
        assertThat(tags.listTags(current(orders), "id")).isEmpty();
    }

    @Test
    void reconstructedServiceReadsTheSameDurableAuthority() {
        tags.setTag(current(orders), "id", "classification", "sensitive", "alice");
        var reopened = new ColumnTagBindings(catalogs.tableGovernance());
        assertThat(reopened.listTags(current(orders), "id")).containsEntry("classification", "sensitive");
    }

    @Test
    void replacementDoesNotInheritTagsAndOldCleanupCannotDeleteNewTags() {
        var original = control.find("cat", "db", "orders").orElseThrow();
        tags.setTag(current(orders), "id", "classification", "old", "alice");
        control.registrations().markDropped(original);
        create("orders");
        assertThat(tags.listTags(current(orders), "id")).isEmpty();
        tags.setTag(current(orders), "id", "classification", "new", "alice");
        assertThat(tags.clearTable(original.tableId())).isEqualTo(1);
        assertThat(tags.listTags(current(orders), "id")).containsEntry("classification", "new");
    }

    @Test
    void renameRetainsTagsWithoutMovingTheDurableKey() {
        tags.setTag(current(orders), "id", "classification", "sensitive", "alice");
        var original = control.find("cat", "db", "orders").orElseThrow();
        var pending = control.renames().begin(original, "db", "renamed");
        var renamed = new QualifiedTableRef("cat", "db", "renamed");
        assertThat(tags.renameTable(original.tableRef(), pending.tableRef())).isEqualTo(1);
        control.renames().complete(pending, Optional.of(identity("orders")), Optional.empty());
        assertThat(tags.listTags(current(renamed), "id")).containsEntry("classification", "sensitive");
        create("orders");
        assertThat(tags.listTags(current(orders), "id")).isEmpty();
    }

    @Test
    void capturedTagOwnerCannotWriteRemoveOrReadAReplacement() {
        var admitted = current(orders);
        tags.setTag(admitted, "id", "classification", "old", "alice");
        control.registrations().markDropped(admitted);
        create("orders");
        tags.setTag(current(orders), "id", "classification", "new", "alice");
        assertThatThrownBy(() -> tags.setTag(admitted, "id", "classification", "stale", "alice"))
                .hasMessageContaining("ownership");
        assertThatThrownBy(() -> tags.unsetTag(admitted, "id", "classification"))
                .hasMessageContaining("ownership");
        assertThatThrownBy(() -> tags.listTags(admitted, "id"))
                .hasMessageContaining("ownership");
        assertThat(tags.listTags(current(orders), "id")).containsEntry("classification", "new");
    }

    @Test
    void lengthFramedReferencesDoNotCollideOnColons() {
        tags.setTag(current(orders), "a:b", "c", "first", "alice");
        tags.setTag(current(orders), "a", "b:c", "second", "alice");
        assertThat(tags.listTags(current(orders), "a:b")).containsExactly(Map.entry("c", "first"));
        assertThat(tags.listTags(current(orders), "a")).containsExactly(Map.entry("b:c", "second"));
    }

    @Test
    void rejectsObservedTablesAndMissingFields() {
        assertThatThrownBy(() -> tags.setTag(current(new QualifiedTableRef("cat", "db", "observed")),
                "id", "classification", "sensitive", "alice")).hasMessageContaining("OBSERVED");
        assertThatThrownBy(() -> tags.setTag(null, "id", "classification", "sensitive", "alice"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tags.setTag(current(orders), null, "classification", "sensitive", "alice"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tags.setTag(current(orders), "id", null, "sensitive", "alice"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tags.setTag(current(orders), "id", "classification", null, "alice"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void independentMaskReadersSeeCanonicalTagsWithoutEventDelivery() {
        var persisted = new org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore(source);
        tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice");
        var first = new TableControlColumnMaskStore(catalogs.tableGovernance(), persisted);
        var peer = new TableControlColumnMaskStore(catalogs.tableGovernance(),
                new org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore(source));
        assertThat(first.getColumnMasks("cat", "db", "orders", "email")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("CASE WHEN {column} IS NULL THEN NULL ELSE '***@***' END"));
        assertThat(peer.getColumnMasks("cat", "db", "orders", "email"))
                .isEqualTo(first.getColumnMasks("cat", "db", "orders", "email"));
        assertThat(persisted.getTableMasks("cat", "db", "orders")).isEmpty();
        tags.unsetTag(current(orders), "email", "pii_type");
        assertThat(peer.getTableMasks("cat", "db", "orders")).isEmpty();
    }

    @Test
    void notificationFailureCannotLeaveCommittedClassificationUnmasked() {
        tags.events().addListener(event -> { throw new IllegalStateException("notification failed"); });
        assertThatThrownBy(() -> tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice"))
                .hasMessage("notification failed");
        var persisted = new org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore(source);
        assertThat(new TableControlColumnMaskStore(catalogs.tableGovernance(), persisted)
                .getColumnMasks("cat", "db", "orders", "email")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("CASE WHEN {column} IS NULL THEN NULL ELSE '***@***' END"));
    }

    @Test
    void authoredMasksBindAtWriteAndNeverFollowAReusedName() {
        var persisted = new org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore(source);
        var masks = new TableControlColumnMaskStore(catalogs.tableGovernance(), persisted);
        masks.saveColumnMask(new org.pangrova.security.api.mask.ColumnMask(0, "manual", "cat", "db", "orders",
                "email", "'private'", java.util.List.of(), java.util.List.of(), true, null));
        var original = control.find("cat", "db", "orders").orElseThrow();
        var captured = masks.getColumnMasks("cat", "db", "orders", "email").get(0);
        assertThat(captured.tableId()).isEqualTo(original.tableId());
        control.registrations().markDropped(original);
        create("orders");
        assertThat(masks.getTableMasks("cat", "db", "orders")).isEmpty();
        assertThatThrownBy(() -> masks.saveColumnMask(captured)).hasMessageContaining("replaced");
        masks.saveColumnMask(new org.pangrova.security.api.mask.ColumnMask(0, "manual", "cat", "db", "orders",
                "email", "'new'", java.util.List.of(), java.util.List.of(), true, null));
        for (var stale : persisted.getMasks(original.tableId(), "cat", "db", "orders", "email")) {
            masks.deleteMask(stale.maskId());
        }
        assertThat(masks.getColumnMasks("cat", "db", "orders", "email")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("'new'"));
    }

    @Test
    void mandatoryClassificationCannotBeBypassedByAnAuthoredMaskExemption() {
        var persisted = new org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore(source);
        var masks = new TableControlColumnMaskStore(catalogs.tableGovernance(), persisted);
        masks.saveColumnMask(new org.pangrova.security.api.mask.ColumnMask(0, "authored", "cat", "db", "orders",
                "email", "{column}", java.util.List.of(), java.util.List.of("reader"), true, null));
        tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice");
        var user = new org.pangrova.security.api.identity.UserIdentity("alice", Set.of(), Map.of(), java.time.Instant.now());
        assertThat(new org.pangrova.security.mask.ColumnMaskingProcessor(masks)
                .getMask(user, Set.of("reader"), "cat", "db", "orders", "email"))
                .hasValueSatisfying(expression -> assertThat(expression).contains("***@***"));
    }

    private org.pangrova.table.api.ownership.TableControlRecord current(QualifiedTableRef table) {
        return catalogs.tableGovernance().requireBindable(table.catalog(), table.schema(), table.table());
    }

    private void create(String table) {
        control.registrations().registerCreated("cat", "db", table, "alice", Optional.of(identity(table)), Optional.empty());
    }

    @Test
    void deferredRawReadsCannotBypassCanonicalTagsWithoutAnEventProjection() throws Exception {
        var roles = new org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore(source);
        roles.createRole(org.pangrova.security.api.policy.Role.of("reader"));
        roles.grantRoleToUser(org.pangrova.security.api.policy.UserRoleAssignment.of("alice", "reader"));
        var grants = new org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore(source);
        grants.addGrant(org.pangrova.security.api.policy.PrivilegeGrant.of("reader",
                org.pangrova.security.api.enums.Privilege.ALL, "TABLE", "cat", "db", "orders", null));
        var table = new org.pangrova.connector.api.metadata.table.QualifiedTableName("cat", "db", "orders");
        var scope = org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(table, Set.of("email"), table);
        try (var transaction = source.getConnection()) {
            transaction.setAutoCommit(false);
            TableWriteAuthorityFixture.create().requireCurrent(transaction, "alice", scope);
            transaction.rollback();
            tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice");
            assertThatThrownBy(() -> TableWriteAuthorityFixture.create().requireCurrent(transaction, "alice", scope))
                    .hasMessageContaining("Raw artifact reads cannot bypass");
            transaction.rollback();
            tags.unsetTag(current(orders), "email", "pii_type");
            TableWriteAuthorityFixture.create().requireCurrent(transaction, "alice", scope);
            transaction.rollback();
        }
    }

    private static TableIdentity identity(String table) {
        return new TableIdentity("TEST", "uuid-" + table, "file:/" + table, "schema");
    }
}
