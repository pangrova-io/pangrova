package org.pangrova.metadata.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.control.CatalogDeploymentIdentity;
import org.pangrova.metadata.control.JdbcTableControlGovernanceBindingStore;
import org.pangrova.metadata.control.JdbcTableControlStore;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.control.TableControlSettings;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.connector.metadata.table.TableIdentity;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataTagOperationsTest {
    private CatalogRegistry catalogs;
    private javax.sql.DataSource source;
    private TableControlService control;
    private MetadataTagOperations tags;
    private final QualifiedTableRef orders = new QualifiedTableRef("cat", "db", "orders");

    @BeforeEach
    void setUp() throws Exception {
        source = SharedPostgresContainer.migratedDataSource("stable_tags");
        var metadata = new JdbcMetadataStore(source);
        var descriptor = new org.pangrova.metadata.catalog.CatalogDescriptor("cat",
                org.pangrova.metadata.catalog.CatalogType.CONNECTOR, "test",
                org.pangrova.metadata.catalog.CatalogState.ACTIVE, Map.of(), "", Set.of());
        metadata.saveCatalogDescriptor(descriptor);
        catalogs = new CatalogRegistry(metadata);
        control = new TableControlService(new JdbcTableControlStore(source),
                name -> new CatalogDeploymentIdentity(descriptor.incarnationId(), descriptor.revision()),
                new TableControlSettings(true, true, Set.of()));
        catalogs.setTableControlService(control);
        catalogs.setGovernanceBindingStore(new JdbcTableControlGovernanceBindingStore(source));
        tags = new MetadataTagOperations(catalogs);
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
        var reopened = new MetadataTagOperations(catalogs);
        assertThat(reopened.listTags(current(orders), "id")).containsEntry("classification", "sensitive");
    }

    @Test
    void replacementDoesNotInheritTagsAndOldCleanupCannotDeleteNewTags() {
        var original = control.find("cat", "db", "orders").orElseThrow();
        tags.setTag(current(orders), "id", "classification", "old", "alice");
        control.markDropped(original);
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
        var pending = control.rename(original, "db", "renamed");
        var renamed = new QualifiedTableRef("cat", "db", "renamed");
        assertThat(tags.renameTable(orders, renamed)).isEqualTo(1);
        control.completeRename(pending, Optional.of(identity("orders")), Optional.empty());
        assertThat(tags.listTags(current(renamed), "id")).containsEntry("classification", "sensitive");
        create("orders");
        assertThat(tags.listTags(current(orders), "id")).isEmpty();
    }

    @Test
    void capturedTagOwnerCannotWriteRemoveOrReadAReplacement() {
        var admitted = current(orders);
        tags.setTag(admitted, "id", "classification", "old", "alice");
        control.markDropped(admitted);
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
        var persisted = new org.pangrova.security.store.PersistentColumnMaskStore(source);
        persisted.initSchema();
        tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice");
        var first = new TableControlColumnMaskStore(catalogs, persisted);
        var peer = new TableControlColumnMaskStore(catalogs,
                new org.pangrova.security.store.PersistentColumnMaskStore(source));
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
        var events = new org.pangrova.governance.classification.ColumnTagService(table ->
                catalogs.tableGovernance().requireBindable(table.catalog(), table.schema(), table.table()).tableId());
        events.addListener(event -> { throw new IllegalStateException("notification failed"); });
        tags.setColumnTagService(events);
        assertThatThrownBy(() -> tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice"))
                .hasMessage("notification failed");
        var persisted = new org.pangrova.security.store.PersistentColumnMaskStore(source);
        persisted.initSchema();
        assertThat(new TableControlColumnMaskStore(catalogs, persisted)
                .getColumnMasks("cat", "db", "orders", "email")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("CASE WHEN {column} IS NULL THEN NULL ELSE '***@***' END"));
    }

    @Test
    void authoredMasksBindAtWriteAndNeverFollowAReusedName() {
        var persisted = new org.pangrova.security.store.PersistentColumnMaskStore(source);
        persisted.initSchema();
        var masks = new TableControlColumnMaskStore(catalogs, persisted);
        masks.saveColumnMask(new org.pangrova.security.mask.ColumnMask(0, "manual", "cat", "db", "orders",
                "email", "'private'", java.util.List.of(), java.util.List.of(), true, null));
        var original = control.find("cat", "db", "orders").orElseThrow();
        var captured = masks.getColumnMasks("cat", "db", "orders", "email").get(0);
        assertThat(captured.tableId()).isEqualTo(original.tableId());
        control.markDropped(original);
        create("orders");
        assertThat(masks.getTableMasks("cat", "db", "orders")).isEmpty();
        assertThatThrownBy(() -> masks.saveColumnMask(captured)).hasMessageContaining("replaced");
        masks.saveColumnMask(new org.pangrova.security.mask.ColumnMask(0, "manual", "cat", "db", "orders",
                "email", "'new'", java.util.List.of(), java.util.List.of(), true, null));
        for (var stale : persisted.getMasks(original.tableId(), "cat", "db", "orders", "email")) {
            masks.deleteMask(stale.maskId());
        }
        assertThat(masks.getColumnMasks("cat", "db", "orders", "email")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("'new'"));
    }

    @Test
    void mandatoryClassificationCannotBeBypassedByAnAuthoredMaskExemption() {
        var persisted = new org.pangrova.security.store.PersistentColumnMaskStore(source);
        persisted.initSchema();
        var masks = new TableControlColumnMaskStore(catalogs, persisted);
        masks.saveColumnMask(new org.pangrova.security.mask.ColumnMask(0, "authored", "cat", "db", "orders",
                "email", "{column}", java.util.List.of(), java.util.List.of("reader"), true, null));
        tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice");
        var user = new org.pangrova.spi.security.UserIdentity("alice", Set.of(), Map.of(), java.time.Instant.now());
        assertThat(new org.pangrova.security.mask.ColumnMaskingProcessor(masks)
                .getMask(user, Set.of("reader"), "cat", "db", "orders", "email"))
                .hasValueSatisfying(expression -> assertThat(expression).contains("***@***"));
    }

    private org.pangrova.metadata.control.TableControlRecord current(QualifiedTableRef table) {
        return catalogs.tableGovernance().requireBindable(table.catalog(), table.schema(), table.table());
    }

    private void create(String table) {
        control.registerCreated("cat", "db", table, "alice", Optional.of(identity(table)), Optional.empty());
    }

    @Test
    void deferredRawReadsCannotBypassCanonicalTagsWithoutAnEventProjection() throws Exception {
        var roles = new org.pangrova.security.store.PersistentRoleStore(source);
        roles.initSchema();
        roles.createRole(org.pangrova.security.authz.Role.of("reader"));
        roles.grantRoleToUser(org.pangrova.security.authz.UserRoleAssignment.of("alice", "reader"));
        var grants = new org.pangrova.security.store.PersistentGrantStore(source);
        grants.addGrant(org.pangrova.security.authz.PrivilegeGrant.of("reader",
                org.pangrova.security.authz.Privilege.ALL, "TABLE", "cat", "db", "orders", null));
        var table = new org.pangrova.spi.connector.metadata.table.QualifiedTableName("cat", "db", "orders");
        var scope = org.pangrova.security.authz.DeferredAuthorizationScope.raw(table, Set.of("email"), table);
        try (var transaction = source.getConnection()) {
            transaction.setAutoCommit(false);
            TableControlDeferredPolicy.requireCurrent(transaction, "alice", scope);
            transaction.rollback();
            tags.setTag(current(orders), "email", "pii_type", "PII_EMAIL", "alice");
            assertThatThrownBy(() -> TableControlDeferredPolicy.requireCurrent(transaction, "alice", scope))
                    .hasMessageContaining("Raw artifact reads cannot bypass");
            transaction.rollback();
            tags.unsetTag(current(orders), "email", "pii_type");
            TableControlDeferredPolicy.requireCurrent(transaction, "alice", scope);
            transaction.rollback();
        }
    }

    private static TableIdentity identity(String table) {
        return new TableIdentity("TEST", "uuid-" + table, "file:/" + table, "schema");
    }
}
