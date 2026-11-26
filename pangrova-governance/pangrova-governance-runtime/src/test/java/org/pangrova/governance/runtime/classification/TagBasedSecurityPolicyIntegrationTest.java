package org.pangrova.governance.runtime.classification;

import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.api.rowfilter.RowPolicyStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.governance.testinfra.GovernanceRoleFixtures.mappedRoles;
import org.pangrova.governance.api.classification.AuditConfigStore;
import org.pangrova.governance.api.classification.TagChangedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies TagBasedSecurityPolicy writes masks via SPI ColumnMaskStore,
 * and DynamicRbacAccessControl reads them via ColumnMaskingProcessor — shared store wiring.
 */
class TagBasedSecurityPolicyIntegrationTest {

    @Test
    void tagChangeCreatesColumnMaskVisibleToAccessControl() {
        var sharedStore = new InMemoryColumnMaskStore();
        var policy = new TagBasedSecurityPolicy(sharedStore, new NoOpAuditConfigStore());
        var maskProcessor = new ColumnMaskingProcessor(sharedStore);
        var accessControl = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"))),
                new RowLevelSecurityFilter(emptyRowPolicyStore()),
                maskProcessor);

        // Simulate PII tag applied to a column
        var event = new TagChangedEvent(
                "catalog", "schema", "users", "email",
                "pii_type", null, "PII_EMAIL", null);
        policy.onTagChanged(event);

        // Verify mask is visible through AccessControl
        UserIdentity user = new UserIdentity("alice", Set.of("analyst"), Map.of(), Instant.now());
        Optional<String> mask = accessControl.getColumnMasks(
                user, "catalog", "schema", "users").mask("email");
        assertThat(mask).isPresent();
        assertThat(mask.get()).contains("***@***");
    }

    @Test
    void tagRemovalDeletesMaskFromAccessControl() {
        var sharedStore = new InMemoryColumnMaskStore();
        var policy = new TagBasedSecurityPolicy(sharedStore, new NoOpAuditConfigStore());
        var maskProcessor = new ColumnMaskingProcessor(sharedStore);
        var accessControl = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"))),
                new RowLevelSecurityFilter(emptyRowPolicyStore()),
                maskProcessor);

        // Apply tag
        policy.onTagChanged(new TagChangedEvent(
                "catalog", "schema", "users", "ssn",
                "pii_type", null, "PII_SSN", null));

        // Verify mask exists
        UserIdentity user = new UserIdentity("bob", Set.of("analyst"), Map.of(), Instant.now());
        assertThat(accessControl.getColumnMasks(user, "catalog", "schema", "users").mask("ssn"))
                .isPresent();

        // Remove tag
        policy.onTagChanged(new TagChangedEvent(
                "catalog", "schema", "users", "ssn",
                "pii_type", "PII_SSN", null, null));

        // Verify mask is gone
        assertThat(accessControl.getColumnMasks(user, "catalog", "schema", "users").mask("ssn"))
                .isEmpty();
    }

    private static RowPolicyStore emptyRowPolicyStore() {
        return new RowPolicyStore() {
            @Override public List<RowPolicy> getPolicies(String c, String s, String t) {
                return List.of();
            }
            @Override public void savePolicy(RowPolicy p) { }
            @Override public void deletePolicy(long id) { }
        };
    }

    static class InMemoryColumnMaskStore implements EnrichedColumnMaskStore {
        private final List<ColumnMask> masks = new ArrayList<>();
        private long nextId = 1;

        @Override
        public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
            return masks.stream().filter(mask -> mask.catalogName().equals(catalog)
                    && mask.schemaName().equals(schema) && mask.tableName().equals(table)).toList();
        }

        @Override
        public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
            return masks.stream()
                    .filter(m -> m.catalogName().equals(catalog)
                            && m.schemaName().equals(schema)
                            && m.tableName().equals(table)
                            && m.columnName().equals(column))
                    .toList();
        }

        @Override
        public void saveColumnMask(ColumnMask mask) {
            masks.removeIf(m -> m.maskName().equals(mask.maskName())
                    && m.tableName().equals(mask.tableName())
                    && m.columnName().equals(mask.columnName()));
            masks.add(new ColumnMask(nextId++, mask.maskName(), mask.catalogName(),
                    mask.schemaName(), mask.tableName(), mask.columnName(),
                    mask.maskExpression(), mask.applyToRoles(), mask.exemptRoles(),
                    mask.enabled(), mask.tableId()));
        }

        @Override
        public void deleteMask(long maskId) {
            masks.removeIf(m -> m.maskId() == maskId);
        }
    }

    static class NoOpAuditConfigStore implements AuditConfigStore {
        @Override public void enableDetailedAudit(String cat, String schema, String table) { }
        @Override public void disableDetailedAudit(String cat, String schema, String table) { }
        @Override public boolean isDetailedAuditEnabled(String cat, String schema, String table) {
            return false;
        }
    }
}
