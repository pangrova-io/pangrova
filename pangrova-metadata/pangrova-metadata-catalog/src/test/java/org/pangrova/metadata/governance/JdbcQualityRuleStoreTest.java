package org.pangrova.metadata.governance;

import org.pangrova.persistence.governance.postgresql.JdbcQualityRuleStore;
import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.enums.Severity;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcQualityRuleStoreTest {

    private JdbcQualityRuleStore store;

    @BeforeEach
    void setUp() throws Exception {
        store = new JdbcQualityRuleStore(
                SharedPostgresContainer.migratedDataSource("quality_rules"));
    }

    @Test
    void saveAndGetRuleRoundTrip() {
        DataQualityRule rule = createRule(1L, "null_check", "catalog1", "schema1",
                "orders", "col IS NOT NULL", Severity.ERROR, true);
        store.saveRule(rule);

        Optional<DataQualityRule> loaded = store.getRule(1L);
        assertThat(loaded).isPresent();
        assertThat(loaded.get().ruleId()).isEqualTo(1L);
        assertThat(loaded.get().ruleName()).isEqualTo("null_check");
        assertThat(loaded.get().catalogName()).isEqualTo("catalog1");
        assertThat(loaded.get().schemaName()).isEqualTo("schema1");
        assertThat(loaded.get().tableName()).isEqualTo("orders");
        assertThat(loaded.get().checkExpression()).isEqualTo("col IS NOT NULL");
        assertThat(loaded.get().severity()).isEqualTo(Severity.ERROR);
        assertThat(loaded.get().enabled()).isTrue();
    }

    @Test
    void getRuleReturnsEmptyForMissing() {
        assertThat(store.getRule(999L)).isEmpty();
    }

    @Test
    void saveRuleUpsertOverwritesExisting() {
        DataQualityRule original = createRule(2L, "range_check", "cat", "sch",
                "items", "price > 0", Severity.WARNING, true);
        store.saveRule(original);

        DataQualityRule updated = new DataQualityRule(2L, "range_check_v2", "cat", "sch",
                "items", "price > 0 AND price < 10000", Severity.ERROR, "updated msg",
                false, "120", Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);
        store.saveRule(updated);

        DataQualityRule loaded = store.getRule(2L).orElseThrow();
        assertThat(loaded.ruleName()).isEqualTo("range_check_v2");
        assertThat(loaded.checkExpression()).isEqualTo("price > 0 AND price < 10000");
        assertThat(loaded.severity()).isEqualTo(Severity.ERROR);
        assertThat(loaded.enabled()).isFalse();
    }

    @Test
    void getRulesForTableFiltersCorrectly() {
        store.saveRule(createRule(10L, "r1", "cat", "sch", "orders", "x", Severity.ERROR, true));
        store.saveRule(createRule(11L, "r2", "cat", "sch", "orders", "y", Severity.WARNING, true));
        store.saveRule(createRule(12L, "r3", "cat", "sch", "users", "z", Severity.INFO, true));
        store.saveRule(createRule(13L, "r4", "other_cat", "sch", "orders", "w", Severity.ERROR, true));

        List<DataQualityRule> rules = store.getRulesForTable("cat", "sch", "orders");
        assertThat(rules).hasSize(2);
        assertThat(rules).extracting(DataQualityRule::ruleId)
                .containsExactlyInAnyOrder(10L, 11L);
    }

    @Test
    void getAllEnabledRulesFiltersDisabled() {
        store.saveRule(createRule(20L, "enabled1", "c", "s", "t", "x", Severity.ERROR, true));
        store.saveRule(createRule(21L, "enabled2", "c", "s", "t", "y", Severity.WARNING, true));
        store.saveRule(createRule(22L, "disabled1", "c", "s", "t", "z", Severity.INFO, false));

        List<DataQualityRule> enabled = store.getAllEnabledRules();
        assertThat(enabled).hasSize(2);
        assertThat(enabled).extracting(DataQualityRule::ruleName)
                .containsExactlyInAnyOrder("enabled1", "enabled2");
    }

    @Test
    void deleteRuleRemovesExisting() {
        store.saveRule(createRule(30L, "to_delete", "c", "s", "t", "x", Severity.ERROR, true));
        assertThat(store.getRule(30L)).isPresent();

        boolean deleted = store.deleteRule(30L);
        assertThat(deleted).isTrue();
        assertThat(store.getRule(30L)).isEmpty();
    }

    @Test
    void deleteRuleReturnsFalseForMissing() {
        assertThat(store.deleteRule(999L)).isFalse();
    }

    @Test
    void saveAndRetrieveCheckResult() {
        Instant now = Instant.now();
        QualityCheckResult result = new QualityCheckResult(
                100L, 1L, "null_check", "orders",
                Severity.ERROR, now, false, 1000L, 50L, 250L, "50 nulls found", null);
        store.saveResult(result);
    }

    @Test
    void ruleWithAllFieldsPersistsCorrectly() {
        Instant created = Instant.parse("2024-01-15T10:30:00Z");
        Instant updated = Instant.parse("2024-06-20T14:00:00Z");
        DataQualityRule rule = new DataQualityRule(
                50L, "completeness_check", "production", "analytics", "fact_sales",
                "COUNT(*) WHERE amount IS NULL = 0", Severity.ERROR,
                "Sales amount must not be null", true, "*/5 * * * *",
                created, updated,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);
        store.saveRule(rule);

        DataQualityRule loaded = store.getRule(50L).orElseThrow();
        assertThat(loaded.ruleId()).isEqualTo(50L);
        assertThat(loaded.ruleName()).isEqualTo("completeness_check");
        assertThat(loaded.catalogName()).isEqualTo("production");
        assertThat(loaded.schemaName()).isEqualTo("analytics");
        assertThat(loaded.tableName()).isEqualTo("fact_sales");
        assertThat(loaded.checkExpression()).isEqualTo("COUNT(*) WHERE amount IS NULL = 0");
        assertThat(loaded.severity()).isEqualTo(Severity.ERROR);
        assertThat(loaded.message()).isEqualTo("Sales amount must not be null");
        assertThat(loaded.enabled()).isTrue();
        assertThat(loaded.schedule()).isEqualTo("*/5 * * * *");
        assertThat(loaded.createdAt()).isNotNull();
        assertThat(loaded.updatedAt()).isNotNull();
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private static DataQualityRule createRule(long id, String name, String catalog,
                                              String schema, String table, String expression,
                                              Severity severity, boolean enabled) {
        return new DataQualityRule(id, name, catalog, schema, table, expression,
                severity, "test message", enabled, "60",
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);
    }
}
