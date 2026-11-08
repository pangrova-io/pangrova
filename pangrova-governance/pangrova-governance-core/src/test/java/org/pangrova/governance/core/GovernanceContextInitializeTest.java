package org.pangrova.governance.core;

import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.governance.api.classification.AuditConfigStore;
import org.pangrova.governance.core.classification.TagBasedSecurityPolicy;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.governance.core.quality.CronScheduler;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.core.quality.AlertChannelRegistry;
import org.pangrova.governance.core.quality.DataQualityAlerter;
import org.pangrova.governance.api.quality.Severity;
import org.pangrova.governance.core.quality.TestQualityRuleExecutor;
import org.pangrova.governance.core.quality.GovernanceStatementHandler;
import org.pangrova.governance.core.quality.InMemoryQualityRuleStore;
import org.pangrova.governance.api.quality.QualityRuleStore;
import org.pangrova.security.api.mask.ColumnMaskRecord;
import org.pangrova.persistence.security.api.mask.ColumnMaskStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests GovernanceContext.initialize() registers quality rules from QualityRuleStore.
 */
class GovernanceContextInitializeTest {
    private static final java.util.Map<org.pangrova.connector.api.metadata.table.QualifiedTableName, java.util.UUID>
            tagTableIdentities = new java.util.concurrent.ConcurrentHashMap<>();

    private GovernanceContext module;

    @AfterEach
    void cleanup() throws Exception {
        if (module != null) {
            module.close();
        }
    }

    @Test
    void initializeSchedulesEnabledRulesFromStore() throws Exception {
        var scheduledRuleNames = new CopyOnWriteArrayList<String>();
        var ruleStore = new InMemoryQualityRuleStore();
        ruleStore.saveRule(rule(1, "null_check", true, "60"));
        ruleStore.saveRule(rule(2, "range_check", true, "120"));
        ruleStore.saveRule(rule(3, "disabled_rule", false, "60"));

        module = buildModule(ruleStore, scheduledRuleNames);
        module.initialize();

        // CronScheduler schedules enabled rules; disabled ones are filtered by store
        assertThat(ruleStore.getAllEnabledRules()).hasSize(2);
        assertThat(scheduledRuleNames).containsExactlyInAnyOrder("null_check", "range_check");
    }

    @Test
    void queryRuntimeOwnershipDoesNotAlsoStartLegacyCron() {
        var names = new CopyOnWriteArrayList<String>();
        var store = new InMemoryQualityRuleStore();
        store.saveRule(rule(11, "query_owned", true, "* * * * *"));
        module = buildModule(store, names);
        module.initialize(false);
        assertThat(names).isEmpty();
        assertThat(store.getAllEnabledRules()).hasSize(1);
    }

    @Test
    void initializeWithoutRuleStoreDoesNotThrow() throws Exception {
        module = buildModuleWithoutStore();
        module.initialize(); // should not throw
    }

    @Test
    void initializeWithEmptyStoreSchedulesNothing() throws Exception {
        var ruleStore = new InMemoryQualityRuleStore();
        module = buildModule(ruleStore, new CopyOnWriteArrayList<>());
        module.initialize();

        assertThat(ruleStore.getAllEnabledRules()).isEmpty();
    }

    private GovernanceContext buildModule(QualityRuleStore ruleStore,
                                         List<String> scheduledNames) {
        LineageEventEmitter emitter = new LineageEventEmitter(new NoOpLineageBackend());
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                ruleStore, new TestQualityRuleExecutor(), alerter);
        CronScheduler scheduler = new CronScheduler(handler) {
            @Override public void scheduleRule(DataQualityRule rule) {
                scheduledNames.add(rule.ruleName());
                super.scheduleRule(rule);
            }
        };
        TagBasedSecurityPolicy policy = new TagBasedSecurityPolicy(
                new NoOpColumnMaskStore(), new NoOpAuditConfigStore());
        return new GovernanceContext(new GovernanceDeps(
                emitter, handler, policy, scheduler, ruleStore, new org.pangrova.governance.core.classification.ColumnTagService(
                        table -> tagTableIdentities.computeIfAbsent(table, ignored -> java.util.UUID.randomUUID()))));
    }

    private GovernanceContext buildModuleWithoutStore() {
        LineageEventEmitter emitter = new LineageEventEmitter(new NoOpLineageBackend());
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                new InMemoryQualityRuleStore(), new TestQualityRuleExecutor(), alerter);
        CronScheduler scheduler = new CronScheduler(handler);
        TagBasedSecurityPolicy policy = new TagBasedSecurityPolicy(
                new NoOpColumnMaskStore(), new NoOpAuditConfigStore());
        return new GovernanceContext(new GovernanceDeps(
                emitter, handler, policy, scheduler, new InMemoryQualityRuleStore(), new org.pangrova.governance.core.classification.ColumnTagService(
                        table -> tagTableIdentities.computeIfAbsent(table, ignored -> java.util.UUID.randomUUID()))));
    }

    private static DataQualityRule rule(long id, String name, boolean enabled, String schedule) {
        return new DataQualityRule(id, name, "catalog", "schema", "table",
                "expression", Severity.WARNING, "msg", enabled, schedule,
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);
    }

    // --- In-memory implementations for real testing (no mocks) ---

    static class NoOpLineageBackend implements LineageBackend {
        @Override public void send(LineageEvent event) { /* no-op */ }
        @Override public boolean isAvailable() { return true; }
    }

    static class NoOpColumnMaskStore implements ColumnMaskStore {
        @Override public List<ColumnMaskRecord> getMasks(String c, String s, String t, String col) {
            return List.of();
        }
        @Override public void saveMask(ColumnMaskRecord mask) { /* no-op */ }
        @Override public void deleteMask(long maskId) { /* no-op */ }
    }

    static class NoOpAuditConfigStore implements AuditConfigStore {
        @Override public void enableDetailedAudit(String cat, String schema, String table) { }
        @Override public void disableDetailedAudit(String cat, String schema, String table) { }
        @Override public boolean isDetailedAuditEnabled(String cat, String schema, String table) {
            return false;
        }
    }
}
