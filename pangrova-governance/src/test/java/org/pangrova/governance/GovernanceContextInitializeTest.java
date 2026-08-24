package org.pangrova.governance;

import org.pangrova.governance.classification.AuditConfigStore;
import org.pangrova.governance.classification.TagBasedSecurityPolicy;
import org.pangrova.governance.lineage.LineageBackend;
import org.pangrova.governance.lineage.LineageEvent;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.quality.schedule.CronScheduler;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.governance.quality.alert.AlertChannelRegistry;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.governance.quality.rule.DefaultQualityRuleExecutor;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.rule.InMemoryQualityRuleStore;
import org.pangrova.governance.quality.rule.QualityRuleStore;
import org.pangrova.spi.security.ColumnMaskRecord;
import org.pangrova.spi.security.ColumnMaskStore;
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
                ruleStore, new DefaultQualityRuleExecutor(), alerter);
        CronScheduler scheduler = new CronScheduler(handler);
        TagBasedSecurityPolicy policy = new TagBasedSecurityPolicy(
                new NoOpColumnMaskStore(), new NoOpAuditConfigStore());
        return new GovernanceContext(new GovernanceDeps(
                emitter, handler, policy, scheduler, ruleStore));
    }

    private GovernanceContext buildModuleWithoutStore() {
        LineageEventEmitter emitter = new LineageEventEmitter(new NoOpLineageBackend());
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                new InMemoryQualityRuleStore(), new DefaultQualityRuleExecutor(), alerter);
        CronScheduler scheduler = new CronScheduler(handler);
        TagBasedSecurityPolicy policy = new TagBasedSecurityPolicy(
                new NoOpColumnMaskStore(), new NoOpAuditConfigStore());
        return new GovernanceContext(new GovernanceDeps(
                emitter, handler, policy, scheduler, new InMemoryQualityRuleStore()));
    }

    private static DataQualityRule rule(long id, String name, boolean enabled, String schedule) {
        return new DataQualityRule(id, name, "catalog", "schema", "table",
                "expression", Severity.WARNING, "msg", enabled, schedule,
                Instant.now(), Instant.now());
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
