package org.pangrova.governance.classification;

import org.pangrova.governance.GovernanceDeps;
import org.pangrova.governance.GovernanceContext;
import org.pangrova.governance.lineage.LineageBackend;
import org.pangrova.governance.lineage.LineageEvent;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.quality.alert.AlertChannelRegistry;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.pangrova.governance.quality.rule.UnconfiguredQualityRuleExecutor;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.rule.InMemoryQualityRuleStore;
import org.pangrova.governance.quality.schedule.CronScheduler;
import org.pangrova.spi.security.ColumnMaskRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the full TagChangedEvent pipeline through GovernanceContext:
 * ColumnTagService emits event → TagBasedSecurityPolicy receives it → mask applied.
 */
class TagChangedEventWiringTest {
    private static final java.util.Map<org.pangrova.spi.connector.metadata.table.QualifiedTableName, java.util.UUID>
            tagTableIdentities = new java.util.concurrent.ConcurrentHashMap<>();


    private GovernanceContext wiring;
    private InMemoryColumnMaskStore maskStore;

    @BeforeEach
    void setUp() {
        maskStore = new InMemoryColumnMaskStore();
        TagBasedSecurityPolicy securityPolicy = new TagBasedSecurityPolicy(
                maskStore, new InMemoryAuditConfigStore());
        LineageEventEmitter emitter = new LineageEventEmitter(new NoOpLineageBackend());
        InMemoryQualityRuleStore ruleStore = new InMemoryQualityRuleStore();
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                ruleStore, new UnconfiguredQualityRuleExecutor(), alerter);
        CronScheduler scheduler = new CronScheduler(handler);

        wiring = new GovernanceContext(new GovernanceDeps(
                emitter, handler, securityPolicy, scheduler, ruleStore, new org.pangrova.governance.classification.ColumnTagService(
                        table -> tagTableIdentities.computeIfAbsent(table, ignored -> java.util.UUID.randomUUID()))));
        wiring.initialize();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (wiring != null) {
            wiring.close();
        }
    }

    @Test
    void assignPiiTagEmitsEventAndMaskAppliedThroughWiring() {
        ColumnTagService tagService = wiring.columnTagService();

        // Assign PII tag to column
        tagService.assignTag("catalog", "schema", "users", "email",
                "pii_type", null, "PII_EMAIL");

        // Verify mask was applied by TagBasedSecurityPolicy via the event pipeline
        List<ColumnMaskRecord> masks = maskStore.getMasks("catalog", "schema", "users", "email");
        assertThat(masks).hasSize(1);
        assertThat(masks.get(0).maskExpression()).isEqualTo("CASE WHEN {column} IS NULL THEN NULL ELSE '***@***' END");
        assertThat(masks.get(0).maskName()).isEqualTo("auto_pii_email");
        assertThat(masks.get(0).enabled()).isTrue();
    }

    @Test
    void tagChangedEventEmittedOnAssign() {
        ColumnTagService tagService = wiring.columnTagService();
        AtomicReference<TagChangedEvent> captured = new AtomicReference<>();
        tagService.addListener(captured::set);

        tagService.assignTag("cat", "sch", "tbl", "phone",
                "pii_type", null, "PII_PHONE");

        TagChangedEvent event = captured.get();
        assertThat(event).isNotNull();
        assertThat(event.catalog()).isEqualTo("cat");
        assertThat(event.schema()).isEqualTo("sch");
        assertThat(event.table()).isEqualTo("tbl");
        assertThat(event.columnName()).isEqualTo("phone");
        assertThat(event.tagKey()).isEqualTo("pii_type");
        assertThat(event.oldValue()).isNull();
        assertThat(event.newValue()).isEqualTo("PII_PHONE");
    }

    @Test
    void removeTagEmitsEventAndRemovesMask() {
        ColumnTagService tagService = wiring.columnTagService();

        // Assign then remove
        tagService.assignTag("catalog", "schema", "users", "ssn",
                "pii_type", null, "PII_SSN");
        assertThat(maskStore.getMasks("catalog", "schema", "users", "ssn")).hasSize(1);

        tagService.removeTag("catalog", "schema", "users", "ssn",
                "pii_type", "PII_SSN");

        assertThat(maskStore.getMasks("catalog", "schema", "users", "ssn")).isEmpty();
    }

    static class NoOpLineageBackend implements LineageBackend {
        @Override
        public void send(LineageEvent event) { }

        @Override
        public boolean isAvailable() { return true; }
    }
}
