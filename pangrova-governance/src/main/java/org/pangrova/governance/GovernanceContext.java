package org.pangrova.governance;

import org.pangrova.governance.classification.ColumnTagService;
import org.pangrova.governance.classification.TagBasedSecurityPolicy;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.schedule.CronScheduler;
import org.pangrova.governance.quality.rule.QualityRuleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point wiring for the governance module.
 * Connects lineage, quality, and classification components.
 */
public class GovernanceContext implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GovernanceContext.class);

    private final LineageEventEmitter lineageEmitter;
    private final GovernanceStatementHandler statementHandler;
    private final TagBasedSecurityPolicy securityPolicy;
    private final ColumnTagService columnTagService;
    private final CronScheduler cronScheduler;
    private final QualityRuleStore ruleStore;

    /** Primary constructor accepting a dependencies parameter object. */
    public GovernanceContext(GovernanceDeps deps) {
        this.lineageEmitter = deps.lineageEmitter();
        this.statementHandler = deps.statementHandler();
        this.securityPolicy = deps.securityPolicy();
        this.cronScheduler = deps.cronScheduler();
        this.ruleStore = deps.ruleStore();
        this.columnTagService = new ColumnTagService();
    }

    /**
     * Loads all enabled quality rules from the store and schedules them.
     * Registers TagBasedSecurityPolicy as a listener on ColumnTagService
     * so masking rules activate on tag changes.
     * Called during bootstrap to activate governance capabilities.
     */
    public void initialize() {
        initialize(true);
    }

    /** Query coordinators may own durable scheduling separately from lineage and classification. */
    public void initialize(boolean scheduleQualityRules) {
        if (securityPolicy != null) {
            columnTagService.addListener(securityPolicy);
            LOG.info("action=tag_listener_registered listener=TagBasedSecurityPolicy");
        }
        var rules = scheduleQualityRules ? ruleStore.getAllEnabledRules() : java.util.List.<
                org.pangrova.governance.quality.rule.DataQualityRule>of();
        rules.forEach(cronScheduler::scheduleRule);
        LOG.info("action=governance_initialized rulesScheduled={}", rules.size());
    }

    public LineageEventEmitter lineageEmitter() { return lineageEmitter; }
    public GovernanceStatementHandler statementHandler() { return statementHandler; }
    public TagBasedSecurityPolicy securityPolicy() { return securityPolicy; }
    public ColumnTagService columnTagService() { return columnTagService; }
    public CronScheduler cronScheduler() { return cronScheduler; }

    @Override
    public void close() {
        // Stop actual quality producers before joining the lineage consumer.
        cronScheduler.close();
        lineageEmitter.close();
        if (lineageEmitter.shutdownTimedOut()) {
            throw new IllegalStateException("Governance lineage owner has not actually terminated");
        }
    }
}
