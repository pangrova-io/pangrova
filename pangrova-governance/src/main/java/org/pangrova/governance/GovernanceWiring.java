package org.pangrova.governance;

import org.pangrova.governance.classification.TagBasedSecurityPolicy;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.schedule.CronScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy thin wrapper around {@link GovernanceContext}. Retained because
 * {@code ModuleIntegrationWiring} (the legacy bootstrap variant) and a couple
 * of tests still construct one. New call sites should use {@link GovernanceContext}
 * directly via {@link GovernanceDeps}.
 */
public class GovernanceWiring implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GovernanceWiring.class);

    private final GovernanceContext context;

    public GovernanceWiring(GovernanceContext context) {
        this.context = context;
    }

    public void initialize() {
        context.initialize();
    }

    public LineageEventEmitter lineageEmitter() { return context.lineageEmitter(); }
    public GovernanceStatementHandler statementHandler() { return context.statementHandler(); }
    public TagBasedSecurityPolicy securityPolicy() { return context.securityPolicy(); }
    public org.pangrova.governance.classification.ColumnTagService columnTagService() {
        return context.columnTagService();
    }
    public CronScheduler cronScheduler() { return context.cronScheduler(); }
    public GovernanceContext context() { return context; }

    @Override
    public void close() {
        context.close();
    }
}
