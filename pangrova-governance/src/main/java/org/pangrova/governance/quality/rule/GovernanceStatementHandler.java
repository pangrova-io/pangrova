package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Handles data quality SQL statements: CREATE/SHOW/RUN DATA QUALITY.
 */
public class GovernanceStatementHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GovernanceStatementHandler.class);

    private final QualityRuleStore ruleStore;
    private final QualityRuleExecutor ruleExecutor;
    private final DataQualityAlerter alerter;

    public GovernanceStatementHandler(QualityRuleStore ruleStore,
                                      QualityRuleExecutor ruleExecutor,
                                      DataQualityAlerter alerter) {
        this.ruleStore = checkNotNull(ruleStore, "ruleStore must not be null");
        this.ruleExecutor = checkNotNull(ruleExecutor, "ruleExecutor must not be null");
        this.alerter = checkNotNull(alerter, "alerter must not be null");
    }

    public void handleCreateRule(DataQualityRule rule) {
        ruleStore.saveRule(rule);
        LOG.info("event=quality_rule_created ruleName={} tableName={}",
                rule.ruleName(), rule.tableName());
    }

    public List<DataQualityRule> handleShowRules(String catalogName, String schemaName,
                                                  String tableName) {
        return ruleStore.getRulesForTable(catalogName, schemaName, tableName);
    }

    public List<QualityCheckResult> handleRunCheck(String catalogName, String schemaName,
                                                    String tableName) {
        List<DataQualityRule> rules = ruleStore.getRulesForTable(
                catalogName, schemaName, tableName);
        return rules.stream()
            .filter(DataQualityRule::enabled)
            .map(this::executeAndAlert)
            .toList();
    }

    /** Re-read the current rule so scheduled callbacks never execute a stale definition or a sibling rule. */
    public Optional<QualityCheckResult> handleRunRule(long ruleId) {
        return ruleStore.getRule(ruleId).filter(DataQualityRule::enabled).map(this::executeAndAlert);
    }

    private QualityCheckResult executeAndAlert(DataQualityRule rule) {
        QualityCheckResult result = executeRule(rule);
        ruleStore.saveResult(result);
        alerter.alert(result);
        return result;
    }

    private QualityCheckResult executeRule(DataQualityRule rule) {
        LOG.info("event=quality_rule_executing ruleName={} tableName={}",
                rule.ruleName(), rule.tableName());
        long startNanos = System.nanoTime();
        try {
            QualityCheckResult result = ruleExecutor.execute(rule);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            LOG.info("event=quality_rule_completed ruleName={} passed={} totalRows={} failedRows={} executionTimeMs={}",
                    rule.ruleName(), result.passed(),
                    result.totalRows(), result.failedRows(), elapsedMs);
            return result;
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            LOG.error("event=quality_rule_failed ruleName={} tableName={} executionTimeMs={} error={}",
                    rule.ruleName(), rule.tableName(), elapsedMs, e.getMessage(), e);
            return new QualityCheckResult(
                    0, rule.ruleId(), rule.ruleName(), rule.tableName(),
                    rule.severity(), Instant.now(), false, 0, 0, elapsedMs,
                    e.getMessage());
        }
    }
}
