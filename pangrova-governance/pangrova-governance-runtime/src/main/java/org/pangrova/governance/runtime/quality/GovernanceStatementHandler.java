package org.pangrova.governance.runtime.quality;

import org.pangrova.governance.api.quality.QualityCheckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityRuleExecutor;
import org.pangrova.governance.api.quality.QualityRuleStore;

/**
 * Runs quality rule administration and checks for the scheduler and embedded callers.
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

    /** Runs every enabled quality rule of a table and raises alerts for failures. */
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
        QualityCheckResult result = QualityRuleEvaluation.execute(ruleExecutor, rule);
        ruleStore.saveResult(result);
        alerter.alert(result);
        return result;
    }
}
