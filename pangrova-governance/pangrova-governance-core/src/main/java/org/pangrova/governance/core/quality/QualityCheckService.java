package org.pangrova.governance.core.quality;

import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityRuleExecutor;
import org.pangrova.governance.api.quality.QualityRuleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import org.pangrova.governance.api.quality.QualityCheckResult;

/**
 * On-demand data quality check API.
 *
 * <p>Provides programmatic access to trigger quality checks for specific tables
 * or individual rules. Results are persisted to the governance catalog and
 * alerts are fired for failures.</p>
 *
 * <p>This service is the primary entry point for on-demand quality evaluation,
 * complementing the pre-commit gate (write-time) and scheduled scans (periodic).</p>
 */
public final class QualityCheckService {

    private static final Logger LOG = LoggerFactory.getLogger(QualityCheckService.class);

    private final QualityRuleStore ruleStore;
    private final QualityRuleExecutor ruleExecutor;
    private final DataQualityAlerter alerter;

    public QualityCheckService(QualityRuleStore ruleStore,
                               QualityRuleExecutor ruleExecutor,
                               DataQualityAlerter alerter) {
        this.ruleStore = checkNotNull(ruleStore, "ruleStore must not be null");
        this.ruleExecutor = checkNotNull(ruleExecutor, "ruleExecutor must not be null");
        this.alerter = checkNotNull(alerter, "alerter must not be null");
    }

    /**
     * Run all enabled quality rules for the specified table.
     *
     * @param catalogName catalog containing the target table
     * @param schemaName  schema containing the target table
     * @param tableName   target table name
     * @return results for each evaluated rule
     */
    public List<QualityCheckResult> checkTable(String catalogName, String schemaName,
                                               String tableName) {
        checkNotNull(catalogName, "catalogName must not be null");
        checkNotNull(schemaName, "schemaName must not be null");
        checkNotNull(tableName, "tableName must not be null");

        List<DataQualityRule> rules = ruleStore.getRulesForTable(catalogName, schemaName, tableName)
                .stream()
                .filter(DataQualityRule::enabled)
                .toList();

        LOG.info("event=on_demand_quality_check_started table={}.{}.{} ruleCount={}",
                catalogName, schemaName, tableName, rules.size());

        List<QualityCheckResult> results = rules.stream()
                .map(this::executeAndStore)
                .toList();

        long failedCount = results.stream().filter(r -> !r.passed()).count();
        LOG.info("event=on_demand_quality_check_completed table={}.{}.{} total={} passed={} failed={}",
                catalogName, schemaName, tableName, results.size(),
                results.size() - failedCount, failedCount);

        return results;
    }

    /**
     * Run a single quality rule by ID.
     *
     * @param ruleId the rule identifier
     * @return the check result
     * @throws IllegalArgumentException if the rule does not exist
     */
    public QualityCheckResult checkRule(long ruleId) {
        DataQualityRule rule = ruleStore.getRule(ruleId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Quality rule not found: ruleId=" + ruleId));

        if (!rule.enabled()) {
            throw new IllegalArgumentException("Quality rule is disabled: ruleId=" + ruleId);
        }
        LOG.info("event=on_demand_single_rule_check ruleId={} ruleName={}",
                rule.ruleId(), rule.ruleName());

        return executeAndStore(rule);
    }

    private QualityCheckResult executeAndStore(DataQualityRule rule) {
        QualityCheckResult result = ruleExecutor.execute(rule);
        ruleStore.saveResult(result);
        alerter.alert(result);
        return result;
    }
}
