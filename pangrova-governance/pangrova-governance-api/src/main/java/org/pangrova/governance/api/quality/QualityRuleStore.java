package org.pangrova.governance.api.quality;

import java.util.List;
import java.util.Optional;

/**
 * SPI for persisting data quality rules and results.
 */
public interface QualityRuleStore {
    void saveRule(DataQualityRule rule);
    Optional<DataQualityRule> getRule(long ruleId);
    List<DataQualityRule> getRulesForTable(String catalogName, String schemaName, String tableName);
    List<DataQualityRule> getAllEnabledRules();
    void saveResult(QualityCheckResult result);
}
