package org.pangrova.governance;

import java.util.Objects;

import org.pangrova.governance.classification.TagBasedSecurityPolicy;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.rule.QualityRuleStore;
import org.pangrova.governance.quality.schedule.CronScheduler;

/**
 * Groups GovernanceContext dependencies into a single parameter object.
 * Keeps the constructor within the ≤4 param governance limit.
 *
 * @param lineageEmitter   lineage event emitter (required)
 * @param statementHandler governance statement handler (required)
 * @param securityPolicy   tag-based security policy (nullable)
 * @param cronScheduler    quality rule scheduler (required)
 * @param ruleStore        quality rule store (required)
 */
public record GovernanceDeps(
        LineageEventEmitter lineageEmitter,
        GovernanceStatementHandler statementHandler,
        TagBasedSecurityPolicy securityPolicy,
        CronScheduler cronScheduler,
        QualityRuleStore ruleStore) {

    public GovernanceDeps {
        Objects.requireNonNull(lineageEmitter, "lineageEmitter must not be null");
        Objects.requireNonNull(statementHandler, "statementHandler must not be null");
        Objects.requireNonNull(cronScheduler, "cronScheduler must not be null");
        Objects.requireNonNull(ruleStore, "ruleStore must not be null");
    }
}
