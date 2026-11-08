package org.pangrova.governance.core.quality;

import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.GovernanceQueryException;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.QualityRuleExecutor;

import java.util.Objects;

/** Test-only executor proving core orchestration does not depend on a provider implementation. */
public final class TestQualityRuleExecutor implements QualityRuleExecutor {
    @Override
    public QualityCheckResult execute(DataQualityRule rule) {
        Objects.requireNonNull(rule, "rule");
        throw new GovernanceQueryException("", "Quality rule '" + rule.ruleName()
                + "' was not evaluated: test executor");
    }
}
