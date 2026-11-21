package org.pangrova.metadata.governance;

import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.exception.GovernanceQueryException;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.QualityRuleExecutor;

import java.util.Objects;

/** Test-only executor; avoids linking a governance provider into Metadata Core tests. */
public final class TestQualityRuleExecutor implements QualityRuleExecutor {
    @Override
    public QualityCheckResult execute(DataQualityRule rule) {
        Objects.requireNonNull(rule, "rule");
        throw new GovernanceQueryException("", "Quality rule '" + rule.ruleName()
                + "' was not evaluated: metadata test executor");
    }
}
