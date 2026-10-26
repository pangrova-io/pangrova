package org.pangrova.governance.quality.rule;

import java.util.List;
import java.util.UUID;

/** Canonical rule persistence with table ownership independent of diagnostic names. */
public interface IdentityQualityRuleStore extends QualityRuleStore {
    List<DataQualityRule> getRules(UUID tableId);
}
