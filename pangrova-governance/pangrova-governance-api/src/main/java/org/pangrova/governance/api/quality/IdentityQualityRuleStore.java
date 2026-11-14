package org.pangrova.governance.api.quality;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.UUID;

/** Canonical rule persistence with table ownership independent of diagnostic names. */
@Experimental
public interface IdentityQualityRuleStore extends QualityRuleStore {
    List<DataQualityRule> getRules(UUID tableId);
}
