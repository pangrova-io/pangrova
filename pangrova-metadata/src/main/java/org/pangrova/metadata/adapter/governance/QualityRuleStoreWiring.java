package org.pangrova.metadata.adapter.governance;

import java.util.Objects;

import org.pangrova.governance.core.quality.IdentityQualityRuleStore;
import org.pangrova.governance.api.quality.QualityRuleStore;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.service.RegistryTablePolicyAuthority;

/**
 * Metadata owner wiring entry for the quality-rule projection onto stable table identities.
 * Composition roots obtain the decorated store here rather than constructing it directly.
 */
public final class QualityRuleStoreWiring {

    private QualityRuleStoreWiring() {
    }

    /** Wraps the governance store with metadata's stable table-identity projection. */
    public static QualityRuleStore tableControl(IdentityQualityRuleStore delegate, CatalogRegistry catalogs) {
        return new TableControlQualityRuleStore(
                new RegistryTablePolicyAuthority(Objects.requireNonNull(catalogs, "catalogs")),
                Objects.requireNonNull(delegate, "delegate"));
    }
}
