package org.pangrova.persistence.governance.postgresql;

import java.util.Objects;

import javax.sql.DataSource;

import org.pangrova.governance.api.quality.IdentityQualityRuleStore;

/**
 * Owner wiring entry for quality-rule persistence. Composition roots obtain stores here
 * instead of constructing governance repositories directly.
 */
public final class QualityRuleStores {

    private QualityRuleStores() {
    }

    /** Creates the durable postgres-backed quality rule store from the canonical migrated schema. */
    public static IdentityQualityRuleStore jdbc(DataSource dataSource) {
        return new JdbcQualityRuleStore(Objects.requireNonNull(dataSource, "dataSource"));
    }
}
