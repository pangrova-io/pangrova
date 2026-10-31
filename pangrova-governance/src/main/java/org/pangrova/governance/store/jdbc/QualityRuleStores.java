package org.pangrova.governance.store.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import org.pangrova.governance.core.quality.IdentityQualityRuleStore;

/**
 * Owner wiring entry for quality-rule persistence. Composition roots obtain stores here
 * instead of constructing governance repositories directly.
 */
public final class QualityRuleStores {

    private QualityRuleStores() {
    }

    /** Creates and initializes the durable postgres-backed quality rule store. */
    public static IdentityQualityRuleStore jdbc(DataSource dataSource) {
        JdbcQualityRuleStore store = new JdbcQualityRuleStore(
                Objects.requireNonNull(dataSource, "dataSource"));
        store.initSchema();
        return store;
    }
}
