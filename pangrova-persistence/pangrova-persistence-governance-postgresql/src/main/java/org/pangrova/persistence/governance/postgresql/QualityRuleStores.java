package org.pangrova.persistence.governance.postgresql;

import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import org.pangrova.governance.api.quality.IdentityQualityRuleStore;
import org.pangrova.governance.api.exception.QualityRuleStoreException;

/**
 * Owner wiring entry for quality-rule persistence. Composition roots obtain stores here
 * instead of constructing governance repositories directly.
 */
public final class QualityRuleStores {

    private QualityRuleStores() {
    }

    /** Creates the durable postgres-backed quality rule store from the canonical migrated schema. */
    public static IdentityQualityRuleStore jdbc(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        // Resolve required relations and authority columns without changing schema or reading rows.
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeQuery("""
                    SELECT r.execution_principal, r.execution_tenant, r.table_id,
                           i.rule_id, i.table_id, c.result_id, c.table_id
                    FROM quality_rules r, quality_rule_identity i, quality_check_results c
                    WHERE FALSE
                    """).close();
        } catch (SQLException failure) {
            throw new QualityRuleStoreException("Cannot open canonical quality-rule schema", failure);
        }
        return new JdbcQualityRuleStore(dataSource);
    }
}
