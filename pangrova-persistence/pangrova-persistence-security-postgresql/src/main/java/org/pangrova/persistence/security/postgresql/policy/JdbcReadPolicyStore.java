package org.pangrova.persistence.security.postgresql.policy;

import org.pangrova.persistence.security.postgresql.support.JdbcPolicyStoreSupport;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Reads the foreground row policies and column masks for one authorized table. */
public final class JdbcReadPolicyStore extends JdbcPolicyStoreSupport {
    private static final String READ_TABLE_POLICIES = """
            WITH read_target(table_id, catalog_name, schema_name, table_name) AS (
                VALUES (CAST(? AS uuid), CAST(? AS text), CAST(? AS text), CAST(? AS text))
            )
            SELECT FALSE AS is_mask, p.catalog_name, p.schema_name, p.table_name, p.table_id,
                   p.apply_to_roles, p.enabled, p.policy_id, p.policy_name, p.filter_expression, p.priority,
                   NULL AS mask_id, NULL AS mask_name, NULL AS column_name, NULL AS mask_expression,
                   NULL AS exempt_roles
            FROM row_policy p CROSS JOIN read_target t
            WHERE p.enabled AND (p.table_id=t.table_id OR (p.table_id IS NULL
                AND p.catalog_name=t.catalog_name AND p.schema_name=t.schema_name AND p.table_name=t.table_name))
            UNION ALL
            SELECT TRUE, m.catalog_name, m.schema_name, m.table_name, m.table_id,
                   m.apply_to_roles, m.enabled, NULL, NULL, NULL, NULL,
                   m.mask_id, m.mask_name, m.column_name, m.mask_expression, m.exempt_roles
            FROM column_mask m CROSS JOIN read_target t
            WHERE m.enabled AND m.catalog_name=t.catalog_name AND m.schema_name=t.schema_name
                AND m.table_name=t.table_name
            ORDER BY is_mask, priority DESC NULLS LAST, policy_id, mask_id
            """;

    public JdbcReadPolicyStore(DataSource dataSource) {
        super(dataSource);
    }

    /** Loads all enabled read policies with the table's current logical name and optional stable identity. */
    public ReadPolicies read(String catalog, String schema, String table, UUID tableId) {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(READ_TABLE_POLICIES)) {
            statement.setObject(1, tableId);
            statement.setString(2, catalog);
            statement.setString(3, schema);
            statement.setString(4, table);
            try (var result = statement.executeQuery()) {
                List<RowPolicy> rows = new ArrayList<>();
                List<ColumnMask> masks = new ArrayList<>();
                while (result.next()) {
                    if (result.getBoolean("is_mask")) {
                        masks.add(readColumnMask(result));
                    } else {
                        rows.add(readRowPolicy(result));
                    }
                }
                return new ReadPolicies(rows, masks);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed reading table row and column policies", failure);
        }
    }

    public record ReadPolicies(List<RowPolicy> rows, List<ColumnMask> masks) {
        public ReadPolicies {
            rows = List.copyOf(rows);
            masks = List.copyOf(masks);
        }
    }
}
