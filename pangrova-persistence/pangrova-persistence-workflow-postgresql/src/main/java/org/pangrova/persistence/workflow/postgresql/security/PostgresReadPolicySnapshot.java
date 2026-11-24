package org.pangrova.persistence.workflow.postgresql.security;

import org.pangrova.persistence.security.postgresql.support.JdbcPolicyStoreSupport;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.policy.GovernanceBinding;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Reads the current table and all foreground policies from the same PostgreSQL snapshot. */
final class PostgresReadPolicySnapshot extends JdbcPolicyStoreSupport {
    private static final String READ = """
            WITH read_target(catalog_name, schema_name, table_name) AS (
                VALUES (CAST(? AS text), CAST(? AS text), CAST(? AS text))
            ), registered AS MATERIALIZED (
                SELECT control.table_id, control.state, control.catalog_incarnation_id, control.catalog_revision,
                       catalog.incarnation_id, catalog.revision, catalog.state AS catalog_state
                FROM table_control_registry control
                LEFT JOIN catalog_definitions catalog ON catalog.name=control.catalog_name
                WHERE control.state<>'DROPPED' AND control.table_id IN (
                    SELECT reservation.table_id FROM table_control_name_reservation reservation
                    JOIN read_target target USING (catalog_name, schema_name, table_name))
            ), policy_scope AS (
                SELECT registered.table_id, target.*
                FROM read_target target LEFT JOIN registered ON TRUE
            )
            SELECT 0 AS entry_kind, r.table_id, r.state, r.catalog_incarnation_id, r.catalog_revision,
                   r.incarnation_id, r.revision, r.catalog_state,
                   NULL::text AS catalog_name, NULL::text AS schema_name, NULL::text AS table_name,
                   NULL::text AS apply_to_roles, NULL::boolean AS enabled,
                   NULL::bigint AS policy_id, NULL::text AS policy_name, NULL::text AS filter_expression,
                   NULL::integer AS priority,
                   NULL::bigint AS mask_id, NULL::text AS mask_name, NULL::text AS column_name,
                   NULL::text AS mask_expression, NULL::text AS exempt_roles,
                   NULL::uuid AS binding_id, NULL::text AS binding_type, NULL::text AS binding_ref,
                   NULL::jsonb AS config, NULL::text AS created_by,
                   NULL::timestamptz AS created_at, NULL::timestamptz AS updated_at
            FROM registered r
            UNION ALL
            SELECT 1, p.table_id, NULL, NULL, NULL, NULL, NULL, NULL,
                   p.catalog_name, p.schema_name, p.table_name, p.apply_to_roles, p.enabled,
                   p.policy_id, p.policy_name, p.filter_expression, p.priority,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM row_policy p CROSS JOIN policy_scope t
            WHERE p.enabled AND (p.table_id=t.table_id OR (p.table_id IS NULL
                AND p.catalog_name=t.catalog_name AND p.schema_name=t.schema_name AND p.table_name=t.table_name))
            UNION ALL
            SELECT 2, m.table_id, NULL, NULL, NULL, NULL, NULL, NULL,
                   m.catalog_name, m.schema_name, m.table_name, m.apply_to_roles, m.enabled,
                   NULL, NULL, NULL, NULL, m.mask_id, m.mask_name, m.column_name, m.mask_expression, m.exempt_roles,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM column_mask m CROSS JOIN read_target t
            WHERE m.enabled AND m.catalog_name=t.catalog_name AND m.schema_name=t.schema_name
                AND m.table_name=t.table_name
            UNION ALL
            SELECT 3, b.table_id, NULL, NULL, NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   b.binding_id, b.binding_type, b.binding_ref, b.config, b.created_by, b.created_at, b.updated_at
            FROM table_control_governance_binding b JOIN registered r ON b.table_id=r.table_id
            ORDER BY entry_kind, priority DESC NULLS LAST, policy_id, mask_id, binding_type, binding_ref
            """;

    PostgresReadPolicySnapshot(DataSource dataSource) {
        super(dataSource);
    }

    Snapshot read(String catalog, String schema, String table) {
        try (var connection = dataSource.getConnection(); var query = connection.prepareStatement(READ)) {
            query.setString(1, catalog);
            query.setString(2, schema);
            query.setString(3, table);
            try (var result = query.executeQuery()) {
                Optional<PostgresPolicyTableIdentity.VerifiedTable> identity = Optional.empty();
                List<RowPolicy> rows = new ArrayList<>();
                List<ColumnMask> masks = new ArrayList<>();
                List<GovernanceBinding> bindings = new ArrayList<>();
                while (result.next()) {
                    switch (result.getInt("entry_kind")) {
                        case 0 -> {
                            identity = Optional.of(PostgresPolicyTableIdentity.verify(result,
                                    "Read policy table identity or catalog deployment changed"));
                        }
                        case 1 -> rows.add(readRowPolicy(result));
                        case 2 -> masks.add(readColumnMask(result));
                        case 3 -> bindings.add(PostgresTablePolicyRepository.readBinding(result));
                        default -> throw new SQLException("Unexpected read policy row type");
                    }
                }
                return new Snapshot(identity, List.copyOf(rows), List.copyOf(masks), List.copyOf(bindings));
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to observe the current table read policies", failure);
        }
    }

    record Snapshot(Optional<PostgresPolicyTableIdentity.VerifiedTable> identity,
            List<RowPolicy> rows, List<ColumnMask> masks, List<GovernanceBinding> bindings) {}
}
