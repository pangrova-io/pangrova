package org.pangrova.persistence.workflow.postgresql.security;

import org.pangrova.persistence.security.postgresql.policy.JdbcPolicyStoreBase;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.security.api.authorization.TableReadPolicySource.Target;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.policy.GovernanceBinding;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Reads the current tables and their foreground policies from one PostgreSQL statement snapshot. */
final class PostgresReadPolicySnapshot extends JdbcPolicyStoreBase {
    private static final String READ = """
            WITH read_target(catalog_name, schema_name, table_name, target_ordinal) AS (
                SELECT * FROM unnest(CAST(? AS text[]), CAST(? AS text[]), CAST(? AS text[])) WITH ORDINALITY
            ), registered AS MATERIALIZED (
                SELECT target.target_ordinal, control.table_id, control.state, control.catalog_incarnation_id,
                       control.catalog_revision, catalog.incarnation_id, catalog.revision,
                       catalog.state AS catalog_state
                FROM read_target target
                JOIN table_control_name_reservation reservation USING (catalog_name, schema_name, table_name)
                JOIN table_control_registry control ON control.table_id=reservation.table_id
                LEFT JOIN catalog_definitions catalog ON catalog.name=control.catalog_name
                WHERE control.state<>'DROPPED'
            ), policy_scope AS (
                SELECT registered.table_id, target.*
                FROM read_target target LEFT JOIN registered USING (target_ordinal)
            )
            SELECT r.target_ordinal, 0 AS entry_kind, r.table_id, r.state, r.catalog_incarnation_id, r.catalog_revision,
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
            SELECT t.target_ordinal, 1, p.table_id, NULL, NULL, NULL, NULL, NULL, NULL,
                   p.catalog_name, p.schema_name, p.table_name, p.apply_to_roles, p.enabled,
                   p.policy_id, p.policy_name, p.filter_expression, p.priority,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM row_policy p CROSS JOIN policy_scope t
            WHERE p.enabled AND (p.table_id=t.table_id OR (p.table_id IS NULL
                AND p.catalog_name=t.catalog_name AND p.schema_name=t.schema_name AND p.table_name=t.table_name))
            UNION ALL
            SELECT t.target_ordinal, 2, m.table_id, NULL, NULL, NULL, NULL, NULL, NULL,
                   m.catalog_name, m.schema_name, m.table_name, m.apply_to_roles, m.enabled,
                   NULL, NULL, NULL, NULL, m.mask_id, m.mask_name, m.column_name, m.mask_expression, m.exempt_roles,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM column_mask m CROSS JOIN read_target t
            WHERE m.enabled AND m.catalog_name=t.catalog_name AND m.schema_name=t.schema_name
                AND m.table_name=t.table_name
            UNION ALL
            SELECT r.target_ordinal, 3, b.table_id, NULL, NULL, NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   b.binding_id, b.binding_type, b.binding_ref, b.config, b.created_by, b.created_at, b.updated_at
            FROM table_control_governance_binding b JOIN registered r ON b.table_id=r.table_id
            ORDER BY target_ordinal, entry_kind, priority DESC NULLS LAST, policy_id, mask_id, binding_type, binding_ref
            """;

    PostgresReadPolicySnapshot(DataSource dataSource) {
        super(dataSource);
    }

    Map<Target, Snapshot> read(Set<Target> targets) {
        if (targets.isEmpty()) return Map.of();
        var ordered = List.copyOf(targets);
        String[][] keys = new String[3][ordered.size()];
        for (int index = 0; index < ordered.size(); index++) {
            var target = ordered.get(index);
            keys[0][index] = target.catalog();
            keys[1][index] = target.schema();
            keys[2][index] = target.table();
        }
        var observed = new SnapshotRows[ordered.size()];
        try (var connection = dataSource.getConnection(); var query = connection.prepareStatement(READ)) {
            Array[] arguments = new Array[keys.length];
            try {
                for (int index = 0; index < keys.length; index++) {
                    arguments[index] = connection.createArrayOf("text", keys[index]);
                    query.setArray(index + 1, arguments[index]);
                }
                try (var result = query.executeQuery()) {
                    while (result.next()) {
                        int index = result.getInt("target_ordinal") - 1;
                        if (index < 0 || index >= observed.length) {
                            throw new SQLException("Unexpected table ordinal in read policy result");
                        }
                        if (observed[index] == null) observed[index] = new SnapshotRows();
                        observed[index].append(result);
                    }
                }
            } finally {
                for (var argument : arguments) {
                    if (argument != null) argument.free();
                }
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to observe the current table read policies", failure);
        }
        Map<Target, Snapshot> policies = new LinkedHashMap<>();
        for (int index = 0; index < observed.length; index++) {
            policies.put(ordered.get(index), observed[index] == null ? Snapshot.EMPTY : observed[index].snapshot());
        }
        return Map.copyOf(policies);
    }

    private static final class SnapshotRows {
        private Optional<PostgresPolicyTableIdentity.VerifiedTable> identity = Optional.empty();
        private final List<RowPolicy> rows = new ArrayList<>();
        private final List<ColumnMask> masks = new ArrayList<>();
        private final List<GovernanceBinding> bindings = new ArrayList<>();

        private void append(ResultSet result) throws SQLException {
            switch (result.getInt("entry_kind")) {
                case 0 -> {
                    if (identity.isPresent()) throw new SQLException("Multiple identities for one read policy target");
                    identity = Optional.of(PostgresPolicyTableIdentity.verify(result,
                            "Read policy table identity or catalog deployment changed"));
                }
                case 1 -> rows.add(readRowPolicy(result));
                case 2 -> masks.add(readColumnMask(result));
                case 3 -> bindings.add(PostgresTablePolicyRepository.readBinding(result));
                default -> throw new SQLException("Unexpected read policy row type");
            }
        }

        private Snapshot snapshot() {
            return new Snapshot(identity, List.copyOf(rows), List.copyOf(masks), List.copyOf(bindings));
        }
    }

    record Snapshot(Optional<PostgresPolicyTableIdentity.VerifiedTable> identity,
            List<RowPolicy> rows, List<ColumnMask> masks, List<GovernanceBinding> bindings) {
        private static final Snapshot EMPTY = new Snapshot(Optional.empty(), List.of(), List.of(), List.of());
    }
}
