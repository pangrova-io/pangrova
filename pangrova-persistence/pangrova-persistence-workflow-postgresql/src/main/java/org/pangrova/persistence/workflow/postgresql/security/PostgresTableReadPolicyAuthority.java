package org.pangrova.persistence.workflow.postgresql.security;

import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.persistence.workflow.api.security.TableControlPolicyProjections;
import org.pangrova.security.api.authorization.TableReadPolicySource;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.mask.TableColumnMasks;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.policy.GovernanceBinding;
import org.pangrova.table.api.policy.TablePolicyAuthority;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** PostgreSQL row and column policies bound to one current table identity. */
public final class PostgresTableReadPolicyAuthority implements TableReadPolicySource {
    private final TablePolicyAuthority tables;
    private final TableControlPolicyProjections projections;
    private final JdbcRowPolicyStore rows;
    private final JdbcColumnMaskStore masks;
    private final RowPolicyRenderer rowRenderer;
    private final ColumnMaskRenderer maskRenderer;

    /** Evaluates the current row policies using the configured Security implementation. */
    @FunctionalInterface
    public interface RowPolicyRenderer {
        Optional<String> render(UserIdentity user, Set<String> roles, List<RowPolicy> policies);
    }

    /** Evaluates current column policies using the configured Security implementation. */
    @FunctionalInterface
    public interface ColumnMaskRenderer {
        Optional<String> render(UserIdentity user, Set<String> roles, String column, List<ColumnMask> masks);
    }

    /** Composes the durable policy stores with the live table identity and governance projections. */
    public PostgresTableReadPolicyAuthority(DataSource dataSource, TablePolicyAuthority tables,
            TableControlPolicyProjections projections,
            RowPolicyRenderer rowRenderer, ColumnMaskRenderer maskRenderer) {
        this.tables = Objects.requireNonNull(tables, "tables");
        this.projections = Objects.requireNonNull(projections, "projections");
        this.rows = new JdbcRowPolicyStore(dataSource);
        this.masks = new JdbcColumnMaskStore(dataSource);
        this.rowRenderer = Objects.requireNonNull(rowRenderer, "rowRenderer");
        this.maskRenderer = Objects.requireNonNull(maskRenderer, "maskRenderer");
    }

    @Override
    public PolicyRead bind(UserIdentity user, Set<String> roles, String catalog, String schema, String table) {
        var record = tables.find(catalog, schema, table);
        record.ifPresent(current -> {
            tables.requireCurrentCatalog(current, "Read policies");
            if (current.state() != TableControlState.ACTIVE) {
                throw new AccessDeniedException("Read policies require an ACTIVE controlled table");
            }
        });
        var target = new PolicyTable(catalog, schema, table, record.map(TableControlRecord::tableId).orElse(null));
        return new BoundPolicies(user, Set.copyOf(roles), target);
    }

    private record PolicyTable(String catalog, String schema, String name, UUID id) {}

    private final class BoundPolicies implements PolicyRead {
        private final UserIdentity user;
        private final Set<String> roles;
        private final PolicyTable table;
        private Optional<String> filter;
        private Map<String, List<ColumnMask>> byColumn;

        private BoundPolicies(UserIdentity user, Set<String> roles, PolicyTable table) {
            this.user = user;
            this.roles = roles;
            this.table = table;
        }

        @Override
        public Optional<String> rowFilter() {
            if (filter == null) {
                var policies = rows.getPoliciesForCurrentTable(
                        table.catalog(), table.schema(), table.name(), table.id());
                filter = rowRenderer.render(user, roles,
                        projections.projectRows(table.id(), table.catalog(), table.schema(), table.name(), policies));
            }
            return filter;
        }

        @Override
        public TableColumnMasks columnMasks() {
            var selected = masksByColumn();
            return column -> {
                var applicable = selected.get(column);
                return applicable == null ? Optional.empty() : renderMask(column, applicable);
            };
        }

        @Override
        public boolean hasApplicableMask() {
            return masksByColumn().entrySet().stream().anyMatch(entry ->
                    renderMask(entry.getKey(), entry.getValue()).isPresent());
        }

        private Optional<String> renderMask(String column, List<ColumnMask> selected) {
            return maskRenderer.render(user, roles, column, selected);
        }

        private Map<String, List<ColumnMask>> masksByColumn() {
            if (byColumn == null) {
                var selected = masks.getTableMasks(table.catalog(), table.schema(), table.name());
                var bindings = table.id() == null ? List.<GovernanceBinding>of()
                        : tables.bindingsFor(table.id());
                var projected = projections.projectColumns(
                        table.id(), table.catalog(), table.schema(), table.name(), selected, bindings);
                byColumn = projected.isEmpty() ? Map.of()
                        : projected.stream().collect(Collectors.groupingBy(ColumnMask::columnName));
            }
            return byColumn;
        }
    }
}
