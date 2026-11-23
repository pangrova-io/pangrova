package org.pangrova.persistence.workflow.postgresql.security;

import org.pangrova.persistence.security.postgresql.policy.JdbcReadPolicyStore;
import org.pangrova.persistence.workflow.api.security.TableControlPolicyProjections;
import org.pangrova.security.api.authorization.TableReadPolicySource;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.mask.TableColumnMasks;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.table.api.policy.GovernanceBinding;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

/** PostgreSQL row and column policies bound to one current table identity. */
public final class PostgresTableReadPolicyAuthority implements TableReadPolicySource {
    private final DataSource dataSource;
    private final Consumer<CatalogDeploymentIdentity> localCatalog;
    private final TableControlPolicyProjections projections;
    private final JdbcReadPolicyStore policyStore;
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
    public PostgresTableReadPolicyAuthority(DataSource dataSource,
            Consumer<CatalogDeploymentIdentity> localCatalog, TableControlPolicyProjections projections,
            RowPolicyRenderer rowRenderer, ColumnMaskRenderer maskRenderer) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.localCatalog = Objects.requireNonNull(localCatalog, "localCatalog");
        this.projections = Objects.requireNonNull(projections, "projections");
        this.policyStore = new JdbcReadPolicyStore(dataSource);
        this.rowRenderer = Objects.requireNonNull(rowRenderer, "rowRenderer");
        this.maskRenderer = Objects.requireNonNull(maskRenderer, "maskRenderer");
    }

    @Override
    public PolicyRead bind(UserIdentity user, Set<String> roles, String catalog, String schema, String table) {
        return new BoundPolicies(user, Set.copyOf(roles), currentTable(catalog, schema, table));
    }

    private PolicyTable currentTable(String catalog, String schema, String table) {
        try (var connection = dataSource.getConnection()) {
            var observed = PostgresPolicyTableIdentity.findRead(connection, catalog, schema, table,
                    "Read policy table identity or catalog deployment changed");
            if (observed.isEmpty()) return new PolicyTable(catalog, schema, table, null, List.of());
            var read = observed.orElseThrow();
            var identity = read.table();
            localCatalog.accept(new CatalogDeploymentIdentity(catalog, identity.incarnation(), identity.revision()));
            return new PolicyTable(catalog, schema, table, identity.tableId(), read.bindings());
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to observe the current table policy identity", failure);
        }
    }

    private record PolicyTable(String catalog, String schema, String name, UUID id,
            List<GovernanceBinding> bindings) {}

    private final class BoundPolicies implements PolicyRead {
        private final UserIdentity user;
        private final Set<String> roles;
        private final PolicyTable table;
        private Optional<String> filter;
        private Map<String, List<ColumnMask>> byColumn;
        private JdbcReadPolicyStore.ReadPolicies policies;

        private BoundPolicies(UserIdentity user, Set<String> roles, PolicyTable table) {
            this.user = user;
            this.roles = roles;
            this.table = table;
        }

        @Override
        public Optional<String> rowFilter() {
            if (filter == null) {
                filter = rowRenderer.render(user, roles,
                        projections.projectRows(table.id(), table.catalog(), table.schema(), table.name(),
                                policies().rows()));
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
                var selected = policies().masks();
                var projected = projections.projectColumns(
                        table.id(), table.catalog(), table.schema(), table.name(), selected, table.bindings());
                byColumn = projected.isEmpty() ? Map.of()
                        : projected.stream().collect(Collectors.groupingBy(ColumnMask::columnName));
            }
            return byColumn;
        }

        private JdbcReadPolicyStore.ReadPolicies policies() {
            if (policies == null) {
                policies = policyStore.read(table.catalog(), table.schema(), table.name(), table.id());
            }
            return policies;
        }
    }
}
