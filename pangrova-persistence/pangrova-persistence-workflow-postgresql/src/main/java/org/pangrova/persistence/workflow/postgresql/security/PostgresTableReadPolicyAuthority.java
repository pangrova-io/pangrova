package org.pangrova.persistence.workflow.postgresql.security;

import org.pangrova.persistence.workflow.api.security.TableControlPolicyProjections;
import org.pangrova.security.api.authorization.TableReadPolicySource;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.mask.TableColumnMasks;
import org.pangrova.security.api.rowfilter.RowPolicy;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
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
    private final Consumer<CatalogDeploymentIdentity> localCatalog;
    private final TableControlPolicyProjections projections;
    private final PostgresReadPolicySnapshot policyStore;
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
        this.localCatalog = Objects.requireNonNull(localCatalog, "localCatalog");
        this.projections = Objects.requireNonNull(projections, "projections");
        this.policyStore = new PostgresReadPolicySnapshot(Objects.requireNonNull(dataSource, "dataSource"));
        this.rowRenderer = Objects.requireNonNull(rowRenderer, "rowRenderer");
        this.maskRenderer = Objects.requireNonNull(maskRenderer, "maskRenderer");
    }

    @Override
    public Map<Target, PolicyRead> bind(UserIdentity user, Set<String> roles, Set<Target> targets) {
        var snapshots = policyStore.read(targets);
        var subjectRoles = Set.copyOf(roles);
        Map<Target, PolicyRead> reads = new LinkedHashMap<>();
        snapshots.forEach((target, policies) -> {
            policies.identity().ifPresent(identity -> localCatalog.accept(
                    new CatalogDeploymentIdentity(target.catalog(), identity.incarnation(), identity.revision())));
            UUID tableId = policies.identity().map(PostgresPolicyTableIdentity.VerifiedTable::tableId).orElse(null);
            var table = new PolicyTable(target.catalog(), target.schema(), target.table(), tableId);
            reads.put(target, new BoundPolicies(user, subjectRoles, table, policies));
        });
        return Map.copyOf(reads);
    }

    private record PolicyTable(String catalog, String schema, String name, UUID id) {}

    private final class BoundPolicies implements PolicyRead {
        private final UserIdentity user;
        private final Set<String> roles;
        private final PolicyTable table;
        private Optional<String> filter;
        private Map<String, List<ColumnMask>> byColumn;
        private final PostgresReadPolicySnapshot.Snapshot policies;

        private BoundPolicies(UserIdentity user, Set<String> roles, PolicyTable table,
                PostgresReadPolicySnapshot.Snapshot policies) {
            this.user = user;
            this.roles = roles;
            this.table = table;
            this.policies = policies;
        }

        @Override
        public Optional<String> rowFilter() {
            if (filter == null) {
                filter = rowRenderer.render(user, roles,
                        projections.projectRows(table.id(), table.catalog(), table.schema(), table.name(),
                                policies.rows()));
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
                var selected = policies.masks();
                var projected = projections.projectColumns(
                        table.id(), table.catalog(), table.schema(), table.name(), selected, policies.bindings());
                byColumn = projected.isEmpty() ? Map.of()
                        : projected.stream().collect(Collectors.groupingBy(ColumnMask::columnName));
            }
            return byColumn;
        }
    }
}
