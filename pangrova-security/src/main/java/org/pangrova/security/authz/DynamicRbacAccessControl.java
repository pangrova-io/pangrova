package org.pangrova.security.authz;

import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Objects;

/**
 * Current authorization from the authoritative role catalog and privilege store.
 * Every check reads current grants; storage failures never reuse an earlier permission.
 * Identity groups remain distinct from explicitly resolved Pangrova roles.
 */
public final class DynamicRbacAccessControl implements AccessControl {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicRbacAccessControl.class);

    private final GrantStore grantStore;
    private final RoleStore roleStore;
    private final RowLevelSecurityFilter rlsFilter;
    private final ColumnMaskingProcessor maskProcessor;

    /** Creates the sole RBAC path with a mandatory role authority. */
    public DynamicRbacAccessControl(GrantStore grantStore, RoleStore roleStore,
                                    RowLevelSecurityFilter rlsFilter,
                                    ColumnMaskingProcessor maskProcessor) {
        this.grantStore = Objects.requireNonNull(grantStore, "grantStore");
        this.roleStore = Objects.requireNonNull(roleStore, "roleStore");
        this.rlsFilter = Objects.requireNonNull(rlsFilter, "rlsFilter");
        this.maskProcessor = Objects.requireNonNull(maskProcessor, "maskProcessor");
    }

    /** Resolves current direct assignments and explicitly mapped identity groups. */
    public Set<String> effectiveRoles(UserIdentity user) {
        if (user == null || user.principal() == null || user.principal().isBlank()) {
            throw new AuthorizationDeniedException("Authenticated principal is required");
        }
        Set<String> roles = new HashSet<>(roleStore.rolesForUser(user.principal()));
        for (String group : user.groups()) {
            roles.addAll(roleStore.rolesForGroup(group));
        }
        roles.removeIf(role -> roleStore.findRole(role).isEmpty());
        return Set.copyOf(roles);
    }

    @Override
    public void checkCanAccessCatalog(UserIdentity user, String catalog) {
        requireAnyGrant(user, catalog, null, null, Privilege.SELECT);
    }

    @Override
    public void checkCanCreateCatalog(UserIdentity user, String catalog) {
        requireCatalogGrant(user, catalog, Privilege.CREATE_CATALOG);
    }

    @Override
    public void checkCanDropCatalog(UserIdentity user, String catalog) {
        requireCatalogGrant(user, catalog, Privilege.DROP_CATALOG);
    }

    @Override
    public void checkCanAlterCatalog(UserIdentity user, String catalog) {
        requireCatalogGrant(user, catalog, Privilege.ALTER_CATALOG);
    }

    @Override
    public void checkCanAccessSchema(UserIdentity user, String catalog, String schema) {
        requireAnyGrant(user, catalog, schema, null, Privilege.SELECT);
    }

    @Override
    public void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.SELECT);
    }

    @Override
    public void checkCanInsertIntoTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.INSERT);
    }

    @Override
    public void checkCanDeleteFromTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.DELETE);
    }

    @Override
    public void checkCanUpdateTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.UPDATE);
    }

    @Override
    public void checkCanDropTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.DROP);
    }

    @Override
    public void checkCanAlterTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.ALTER);
    }

    @Override
    public void checkCanCreateTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.CREATE_TABLE);
    }

    @Override
    public void checkCanRenameTable(UserIdentity user,
                                    String catalog, String schema, String oldTable,
                                    String newCatalog, String newSchema, String newTable) {
        requireTableGrant(user, catalog, schema, oldTable, Privilege.RENAME_TABLE);
        requireTableGrant(user, newCatalog, newSchema, newTable, Privilege.CREATE_TABLE);
    }

    @Override
    public void checkCanCreateSchema(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.CREATE_SCHEMA);
    }

    @Override
    public void checkCanDropSchema(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.DROP_SCHEMA);
    }

    @Override
    public void checkCanAlterSchema(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.ALTER_SCHEMA);
    }

    @Override
    public void checkCanShowSchemas(UserIdentity user, String catalog) {
        requireAnyGrant(user, catalog, null, null, Privilege.SHOW_SCHEMAS);
    }

    @Override
    public void checkCanShowTables(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.SHOW_TABLES);
    }

    @Override
    public void checkCanShowColumns(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.SHOW_COLUMNS);
    }

    @Override
    public void checkCanSelectFromColumns(UserIdentity user, String catalog, String schema,
                                          String table, Set<String> columns) {
        List<PrivilegeGrant> userGrants = resolveGrants(user);
        boolean hasTableSelect = userGrants.stream()
                .anyMatch(g -> g.privilege().implies(Privilege.SELECT) && g.coversTable(catalog, schema, table));
        if (hasTableSelect) {
            return;
        }
        for (String col : columns) {
            checkColumnSelect(userGrants, catalog, schema, table, col);
        }
    }

    @Override
    public Optional<String> getRowFilter(UserIdentity user, String catalog, String schema, String table) {
        return rlsFilter.getFilter(user, effectiveRoles(user), catalog, schema, table);
    }

    @Override
    public Optional<String> getColumnMask(UserIdentity user, String catalog, String schema,
                                          String table, String column) {
        return maskProcessor.getMask(user, effectiveRoles(user), catalog, schema, table, column);
    }

    private List<PrivilegeGrant> resolveGrants(UserIdentity user) {
        Set<String> roles = effectiveRoles(user);
        return grantStore.listGrants().stream()
                .filter(g -> roles.contains(g.roleName()))
                .toList();
    }

    private void requireTableGrant(UserIdentity user, String catalog, String schema,
                                   String table, Privilege privilege) {
        List<PrivilegeGrant> userGrants = resolveGrants(user);
        PrivilegeGrant matchedGrant = userGrants.stream()
                .filter(g -> g.privilege().implies(privilege) && g.coversTable(catalog, schema, table))
                .findFirst()
                .orElse(null);
        if (matchedGrant == null) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on %s.%s.%s".formatted(user.principal(), privilege, catalog, schema, table));
        }
        logWildcardGrantAccess(user, matchedGrant, catalog, schema, table);
    }

    /**
     * Logs a warning when access is granted via a wildcard pattern, to aid
     * audit trail analysis for over-privileged grants.
     */
    private static void logWildcardGrantAccess(UserIdentity user, PrivilegeGrant grant,
                                               String catalog, String schema, String table) {
        boolean hasWildcard = "*".equals(grant.catalog())
                || "*".equals(grant.schema())
                || "*".equals(grant.table());
        if (hasWildcard) {
            LOG.info("event=wildcard_grant_access principal={} privilege={} "
                    + "grantPattern={}.{}.{} accessedResource={}.{}.{}",
                    user.principal(), grant.privilege(),
                    grant.catalog(), grant.schema(), grant.table(),
                    catalog, schema, table);
        }
    }

    private void requireSchemaGrant(UserIdentity user, String catalog, String schema,
                                    Privilege privilege) {
        boolean granted = resolveGrants(user).stream()
                .anyMatch(g -> g.privilege().implies(privilege) && g.coversTable(catalog, schema, null));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on %s.%s".formatted(user.principal(), privilege, catalog, schema));
        }
    }

    private void requireCatalogGrant(UserIdentity user, String catalog, Privilege privilege) {
        boolean granted = resolveGrants(user).stream().anyMatch(grant ->
                "CATALOG".equalsIgnoreCase(grant.objectType())
                        && grant.privilege().implies(privilege) && grant.coversTable(catalog, null, null));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on catalog '%s'".formatted(user.principal(), privilege, catalog));
        }
    }

    private void requireAnyGrant(UserIdentity user, String catalog, String schema,
                                 String table, Privilege privilege) {
        boolean granted = resolveGrants(user).stream()
                .anyMatch(g -> g.privilege().implies(privilege) && g.coversTable(catalog, schema, table));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on catalog '%s'".formatted(user.principal(), privilege, catalog));
        }
    }

    private void checkColumnSelect(List<PrivilegeGrant> userGrants, String catalog,
                                   String schema, String table, String column) {
        boolean granted = userGrants.stream()
                .anyMatch(g -> g.privilege().implies(Privilege.SELECT)
                    && g.coversColumn(catalog, schema, table, column));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User lacks SELECT on column '%s' in %s.%s.%s".formatted(column, catalog, schema, table));
        }
    }
}
